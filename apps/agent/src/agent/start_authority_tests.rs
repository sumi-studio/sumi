use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use tokio::sync::mpsc;

use super::{
    AdmittedCommand, InjectedRunDriver, RunCompletion, RunControl, RunCore, RunOutput, RunWorker,
    SequentialRunWorker, Session, SessionStartAuthority, StreamStarter, WorkerFuture,
};
use crate::{
    approval::{ApprovalBroker, Policy, SecretAwareActionProjector, SecretDigestKey},
    gateway::{
        AgentHello, ApiHello, Gateway, GatewayClosed, GatewayReader, GatewayWriter, HelloError,
        InboundCommand, OutboundFrame,
    },
    provider::{ModelSpec, RequestOptions, types::PromptContext},
    runtime::{
        authority::RuntimeEpochAuthority,
        contracts::{
            GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease, RpcBootNonce,
            RpcIdentity,
        },
    },
    store::{HydratedRunState, HydrationOutcome, Redactor, Store},
    tools::{
        WorkspacePaths,
        executor::{ExecutorClient, remote_executor_registry},
    },
};

const PAID_A: &str = "0198f0f4-9b72-7000-8000-000000000001";
const PAID_B: &str = "0198f0f4-9b72-7000-8000-000000000002";

fn generation() -> ProcessGeneration {
    ProcessGeneration::from_wire(73).expect("valid generation")
}

fn runtime_authority(
    store: &Store,
    nonce: &str,
    lease_id: &str,
    fence_id: &str,
) -> RuntimeEpochAuthority {
    let rpc_identity = RpcIdentity::new(
        store.scope().personality_agent_id.clone(),
        generation(),
        RpcBootNonce::new(nonce).expect("valid nonce"),
    );
    let lease = ProcessGenerationLease::new(
        store.scope().personality_agent_id.clone(),
        generation(),
        lease_id,
    )
    .expect("valid lease");
    let fence = GenerationRecoveryFence::new(&lease, fence_id).expect("valid fence");
    RuntimeEpochAuthority::new(rpc_identity, lease, fence).expect("consistent runtime authority")
}

async fn hydrate_complete(store: &Store, authority: &RuntimeEpochAuthority) -> HydratedRunState {
    match store
        .hydrate(authority.lease(), authority.fence())
        .await
        .expect("hydrate test Store")
    {
        HydrationOutcome::Complete(hydrated) => hydrated,
        other => panic!("empty test Store must hydrate completely: {other:?}"),
    }
}

async fn data_key_count(store: &Store) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM data_keys")
        .fetch_one(store.pool())
        .await
        .expect("count data keys")
}

fn approval_broker() -> Arc<ApprovalBroker> {
    Arc::new(ApprovalBroker::headless(
        Policy::new("/workspace"),
        SecretAwareActionProjector::new(Redactor::v1(), SecretDigestKey::fixture()),
    ))
}

fn remote_worker(identity: RpcIdentity) -> Arc<dyn RunWorker> {
    let registry = remote_executor_registry(Arc::new(ExecutorClient::new(
        "/tmp/sumi-unused-session-executor.sock",
        identity.clone(),
    )))
    .expect("remote registry");
    let prompt = PromptContext {
        system_prompt: "session identity fixture".to_owned(),
        memory_blocks: Vec::new(),
        messages: Vec::new(),
        provider_context: Vec::new(),
        tools: registry.definitions(),
        replay_provenance: None,
    };
    let stream_starter: Arc<StreamStarter> =
        Arc::new(|_, _, _, _, _| panic!("identity validation must not start a provider"));
    let driver = InjectedRunDriver::with_stream_starter(
        ModelSpec::preset("kimi-k3").expect("model"),
        RequestOptions::default(),
        Some(prompt),
        Some(registry),
        Some(WorkspacePaths::new("/workspace").expect("workspace")),
        Some(identity.generation()),
        stream_starter,
    )
    .expect("identity-bound driver");
    Arc::new(SequentialRunWorker::new(Arc::new(driver)))
}

struct ProbeGateway {
    split_count: Arc<AtomicUsize>,
}

struct ClosedReader;
struct SinkWriter;

#[async_trait]
impl GatewayReader for ClosedReader {
    async fn next_command(&mut self) -> Result<InboundCommand> {
        Err(GatewayClosed.into())
    }
}

#[async_trait]
impl GatewayWriter for SinkWriter {
    async fn send(&mut self, _frame: OutboundFrame) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Gateway for ProbeGateway {
    type Reader = ClosedReader;
    type Writer = SinkWriter;

    async fn authenticate_hello(
        &mut self,
        _hello: AgentHello,
    ) -> std::result::Result<ApiHello, HelloError> {
        panic!("Session receives an already-authenticated gateway")
    }

    fn split(self) -> (Self::Reader, Self::Writer) {
        self.split_count.fetch_add(1, Ordering::SeqCst);
        (ClosedReader, SinkWriter)
    }
}

struct ProbeWorker {
    expected_identity: RpcIdentity,
    validation_count: Arc<AtomicUsize>,
}

impl RunWorker for ProbeWorker {
    fn validate_runtime_identity(&self, identity: &RpcIdentity) -> Result<()> {
        self.validation_count.fetch_add(1, Ordering::SeqCst);
        if identity != &self.expected_identity {
            return Err(anyhow!("unexpected executor RPC identity"));
        }
        Ok(())
    }

    fn validate_executor_generation(&self, generation: ProcessGeneration) -> Result<()> {
        self.validation_count.fetch_add(1, Ordering::SeqCst);
        if generation != self.expected_identity.generation() {
            return Err(anyhow!("unexpected executor generation"));
        }
        Ok(())
    }

    fn run(
        &self,
        core: RunCore,
        _initial: AdmittedCommand,
        _controls: mpsc::Receiver<RunControl>,
        _events: mpsc::Sender<RunOutput>,
    ) -> WorkerFuture {
        Box::pin(async move { RunCompletion::Completed(core) })
    }
}

fn probes(
    expected_identity: RpcIdentity,
) -> (
    ProbeGateway,
    Arc<AtomicUsize>,
    Arc<dyn RunWorker>,
    Arc<AtomicUsize>,
) {
    let split_count = Arc::new(AtomicUsize::new(0));
    let validation_count = Arc::new(AtomicUsize::new(0));
    (
        ProbeGateway {
            split_count: split_count.clone(),
        },
        split_count,
        Arc::new(ProbeWorker {
            expected_identity,
            validation_count: validation_count.clone(),
        }),
        validation_count,
    )
}

#[tokio::test]
async fn valid_hydrated_authority_starts_the_exact_bound_core() {
    let store = Store::session_test_store(PAID_A).await.expect("store");
    let runtime = runtime_authority(&store, "boot-valid", "lease-valid", "fence-valid");
    let runtime_identity = runtime.rpc_identity().clone();
    let hydrated = hydrate_complete(&store, &runtime).await;
    let approval = approval_broker();
    let expected_approval = approval.clone();
    let (core, start_authority) =
        SessionStartAuthority::from_hydrated(runtime, &hydrated, approval).expect("bind hydration");
    let bound_core_id = core.ownership_id();
    let (gateway, split_count, worker, validation_count) = probes(runtime_identity);

    let session = Session::start_hydrated(store, gateway, core, worker, start_authority)
        .await
        .expect("valid hydrated Session start");

    assert_eq!(split_count.load(Ordering::SeqCst), 1);
    assert_eq!(validation_count.load(Ordering::SeqCst), 1);
    let session_core = session.core.as_ref().expect("Session core");
    assert_eq!(session_core.ownership_id(), bound_core_id);
    assert!(Arc::ptr_eq(
        session_core
            .approval
            .as_ref()
            .and_then(super::ApprovalRuntime::legacy)
            .expect("composed legacy approval"),
        &expected_approval
    ));
    assert_eq!(session.executor_generation, generation());
}

#[tokio::test]
async fn cross_paid_authority_fails_before_store_gateway_or_worker_side_effects() {
    let source_store = Store::session_test_store(PAID_A)
        .await
        .expect("source store");
    let runtime = runtime_authority(
        &source_store,
        "boot-cross-paid",
        "lease-cross-paid",
        "fence-cross-paid",
    );
    let runtime_identity = runtime.rpc_identity().clone();
    let hydrated = hydrate_complete(&source_store, &runtime).await;
    let (core, start_authority) =
        SessionStartAuthority::from_hydrated(runtime, &hydrated, approval_broker())
            .expect("bind source hydration");

    let target_store = Store::session_test_store(PAID_B)
        .await
        .expect("target store");
    let target_observer = target_store.clone();
    let keys_before = data_key_count(&target_observer).await;
    let (gateway, split_count, worker, validation_count) = probes(runtime_identity);

    let error =
        match Session::start_hydrated(target_store, gateway, core, worker, start_authority).await {
            Ok(_) => panic!("cross-PAID Session start must fail"),
            Err(error) => error,
        };

    assert!(error.to_string().contains("Session Store PAID"));
    assert_eq!(data_key_count(&target_observer).await, keys_before);
    assert_eq!(split_count.load(Ordering::SeqCst), 0);
    assert_eq!(validation_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn same_generation_stale_lease_or_fence_cannot_bind_hydration() {
    let store = Store::session_test_store(PAID_A).await.expect("store");
    let current = runtime_authority(&store, "boot-current", "lease-current", "fence-current");
    let hydrated = hydrate_complete(&store, &current).await;

    let stale_lease = runtime_authority(&store, "boot-current", "lease-stale", "fence-stale-lease");
    let lease_error =
        SessionStartAuthority::from_hydrated(stale_lease, &hydrated, approval_broker())
            .err()
            .expect("same-generation stale lease must fail");
    assert!(lease_error.to_string().contains("lease is stale"));

    let stale_fence = GenerationRecoveryFence::new(current.lease(), "fence-stale")
        .expect("same-lease stale fence");
    let stale_fence_runtime = RuntimeEpochAuthority::new(
        current.rpc_identity().clone(),
        current.lease().clone(),
        stale_fence,
    )
    .expect("internally consistent stale-fence authority");
    let fence_error =
        SessionStartAuthority::from_hydrated(stale_fence_runtime, &hydrated, approval_broker())
            .err()
            .expect("same-generation stale fence must fail");
    assert!(fence_error.to_string().contains("fence is stale"));
}

#[tokio::test]
async fn forged_hydration_receipt_cannot_mint_start_authority() {
    let store = Store::session_test_store(PAID_A).await.expect("store");
    let runtime = runtime_authority(
        &store,
        "boot-forged-receipt",
        "lease-forged-receipt",
        "fence-forged-receipt",
    );
    let mut hydrated = hydrate_complete(&store, &runtime).await;
    hydrated.receipt.fence_id = "forged-fence".to_owned();

    let error = SessionStartAuthority::from_hydrated(runtime, &hydrated, approval_broker())
        .err()
        .expect("forged hydration receipt must fail");

    assert!(error.to_string().contains("hydration receipt"));
}

#[tokio::test]
async fn arbitrary_core_cannot_use_an_authentic_hydration_authority() {
    let store = Store::session_test_store(PAID_A).await.expect("store");
    let runtime = runtime_authority(
        &store,
        "boot-arbitrary-core",
        "lease-arbitrary-core",
        "fence-arbitrary-core",
    );
    let runtime_identity = runtime.rpc_identity().clone();
    let hydrated = hydrate_complete(&store, &runtime).await;
    let (_bound_core, start_authority) =
        SessionStartAuthority::from_hydrated(runtime, &hydrated, approval_broker())
            .expect("bind hydration");
    let arbitrary_core = RunCore::new();
    let observer = store.clone();
    let keys_before = data_key_count(&observer).await;
    let (gateway, split_count, worker, validation_count) = probes(runtime_identity);

    let error = match Session::start_hydrated(
        store,
        gateway,
        arbitrary_core,
        worker,
        start_authority,
    )
    .await
    {
        Ok(_) => panic!("arbitrary RunCore must not use another core's authority"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("exact unmutated core"));
    assert_eq!(data_key_count(&observer).await, keys_before);
    assert_eq!(split_count.load(Ordering::SeqCst), 0);
    assert_eq!(validation_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn core_mutated_after_hydration_binding_cannot_start() {
    let store = Store::session_test_store(PAID_A).await.expect("store");
    let runtime = runtime_authority(
        &store,
        "boot-mutated-core",
        "lease-mutated-core",
        "fence-mutated-core",
    );
    let runtime_identity = runtime.rpc_identity().clone();
    let hydrated = hydrate_complete(&store, &runtime).await;
    let (mut core, start_authority) =
        SessionStartAuthority::from_hydrated(runtime, &hydrated, approval_broker())
            .expect("bind hydration");
    core.mark_mutated();
    let observer = store.clone();
    let keys_before = data_key_count(&observer).await;
    let (gateway, split_count, worker, validation_count) = probes(runtime_identity);

    let error = match Session::start_hydrated(store, gateway, core, worker, start_authority).await {
        Ok(_) => panic!("core mutation after hydration binding must fail closed"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("exact unmutated core"));
    assert_eq!(data_key_count(&observer).await, keys_before);
    assert_eq!(split_count.load(Ordering::SeqCst), 0);
    assert_eq!(validation_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn approval_broker_replaced_after_hydration_binding_cannot_start() {
    let store = Store::session_test_store(PAID_A).await.expect("store");
    let runtime = runtime_authority(
        &store,
        "boot-replaced-approval",
        "lease-replaced-approval",
        "fence-replaced-approval",
    );
    let runtime_identity = runtime.rpc_identity().clone();
    let hydrated = hydrate_complete(&store, &runtime).await;
    let (mut core, start_authority) =
        SessionStartAuthority::from_hydrated(runtime, &hydrated, approval_broker())
            .expect("bind hydration");
    core.set_approval(approval_broker());

    let observer = store.clone();
    let keys_before = data_key_count(&observer).await;
    let (gateway, split_count, worker, validation_count) = probes(runtime_identity);
    let error = match Session::start_hydrated(store, gateway, core, worker, start_authority).await {
        Ok(_) => panic!("post-bind approval replacement must fail closed"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("exact unmutated core"));
    assert_eq!(data_key_count(&observer).await, keys_before);
    assert_eq!(split_count.load(Ordering::SeqCst), 0);
    assert_eq!(validation_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn worker_bound_to_wrong_paid_rejects_before_session_side_effects() {
    let store = Store::session_test_store(PAID_A).await.expect("store");
    let runtime = runtime_authority(
        &store,
        "boot-worker-paid",
        "lease-worker-paid",
        "fence-worker-paid",
    );
    let hydrated = hydrate_complete(&store, &runtime).await;
    let (core, start_authority) =
        SessionStartAuthority::from_hydrated(runtime, &hydrated, approval_broker())
            .expect("bind hydration");
    let wrong_identity = RpcIdentity::from_wire(PAID_B, generation().as_u64(), "boot-worker-paid")
        .expect("wrong-PAID identity");

    let observer = store.clone();
    let keys_before = data_key_count(&observer).await;
    let split_count = Arc::new(AtomicUsize::new(0));
    let gateway = ProbeGateway {
        split_count: split_count.clone(),
    };
    let worker = remote_worker(wrong_identity);
    let error = match Session::start_hydrated(store, gateway, core, worker, start_authority).await {
        Ok(_) => panic!("worker with wrong PAID must fail closed"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("executor RPC identity"));
    assert_eq!(data_key_count(&observer).await, keys_before);
    assert_eq!(split_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn worker_bound_to_same_generation_wrong_nonce_rejects_before_session_side_effects() {
    let store = Store::session_test_store(PAID_A).await.expect("store");
    let runtime = runtime_authority(
        &store,
        "boot-worker-current",
        "lease-worker-nonce",
        "fence-worker-nonce",
    );
    let hydrated = hydrate_complete(&store, &runtime).await;
    let (core, start_authority) =
        SessionStartAuthority::from_hydrated(runtime, &hydrated, approval_broker())
            .expect("bind hydration");
    let wrong_identity = RpcIdentity::from_wire(PAID_A, generation().as_u64(), "boot-worker-stale")
        .expect("wrong-nonce identity");

    let observer = store.clone();
    let keys_before = data_key_count(&observer).await;
    let split_count = Arc::new(AtomicUsize::new(0));
    let gateway = ProbeGateway {
        split_count: split_count.clone(),
    };
    let worker = remote_worker(wrong_identity);
    let error = match Session::start_hydrated(store, gateway, core, worker, start_authority).await {
        Ok(_) => panic!("worker with wrong boot nonce must fail closed"),
        Err(error) => error,
    };

    assert!(error.to_string().contains("executor RPC identity"));
    assert_eq!(data_key_count(&observer).await, keys_before);
    assert_eq!(split_count.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn production_remote_worker_accepts_the_exact_runtime_identity() {
    let store = Store::session_test_store(PAID_A).await.expect("store");
    let runtime = runtime_authority(
        &store,
        "boot-worker-exact",
        "lease-worker-exact",
        "fence-worker-exact",
    );
    let runtime_identity = runtime.rpc_identity().clone();
    let hydrated = hydrate_complete(&store, &runtime).await;
    let (core, start_authority) =
        SessionStartAuthority::from_hydrated(runtime, &hydrated, approval_broker())
            .expect("bind hydration");
    let split_count = Arc::new(AtomicUsize::new(0));
    let gateway = ProbeGateway {
        split_count: split_count.clone(),
    };

    let _session = Session::start_hydrated(
        store,
        gateway,
        core,
        remote_worker(runtime_identity),
        start_authority,
    )
    .await
    .expect("exact remote executor identity");

    assert_eq!(split_count.load(Ordering::SeqCst), 1);
}
