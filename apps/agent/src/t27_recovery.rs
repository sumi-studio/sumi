//! T27 physical recovery receipt construction and application.
//!
//! This module closes the bootstrap-time loop for `Store::hydrate` returning
//! `HydrationOutcome::RecoveryRequired`. The runtime submits an exact recovery
//! request, but it never kills a cgroup or writes the physical proof.  The host
//! supervisor owns those actions and writes a receipt in a directory which the
//! runtime can only read.  Only after that receipt validates does this module
//! inject the logical terminal EventBatch through `EventWriter`.

use std::{
    collections::BTreeSet,
    fs::{File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};

use anyhow::{Context, Result, bail};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::provider::types::{PublicMessage, ToolResultMessage, UserContent};
use crate::runtime::{
    contracts::{GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease},
    supervisor::kill_and_remove_cgroup,
};
use crate::store::{
    ApplyReceiptOutcome, EventBatch, EventWrite, EventWriter, PhysicalRecoveryIntent,
    PhysicalRecoveryIntentRequest, PhysicalRecoveryReceipt, Projection, ToolExecutionMutation,
};

const RECOVERED_TEXT: &str = "recovered";
const PROOF_VERSION: u32 = 1;
const PROOF_EXT: &str = "t27proof";
const REQUEST_EXT: &str = "t27request";
const REAP_DIR: &str = "reaped-generations";
const SUPERVISOR_RECEIPT_WAIT: Duration = Duration::from_secs(30);

/// A root-supervisor-produced physical proof consumed before T17 logical
/// recovery. This file must live outside the runtime-writable mount.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct PhysicalRecoveryProof {
    version: u32,
    receipt_id: String,
    proof_digest: String,
    lease_id: String,
    generation: u64,
    fence_id: String,
    tenant_id: String,
    agent_id: String,
    conversation_id: String,
    intents: Vec<ProofIntent>,
    killed_cgroup_paths: Vec<String>,
    persisted_at: String,
}

/// The only T27 artifact the runtime may author. It asks the supervisor to
/// bind a previously reaped generation to the exact durable intent digest; it
/// never constitutes physical proof on its own.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct SupervisorRecoveryRequest {
    version: u32,
    receipt_id: String,
    request_digest: String,
    lease_id: String,
    generation: u64,
    fence_id: String,
    tenant_id: String,
    agent_id: String,
    conversation_id: String,
    intents: Vec<ProofIntent>,
}

/// Host-only evidence written immediately after a service/executor generation
/// has been killed and observed empty. A later runtime request can bind it to
/// the exact intent set without obtaining cgroup authority.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct ReapedGenerationProof {
    version: u32,
    tenant_id: String,
    agent_id: String,
    conversation_id: String,
    generation: u64,
    lease_id: String,
    fence_id: String,
    killed_cgroup_paths: Vec<String>,
    reaped_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
struct ProofIntent {
    tool_call_id: String,
    tool_name: String,
    command_id: String,
    run_id: String,
    executor_generation: u64,
}

impl PhysicalRecoveryProof {
    fn new(
        receipt_id: String,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
        tenant_id: &str,
        agent_id: &str,
        conversation_id: &str,
        intents: &[PhysicalRecoveryIntentRequest],
        killed_cgroup_paths: Vec<String>,
    ) -> Self {
        let mut proof_intents: Vec<ProofIntent> = intents
            .iter()
            .map(|intent| ProofIntent {
                tool_call_id: intent.tool_call_id.clone(),
                tool_name: intent.tool_name.clone(),
                command_id: intent.command_id.clone(),
                run_id: intent.run_id.clone(),
                executor_generation: intent.executor_generation.as_u64(),
            })
            .collect();
        proof_intents.sort();

        Self {
            version: PROOF_VERSION,
            receipt_id: receipt_id.clone(),
            proof_digest: proof_digest(
                lease,
                fence,
                tenant_id,
                agent_id,
                conversation_id,
                &proof_intents,
                &killed_cgroup_paths,
            ),
            lease_id: lease.lease_id().to_owned(),
            generation: lease.generation().as_u64(),
            fence_id: fence.fence_id().to_owned(),
            tenant_id: tenant_id.to_owned(),
            agent_id: agent_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            intents: proof_intents,
            killed_cgroup_paths,
            persisted_at: Utc::now().to_rfc3339(),
        }
    }

    fn validate_for(
        &self,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
        tenant_id: &str,
        agent_id: &str,
        conversation_id: &str,
        intents: &[PhysicalRecoveryIntentRequest],
    ) -> Result<()> {
        if self.version != PROOF_VERSION {
            bail!(
                "physical recovery proof has unsupported version {}",
                self.version
            );
        }

        let receipt_id = deterministic_receipt_id(lease, fence, intents);
        if self.receipt_id != receipt_id {
            bail!("physical recovery proof receipt_id does not match current recovery");
        }

        let mut proof_intents: Vec<ProofIntent> = intents
            .iter()
            .map(|intent| ProofIntent {
                tool_call_id: intent.tool_call_id.clone(),
                tool_name: intent.tool_name.clone(),
                command_id: intent.command_id.clone(),
                run_id: intent.run_id.clone(),
                executor_generation: intent.executor_generation.as_u64(),
            })
            .collect();
        proof_intents.sort();

        let expected_digest = proof_digest(
            lease,
            fence,
            tenant_id,
            agent_id,
            conversation_id,
            &proof_intents,
            &self.killed_cgroup_paths,
        );
        if self.proof_digest != expected_digest {
            bail!(
                "physical recovery proof digest does not match canonical intent set and killed cgroup paths"
            );
        }

        if self.intents != proof_intents {
            bail!("physical recovery proof intent set does not match current recovery");
        }
        if self.tenant_id != tenant_id
            || self.agent_id != agent_id
            || self.conversation_id != conversation_id
        {
            bail!("physical recovery proof scope does not match the authenticated runtime scope");
        }

        let proof_generation = ProcessGeneration::from_wire(self.generation).map_err(|error| {
            anyhow::anyhow!("physical recovery proof generation is invalid: {error}")
        })?;
        lease
            .validate_exact(proof_generation, &self.lease_id)
            .map_err(|error| anyhow::anyhow!("physical recovery proof lease mismatch: {error}"))?;
        fence
            .validate_exact(lease, &self.fence_id)
            .map_err(|error| anyhow::anyhow!("physical recovery proof fence mismatch: {error}"))?;

        Ok(())
    }
}

impl SupervisorRecoveryRequest {
    fn new(
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
        tenant_id: &str,
        agent_id: &str,
        conversation_id: &str,
        intents: &[PhysicalRecoveryIntentRequest],
    ) -> Self {
        let mut proof_intents = proof_intents(intents);
        proof_intents.sort();
        let receipt_id = deterministic_receipt_id(lease, fence, intents);
        let request_digest = request_digest(
            lease,
            fence,
            tenant_id,
            agent_id,
            conversation_id,
            &proof_intents,
        );
        Self {
            version: PROOF_VERSION,
            receipt_id,
            request_digest,
            lease_id: lease.lease_id().to_owned(),
            generation: lease.generation().as_u64(),
            fence_id: fence.fence_id().to_owned(),
            tenant_id: tenant_id.to_owned(),
            agent_id: agent_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            intents: proof_intents,
        }
    }

    fn validate_for(
        &self,
        lease: &ProcessGenerationLease,
        fence: &GenerationRecoveryFence,
        tenant_id: &str,
        agent_id: &str,
        conversation_id: &str,
        intents: &[PhysicalRecoveryIntentRequest],
    ) -> Result<()> {
        if self.version != PROOF_VERSION {
            bail!(
                "supervisor recovery request has unsupported version {}",
                self.version
            );
        }
        let expected = Self::new(lease, fence, tenant_id, agent_id, conversation_id, intents);
        if self.receipt_id != expected.receipt_id
            || self.request_digest != expected.request_digest
            || self.lease_id != expected.lease_id
            || self.generation != expected.generation
            || self.fence_id != expected.fence_id
            || self.tenant_id != expected.tenant_id
            || self.agent_id != expected.agent_id
            || self.conversation_id != expected.conversation_id
            || self.intents != expected.intents
        {
            bail!("supervisor recovery request does not match the authenticated recovery");
        }
        Ok(())
    }
}

fn proof_intents(intents: &[PhysicalRecoveryIntentRequest]) -> Vec<ProofIntent> {
    intents
        .iter()
        .map(|intent| ProofIntent {
            tool_call_id: intent.tool_call_id.clone(),
            tool_name: intent.tool_name.clone(),
            command_id: intent.command_id.clone(),
            run_id: intent.run_id.clone(),
            executor_generation: intent.executor_generation.as_u64(),
        })
        .collect()
}

fn proof_digest(
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    intents: &[ProofIntent],
    killed_cgroup_paths: &[String],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"sumi-physical-recovery-proof/v1");
    hasher.update(lease.lease_id().as_bytes());
    hasher.update(lease.generation().as_u64().to_be_bytes());
    hasher.update(fence.fence_id().as_bytes());
    for value in [tenant_id, agent_id, conversation_id] {
        hasher.update((value.len() as u64).to_be_bytes());
        hasher.update(value.as_bytes());
    }
    for intent in intents {
        hasher.update((intent.tool_call_id.len() as u64).to_be_bytes());
        hasher.update(intent.tool_call_id.as_bytes());
        hasher.update((intent.tool_name.len() as u64).to_be_bytes());
        hasher.update(intent.tool_name.as_bytes());
        hasher.update((intent.command_id.len() as u64).to_be_bytes());
        hasher.update(intent.command_id.as_bytes());
        hasher.update((intent.run_id.len() as u64).to_be_bytes());
        hasher.update(intent.run_id.as_bytes());
        hasher.update(intent.executor_generation.to_be_bytes());
    }
    for path in killed_cgroup_paths {
        hasher.update((path.len() as u64).to_be_bytes());
        hasher.update(path.as_bytes());
    }
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn request_digest(
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    intents: &[ProofIntent],
) -> String {
    proof_digest(
        lease,
        fence,
        tenant_id,
        agent_id,
        conversation_id,
        intents,
        &[],
    )
}

/// Consume a supervisor-produced physical proof and produce the logical T17
/// receipt. The runtime has no cgroup path or host proof-write authority.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn consume_physical_recovery(
    writer: &EventWriter,
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
    intents: Vec<PhysicalRecoveryIntentRequest>,
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    request_dir: &Path,
    supervisor_proof_dir: &Path,
) -> Result<PhysicalRecoveryReceipt> {
    if intents.is_empty() {
        bail!("physical recovery requires at least one running tool intent");
    }

    let mut sorted_intents = intents;
    sorted_intents.sort_by(|a, b| a.tool_call_id.cmp(&b.tool_call_id));

    for intent in &sorted_intents {
        if intent.tool_call_id.is_empty()
            || intent.command_id.is_empty()
            || intent.run_id.is_empty()
            || intent.tool_name.is_empty()
        {
            bail!("physical recovery intent identity and tool_name must not be empty");
        }
    }

    let ids: BTreeSet<_> = sorted_intents.iter().map(|i| &i.tool_call_id).collect();
    if ids.len() != sorted_intents.len() {
        bail!("physical recovery intents must have unique tool_call_id values");
    }

    ensure_supervisor_proof_boundary(request_dir, supervisor_proof_dir)?;
    let request = SupervisorRecoveryRequest::new(
        lease,
        fence,
        tenant_id,
        agent_id,
        conversation_id,
        &sorted_intents,
    );
    let request_path = request_dir.join(format!("{}.{}", request.receipt_id, REQUEST_EXT));
    persist_json(&request_path, &request)
        .context("failed to submit T27 supervisor recovery request")?;

    let proof_path = supervisor_proof_dir.join(format!("{}.{}", request.receipt_id, PROOF_EXT));
    let proof = wait_for_supervisor_proof(&proof_path)?;
    proof
        .validate_for(
            lease,
            fence,
            tenant_id,
            agent_id,
            conversation_id,
            &sorted_intents,
        )
        .context("supervisor physical recovery proof does not match this recovery")?;
    for path in &proof.killed_cgroup_paths {
        if Path::new(path).exists() {
            bail!(
                "supervisor physical recovery proof is stale: recorded killed cgroup was re-created at {path}"
            );
        }
    }

    if std::env::var("SUMI_T27_FAILPOINT").unwrap_or_default() == "after-proof-persist" {
        bail!("T27 failpoint: after-proof-persist");
    }

    let receipt = apply_recovery_receipt(writer, lease, fence, &sorted_intents, request.receipt_id)
        .await
        .context("failed to apply physical recovery receipt through EventWriter")?;

    Ok(receipt)
}

fn ensure_supervisor_proof_boundary(request_dir: &Path, proof_dir: &Path) -> Result<()> {
    if request_dir == proof_dir {
        bail!("supervisor proof directory must be distinct from the runtime request directory");
    }
    if !request_dir.is_dir() || !proof_dir.is_dir() {
        bail!(
            "T27 supervisor request/proof directories are unavailable (request={}, proof={})",
            request_dir.display(),
            proof_dir.display()
        );
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(proof_dir)
            .with_context(|| {
                format!(
                    "failed to stat supervisor proof directory {}",
                    proof_dir.display()
                )
            })?
            .permissions()
            .mode();
        if mode & 0o022 != 0 {
            bail!(
                "supervisor proof directory {} is writable by group or other",
                proof_dir.display()
            );
        }
    }
    Ok(())
}

fn wait_for_supervisor_proof(path: &Path) -> Result<PhysicalRecoveryProof> {
    let deadline = Instant::now() + SUPERVISOR_RECEIPT_WAIT;
    loop {
        if path.is_file() {
            let bytes = std::fs::read(path)
                .with_context(|| format!("failed to read supervisor proof {}", path.display()))?;
            return serde_json::from_slice(&bytes).with_context(|| {
                format!("failed to deserialize supervisor proof {}", path.display())
            });
        }
        if Instant::now() >= deadline {
            bail!(
                "timed out waiting for supervisor physical recovery proof {}",
                path.display()
            );
        }
        thread::sleep(Duration::from_millis(25));
    }
}

fn persist_json<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let dir = path
        .parent()
        .context("physical recovery proof has no parent directory")?;
    std::fs::create_dir_all(dir).with_context(|| {
        format!(
            "failed to create physical recovery proof directory {}",
            dir.display()
        )
    })?;

    let temp = dir.join(format!(
        ".{}.tmp",
        path.file_name().and_then(|n| n.to_str()).unwrap_or("proof")
    ));

    {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temp)
            .with_context(|| format!("failed to open temporary proof file {}", temp.display()))?;
        let bytes = serde_json::to_vec_pretty(value)
            .context("failed to serialize T27 supervisor record")?;
        file.write_all(&bytes)
            .with_context(|| format!("failed to write proof to {}", temp.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to sync proof file {}", temp.display()))?;
    }

    std::fs::rename(&temp, path).with_context(|| {
        format!(
            "failed to atomically rename proof from {} to {}",
            temp.display(),
            path.display()
        )
    })?;

    // fsync the directory so the rename is durable.
    let dir_file = File::open(dir)
        .with_context(|| format!("failed to open proof directory {} for fsync", dir.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        unsafe {
            if libc::fsync(dir_file.as_raw_fd()) != 0 {
                bail!(
                    "failed to fsync physical recovery proof directory: {}",
                    std::io::Error::last_os_error()
                );
            }
        }
    }
    #[cfg(not(unix))]
    {
        let _ = dir_file;
    }

    Ok(())
}

/// Called only by the host supervisor immediately after it has killed and
/// reaped a generation's service and executor cgroups. The runtime cannot call
/// this entry point because it is not given the host proof directory or cgroup
/// write authority.
pub(crate) fn supervisor_reap_generation(
    proof_dir: &Path,
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
    executor_cgroup: &Path,
    service_cgroup: &Path,
) -> Result<()> {
    ensure_host_proof_dir(proof_dir)?;
    let mut killed = Vec::new();
    for path in [executor_cgroup, service_cgroup] {
        if !path.is_dir() {
            bail!(
                "supervisor cgroup is unavailable for reap: {}",
                path.display()
            );
        }
        kill_and_remove_cgroup(path)
            .with_context(|| format!("supervisor failed to kill/reap {}", path.display()))?;
        killed.push(path.display().to_string());
    }
    let record = ReapedGenerationProof {
        version: PROOF_VERSION,
        tenant_id: tenant_id.to_owned(),
        agent_id: agent_id.to_owned(),
        conversation_id: conversation_id.to_owned(),
        generation: lease.generation().as_u64(),
        lease_id: lease.lease_id().to_owned(),
        fence_id: fence.fence_id().to_owned(),
        killed_cgroup_paths: killed,
        reaped_at: Utc::now().to_rfc3339(),
    };
    persist_json(&reaped_record_path(proof_dir, record.generation), &record)
}

/// Called by the host supervisor while monitoring its runtime request
/// directory. It binds only a prior host-produced reap record to the exact
/// current intent set and writes the immutable receipt consumed by the runtime.
pub(crate) fn supervisor_fulfill_recovery_request(
    request_path: &Path,
    proof_dir: &Path,
    tenant_id: &str,
    agent_id: &str,
    conversation_id: &str,
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
) -> Result<()> {
    ensure_host_proof_dir(proof_dir)?;
    let bytes = std::fs::read(request_path)
        .with_context(|| format!("failed to read recovery request {}", request_path.display()))?;
    let request: SupervisorRecoveryRequest = serde_json::from_slice(&bytes).with_context(|| {
        format!(
            "failed to parse recovery request {}",
            request_path.display()
        )
    })?;
    let intents = request_to_intents(&request)?;
    request.validate_for(lease, fence, tenant_id, agent_id, conversation_id, &intents)?;

    let mut generations = BTreeSet::new();
    for intent in &intents {
        let generation = intent.executor_generation.as_u64();
        if generation >= lease.generation().as_u64() {
            bail!(
                "recovery request references generation {generation}, not older than current generation"
            );
        }
        generations.insert(generation);
    }

    let mut killed_paths = Vec::new();
    for generation in generations {
        let path = reaped_record_path(proof_dir, generation);
        let record: ReapedGenerationProof = serde_json::from_slice(
            &std::fs::read(&path)
                .with_context(|| format!("missing host reap record {}", path.display()))?,
        )
        .with_context(|| format!("failed to parse host reap record {}", path.display()))?;
        if record.version != PROOF_VERSION
            || record.tenant_id != tenant_id
            || record.agent_id != agent_id
            || record.conversation_id != conversation_id
            || record.generation != generation
            || record.killed_cgroup_paths.is_empty()
        {
            bail!(
                "host reap record {} does not match recovery scope",
                path.display()
            );
        }
        for killed in &record.killed_cgroup_paths {
            if Path::new(killed).exists() {
                bail!("host reap record is stale: killed cgroup was re-created at {killed}");
            }
        }
        killed_paths.extend(record.killed_cgroup_paths);
    }
    killed_paths.sort();
    killed_paths.dedup();
    let proof = PhysicalRecoveryProof::new(
        request.receipt_id.clone(),
        lease,
        fence,
        tenant_id,
        agent_id,
        conversation_id,
        &intents,
        killed_paths,
    );
    persist_json(
        &proof_dir.join(format!("{}.{}", request.receipt_id, PROOF_EXT)),
        &proof,
    )
}

fn request_to_intents(
    request: &SupervisorRecoveryRequest,
) -> Result<Vec<PhysicalRecoveryIntentRequest>> {
    request
        .intents
        .iter()
        .map(|intent| {
            Ok(PhysicalRecoveryIntentRequest {
                tool_call_id: intent.tool_call_id.clone(),
                tool_name: intent.tool_name.clone(),
                command_id: intent.command_id.clone(),
                run_id: intent.run_id.clone(),
                executor_generation: ProcessGeneration::from_wire(intent.executor_generation)
                    .map_err(|error| {
                        anyhow::anyhow!("invalid supervisor request generation: {error}")
                    })?,
            })
        })
        .collect()
}

fn reaped_record_path(proof_dir: &Path, generation: u64) -> PathBuf {
    proof_dir.join(REAP_DIR).join(format!("g{generation}.json"))
}

fn ensure_host_proof_dir(proof_dir: &Path) -> Result<()> {
    if !proof_dir.exists() {
        std::fs::create_dir_all(proof_dir).with_context(|| {
            format!(
                "failed to create host proof directory {}",
                proof_dir.display()
            )
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(proof_dir, std::fs::Permissions::from_mode(0o700))
                .with_context(|| {
                    format!(
                        "failed to protect newly-created host proof directory {}",
                        proof_dir.display()
                    )
                })?;
        }
    }
    std::fs::create_dir_all(proof_dir.join(REAP_DIR)).with_context(|| {
        format!(
            "failed to create host proof directory {}",
            proof_dir.display()
        )
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::{MetadataExt, PermissionsExt};
        let metadata = std::fs::metadata(proof_dir)?;
        let mode = metadata.permissions().mode();
        if mode & 0o022 != 0 {
            bail!(
                "host proof directory must not be group/world writable: {}",
                proof_dir.display()
            );
        }
        if metadata.uid() != unsafe { libc::geteuid() } {
            bail!(
                "host proof directory {} is not owned by this supervisor process",
                proof_dir.display()
            );
        }
    }
    Ok(())
}

async fn apply_recovery_receipt(
    writer: &EventWriter,
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
    sorted_intents: &[PhysicalRecoveryIntentRequest],
    receipt_id: String,
) -> Result<PhysicalRecoveryReceipt> {
    let (outcome, _seqs, receipt) = writer
        .apply_physical_recovery(lease, fence, |next_seq| {
            build_recovery_batch(sorted_intents, receipt_id, lease, fence, next_seq)
        })
        .await
        .context("failed to apply physical recovery receipt through EventWriter")?;

    match outcome {
        ApplyReceiptOutcome::Applied => {
            tracing::info!(
                receipt_id = %receipt.receipt_id,
                intents = receipt.intents.len(),
                first_seq = receipt.logical_suffix_first_seq,
                last_seq = receipt.logical_suffix_last_seq,
                "physical recovery receipt applied"
            );
        }
        ApplyReceiptOutcome::AlreadyApplied => {
            tracing::info!(receipt_id = %receipt.receipt_id, "physical recovery receipt already applied");
        }
    }

    Ok(receipt)
}

fn build_recovery_batch(
    sorted_intents: &[PhysicalRecoveryIntentRequest],
    receipt_id: String,
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
    next_seq: u64,
) -> Result<(EventBatch, PhysicalRecoveryReceipt)> {
    let mut writes = Vec::with_capacity(sorted_intents.len() * 3 + 1);
    let mut receipt_intents = Vec::with_capacity(sorted_intents.len());
    let mut cursor = next_seq;

    for intent in sorted_intents {
        let first_seq = cursor;

        writes.extend(tool_finish_writes(&intent.tool_call_id, &intent.tool_name));

        receipt_intents.push(PhysicalRecoveryIntent {
            tool_call_id: intent.tool_call_id.clone(),
            command_id: intent.command_id.clone(),
            run_id: intent.run_id.clone(),
            executor_generation: intent.executor_generation,
            indeterminate_terminal_seq: first_seq,
        });

        cursor = cursor
            .checked_add(3)
            .context("durable event sequence overflow")?;
    }

    let logical_suffix_first_seq = next_seq;
    let logical_suffix_last_seq = cursor
        .checked_sub(1)
        .context("durable event sequence overflow")?;

    let mut receipt = PhysicalRecoveryReceipt {
        receipt_id,
        lease: lease.clone(),
        fence: fence.clone(),
        intents: receipt_intents,
        logical_suffix_first_seq,
        logical_suffix_last_seq,
        digest: String::new(),
    };
    receipt.digest = receipt.canonical_digest();

    writes.push(EventWrite {
        event: None,
        projections: vec![Projection::PhysicalRecovery(receipt.clone())],
    });

    Ok((
        EventBatch {
            writes,
            injected_commands: Vec::new(),
        },
        receipt,
    ))
}

fn deterministic_receipt_id(
    lease: &ProcessGenerationLease,
    fence: &GenerationRecoveryFence,
    intents: &[PhysicalRecoveryIntentRequest],
) -> String {
    let mut hasher = Sha256::new();
    hasher.update(b"sumi-physical-recovery-receipt-id/v1");
    hasher.update(lease.lease_id().as_bytes());
    hasher.update(fence.fence_id().as_bytes());
    for intent in intents {
        hasher.update(intent.tool_call_id.as_bytes());
    }
    let digest = hasher.finalize();
    let mut encoded = String::with_capacity(digest.len() * 2);
    for byte in digest.iter() {
        encoded.push_str(&format!("{byte:02x}"));
    }
    format!("receipt-{encoded}")
}

fn tool_finish_writes(tool_call_id: &str, tool_name: &str) -> Vec<EventWrite> {
    let text = RECOVERED_TEXT;
    let result = PublicMessage::ToolResult(ToolResultMessage {
        tool_call_id: tool_call_id.to_owned(),
        tool_name: tool_name.to_owned(),
        content: vec![UserContent::Text {
            text: text.to_owned(),
        }],
        details: json!({ "text": text }),
        is_error: true,
        timestamp: Utc::now(),
    });
    let message_id = format!("{tool_call_id}-result");

    vec![
        EventWrite {
            event: Some(
                crate::store::DurableEvent::tool_execution_end(
                    tool_call_id.to_owned(),
                    serde_json::to_value(&result).expect("tool result serializes"),
                    true,
                    "indeterminate".to_owned(),
                    Some("indeterminate".to_owned()),
                )
                .expect("typed ToolExecutionEnd"),
            ),
            projections: vec![Projection::ToolExecution(ToolExecutionMutation::Finish {
                tool_call_id: tool_call_id.to_owned(),
                expected: "running",
                state: "indeterminate",
                error_code: Some("indeterminate"),
            })],
        },
        EventWrite {
            event: Some(
                crate::store::DurableEvent::message("message_start", &message_id, &result)
                    .expect("tool result MessageStart"),
            ),
            projections: Vec::new(),
        },
        EventWrite {
            event: Some(
                crate::store::DurableEvent::message("message_end", &message_id, &result)
                    .expect("tool result MessageEnd"),
            ),
            projections: vec![Projection::MessageEnd {
                message_id,
                role: "tool_result",
                message: result,
                append_to_l0: true,
                provider_context: Vec::new(),
                eviction_footprint_tokens: 0,
            }],
        },
    ]
}

#[cfg(test)]
mod tests {
    use std::{
        os::unix::fs::PermissionsExt,
        path::PathBuf,
        sync::{Arc, Mutex},
    };

    use super::*;
    use crate::runtime::contracts::{
        GenerationRecoveryFence, ProcessGeneration, ProcessGenerationLease,
    };
    use crate::store::{EventWriter, Store};

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn deterministic_receipt_id_is_stable_for_same_intents() {
        let lease =
            ProcessGenerationLease::new(ProcessGeneration::from_wire(1).unwrap(), "lease-1")
                .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-for-lease-1").unwrap();
        let intents = vec![PhysicalRecoveryIntentRequest {
            tool_call_id: "tool-1".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "cmd-1".to_owned(),
            run_id: "run-1".to_owned(),
            executor_generation: ProcessGeneration::from_wire(1).unwrap(),
        }];
        let id1 = deterministic_receipt_id(&lease, &fence, &intents);
        let id2 = deterministic_receipt_id(&lease, &fence, &intents);
        assert_eq!(id1, id2);
        assert!(id1.starts_with("receipt-"));
    }

    #[test]
    fn deterministic_receipt_id_changes_with_tool_call_id() {
        let lease =
            ProcessGenerationLease::new(ProcessGeneration::from_wire(1).unwrap(), "lease-1")
                .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-for-lease-1").unwrap();
        let intents_a = vec![PhysicalRecoveryIntentRequest {
            tool_call_id: "tool-a".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "cmd-1".to_owned(),
            run_id: "run-1".to_owned(),
            executor_generation: ProcessGeneration::from_wire(1).unwrap(),
        }];
        let intents_b = vec![PhysicalRecoveryIntentRequest {
            tool_call_id: "tool-b".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "cmd-1".to_owned(),
            run_id: "run-1".to_owned(),
            executor_generation: ProcessGeneration::from_wire(1).unwrap(),
        }];
        assert_ne!(
            deterministic_receipt_id(&lease, &fence, &intents_a),
            deterministic_receipt_id(&lease, &fence, &intents_b)
        );
    }

    #[test]
    fn tool_finish_writes_uses_intent_tool_name_not_bash_default() {
        let writes = tool_finish_writes("tool-1", "custom_tool");
        assert_eq!(writes.len(), 3);

        let message_end = writes
            .iter()
            .find_map(|write| match &write.projections[..] {
                [Projection::MessageEnd { message, .. }] => Some(message),
                _ => None,
            })
            .expect("message_end projection with the result message");
        match message_end {
            PublicMessage::ToolResult(result) => {
                assert_eq!(result.tool_name, "custom_tool");
                assert_eq!(result.tool_call_id, "tool-1");
                assert!(result.is_error);
            }
            _ => panic!("expected a tool result message"),
        }
    }

    #[test]
    fn proof_digest_changes_with_intent_set() {
        let lease =
            ProcessGenerationLease::new(ProcessGeneration::from_wire(1).unwrap(), "lease-1")
                .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-for-lease-1").unwrap();
        let a = vec![ProofIntent {
            tool_call_id: "a".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "c".to_owned(),
            run_id: "r".to_owned(),
            executor_generation: 1,
        }];
        let b = vec![ProofIntent {
            tool_call_id: "b".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "c".to_owned(),
            run_id: "r".to_owned(),
            executor_generation: 1,
        }];
        assert_ne!(
            proof_digest(&lease, &fence, "t", "a", "c", &a, &[]),
            proof_digest(&lease, &fence, "t", "a", "c", &b, &[])
        );
    }

    #[test]
    fn physical_recovery_proof_validates_against_matching_recovery() {
        let lease =
            ProcessGenerationLease::new(ProcessGeneration::from_wire(7).unwrap(), "lease-7")
                .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-for-lease-7").unwrap();
        let intents = vec![PhysicalRecoveryIntentRequest {
            tool_call_id: "tool-1".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "cmd-1".to_owned(),
            run_id: "run-1".to_owned(),
            executor_generation: ProcessGeneration::from_wire(6).unwrap(),
        }];

        let proof = PhysicalRecoveryProof::new(
            deterministic_receipt_id(&lease, &fence, &intents),
            &lease,
            &fence,
            "t",
            "a",
            "c",
            &intents,
            vec!["/sys/fs/cgroup/sumi-test-killed".to_owned()],
        );

        assert!(
            proof
                .validate_for(&lease, &fence, "t", "a", "c", &intents)
                .is_ok()
        );
    }

    #[test]
    fn physical_recovery_proof_rejects_mismatched_intent() {
        let lease =
            ProcessGenerationLease::new(ProcessGeneration::from_wire(7).unwrap(), "lease-7")
                .unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-for-lease-7").unwrap();
        let intents = vec![PhysicalRecoveryIntentRequest {
            tool_call_id: "tool-1".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "cmd-1".to_owned(),
            run_id: "run-1".to_owned(),
            executor_generation: ProcessGeneration::from_wire(6).unwrap(),
        }];
        let mut tampered = intents.clone();
        tampered[0].tool_call_id = "tool-2".to_owned();

        let proof = PhysicalRecoveryProof::new(
            deterministic_receipt_id(&lease, &fence, &intents),
            &lease,
            &fence,
            "t",
            "a",
            "c",
            &intents,
            vec!["/sys/fs/cgroup/sumi-test-killed".to_owned()],
        );

        assert!(
            proof
                .validate_for(&lease, &fence, "t", "a", "c", &tampered)
                .is_err()
        );
    }

    #[test]
    fn runtime_writable_request_directory_cannot_be_used_as_proof_store() {
        let dir = temp_state_dir();
        std::fs::create_dir_all(&dir).unwrap();
        #[cfg(unix)]
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o700)).unwrap();
        assert!(ensure_supervisor_proof_boundary(&dir, &dir).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn temp_state_dir() -> PathBuf {
        std::env::temp_dir().join(format!("sumi-t27-proof-{}", uuid::Uuid::now_v7()))
    }

    #[tokio::test]
    async fn local_supervisor_harness_orders_reap_proof_then_logical_receipt() {
        // This is deliberately a local protocol harness, not a claim that the
        // host provides a delegated Cloud cgroup boundary. It exercises the
        // cross-authority ordering with a pre-recorded host reap result.
        let state_dir = temp_state_dir();
        std::fs::create_dir_all(&state_dir).expect("create state dir");
        let request_dir = state_dir.join("runtime-requests");
        let proof_dir = state_dir.join("host-proofs");
        std::fs::create_dir_all(&request_dir).expect("create request dir");
        std::fs::create_dir_all(&proof_dir).expect("create proof dir");
        #[cfg(unix)]
        std::fs::set_permissions(&proof_dir, std::fs::Permissions::from_mode(0o700))
            .expect("protect proof dir");

        let store = Arc::new(
            Store::session_test_store("t27-consume-test")
                .await
                .expect("open test store"),
        );
        let writer = EventWriter::new(store.clone());
        writer
            .initialize_recovery_checkpoint()
            .await
            .expect("initialize checkpoint");

        let tenant = "t-consume";
        let agent = "a-consume";
        let conversation = "c-consume";
        let current_gen = ProcessGeneration::from_wire(7).unwrap();
        let old_gen = ProcessGeneration::from_wire(6).unwrap();

        let lease = ProcessGenerationLease::new(current_gen, "lease-consume").unwrap();
        let fence = GenerationRecoveryFence::new(&lease, "fence-for-lease-consume").unwrap();
        let intents = vec![PhysicalRecoveryIntentRequest {
            tool_call_id: "tool-consume".to_owned(),
            tool_name: "bash".to_owned(),
            command_id: "cmd-consume".to_owned(),
            run_id: "run-consume".to_owned(),
            executor_generation: old_gen,
        }];

        let record = ReapedGenerationProof {
            version: PROOF_VERSION,
            tenant_id: tenant.to_owned(),
            agent_id: agent.to_owned(),
            conversation_id: conversation.to_owned(),
            generation: old_gen.as_u64(),
            lease_id: "old-lease".to_owned(),
            fence_id: "old-fence".to_owned(),
            killed_cgroup_paths: vec![
                state_dir
                    .join("already-reaped-executor")
                    .display()
                    .to_string(),
            ],
            reaped_at: Utc::now().to_rfc3339(),
        };
        std::fs::create_dir_all(proof_dir.join(REAP_DIR)).expect("create reaped dir");
        persist_json(&reaped_record_path(&proof_dir, old_gen.as_u64()), &record)
            .expect("persist host reap proof");

        let request =
            SupervisorRecoveryRequest::new(&lease, &fence, tenant, agent, conversation, &intents);
        let request_path = request_dir.join(format!("{}.{}", request.receipt_id, REQUEST_EXT));
        persist_json(&request_path, &request).expect("runtime submits request only");
        supervisor_fulfill_recovery_request(
            &request_path,
            &proof_dir,
            tenant,
            agent,
            conversation,
            &lease,
            &fence,
        )
        .expect("host supervisor binds reap record to exact request");

        let _guard = ENV_LOCK.lock().unwrap();
        unsafe { std::env::set_var("SUMI_T27_FAILPOINT", "after-proof-persist") };
        let result = consume_physical_recovery(
            &writer,
            &lease,
            &fence,
            intents.clone(),
            tenant,
            agent,
            conversation,
            &request_dir,
            &proof_dir,
        )
        .await;
        unsafe { std::env::remove_var("SUMI_T27_FAILPOINT") };
        assert!(
            result.unwrap_err().to_string().contains("T27 failpoint"),
            "runtime must reach the post-proof boundary before logical recovery"
        );
        assert!(
            proof_dir
                .join(format!("{}.{}", request.receipt_id, PROOF_EXT))
                .is_file()
        );

        let mut mismatched = intents;
        mismatched[0].tool_call_id = "replayed-other-tool".to_owned();
        let proof: PhysicalRecoveryProof = serde_json::from_slice(
            &std::fs::read(proof_dir.join(format!("{}.{}", request.receipt_id, PROOF_EXT)))
                .unwrap(),
        )
        .unwrap();
        assert!(
            proof
                .validate_for(&lease, &fence, tenant, agent, conversation, &mismatched)
                .is_err(),
            "mismatched proof must fail closed before logical recovery"
        );

        let _ = std::fs::remove_dir_all(&state_dir);
    }
}
