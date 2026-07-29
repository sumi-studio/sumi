//! Frozen Rust/Go wire contract for the authenticated loopback control fixture.
//!
//! This protocol is local/CI infrastructure for the first browser vertical. It
//! does not replace the production workload-identity issuer or central runtime
//! registry from issue #80.

use serde::{Deserialize, Serialize};

use super::supervisor::DeliveryAuthorization;

pub(crate) const ISSUE_CREDENTIAL_PATH: &str = "local-control/v1/runtime-credentials:issue";
pub(crate) const PUBLISH_RUNTIME_STATE_PATH: &str = "local-control/v1/runtime-state:publish";

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalCredentialIssueRequest {
    pub(crate) request_id: String,
    pub(crate) personality_agent_id: String,
    pub(crate) generation: u64,
    pub(crate) rpc_boot_nonce: String,
    pub(crate) audience: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalCredentialIssueResponse {
    pub(crate) request_id: String,
    pub(crate) personality_agent_id: String,
    pub(crate) generation: u64,
    pub(crate) rpc_boot_nonce: String,
    pub(crate) audience: String,
    pub(crate) expires_at_unix: i64,
    pub(crate) delivery_authorization: DeliveryAuthorization,
    pub(crate) token: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LocalRuntimePublicationState {
    NotReady,
    Ready,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum LocalRuntimePublicationReason {
    Startup,
    Hydrated,
    Shutdown,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalRuntimeStatePublication {
    pub(crate) publication_id: String,
    pub(crate) personality_agent_id: String,
    pub(crate) generation: u64,
    pub(crate) rpc_boot_nonce: String,
    pub(crate) expected_revision: Option<u64>,
    pub(crate) state: LocalRuntimePublicationState,
    pub(crate) hydration_receipt_identity: Option<String>,
    pub(crate) reason: LocalRuntimePublicationReason,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct LocalRuntimeStateAck {
    pub(crate) publication_id: String,
    pub(crate) personality_agent_id: String,
    pub(crate) generation: u64,
    pub(crate) rpc_boot_nonce: String,
    pub(crate) revision: u64,
    pub(crate) state: LocalRuntimePublicationState,
    pub(crate) hydration_receipt_identity: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAID: &str = "0198f0f4-9b72-7000-8000-000000000001";

    #[test]
    fn credential_issue_wire_shape_is_closed_and_exact() {
        let request = LocalCredentialIssueRequest {
            request_id: "0198f0f4-9b72-7000-8000-000000000011".to_owned(),
            personality_agent_id: PAID.to_owned(),
            generation: 7,
            rpc_boot_nonce: "boot-a".to_owned(),
            audience: "sumi:agent:events".to_owned(),
        };
        assert_eq!(
            serde_json::to_value(&request).unwrap(),
            serde_json::json!({
                "request_id": "0198f0f4-9b72-7000-8000-000000000011",
                "personality_agent_id": PAID,
                "generation": 7,
                "rpc_boot_nonce": "boot-a",
                "audience": "sumi:agent:events",
            })
        );

        let response = serde_json::json!({
            "request_id": request.request_id,
            "personality_agent_id": PAID,
            "generation": 7,
            "rpc_boot_nonce": "boot-a",
            "audience": "sumi:agent:events",
            "expires_at_unix": 1_800_000_030_i64,
            "delivery_authorization": "raw",
            "token": "opaque",
        });
        let decoded: LocalCredentialIssueResponse =
            serde_json::from_value(response.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), response);

        let mut unknown = response;
        unknown
            .as_object_mut()
            .unwrap()
            .insert("agent_id".to_owned(), serde_json::json!(PAID));
        assert!(serde_json::from_value::<LocalCredentialIssueResponse>(unknown).is_err());
    }

    #[test]
    fn runtime_state_wire_carries_cas_and_exact_receipt() {
        let publication = LocalRuntimeStatePublication {
            publication_id: "0198f0f4-9b72-7000-8000-000000000012".to_owned(),
            personality_agent_id: PAID.to_owned(),
            generation: 7,
            rpc_boot_nonce: "boot-a".to_owned(),
            expected_revision: Some(3),
            state: LocalRuntimePublicationState::Ready,
            hydration_receipt_identity: Some("receipt-a".to_owned()),
            reason: LocalRuntimePublicationReason::Hydrated,
        };
        assert_eq!(
            serde_json::to_value(&publication).unwrap(),
            serde_json::json!({
                "publication_id": "0198f0f4-9b72-7000-8000-000000000012",
                "personality_agent_id": PAID,
                "generation": 7,
                "rpc_boot_nonce": "boot-a",
                "expected_revision": 3,
                "state": "ready",
                "hydration_receipt_identity": "receipt-a",
                "reason": "hydrated",
            })
        );

        let not_ready = LocalRuntimeStatePublication {
            publication_id: "0198f0f4-9b72-7000-8000-000000000013".to_owned(),
            personality_agent_id: PAID.to_owned(),
            generation: 8,
            rpc_boot_nonce: "boot-b".to_owned(),
            expected_revision: None,
            state: LocalRuntimePublicationState::NotReady,
            hydration_receipt_identity: None,
            reason: LocalRuntimePublicationReason::Startup,
        };
        let value = serde_json::to_value(&not_ready).unwrap();
        assert_eq!(value["expected_revision"], serde_json::Value::Null);
        assert_eq!(value["hydration_receipt_identity"], serde_json::Value::Null);

        let ack = LocalRuntimeStateAck {
            publication_id: publication.publication_id,
            personality_agent_id: PAID.to_owned(),
            generation: 7,
            rpc_boot_nonce: "boot-a".to_owned(),
            revision: 4,
            state: LocalRuntimePublicationState::Ready,
            hydration_receipt_identity: Some("receipt-a".to_owned()),
        };
        let mut unknown = serde_json::to_value(ack).unwrap();
        unknown
            .as_object_mut()
            .unwrap()
            .insert("tenant_id".to_owned(), serde_json::json!("tenant-a"));
        assert!(serde_json::from_value::<LocalRuntimeStateAck>(unknown).is_err());
    }
}
