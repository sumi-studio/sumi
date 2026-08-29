//! Provider-neutral policy-source state and the ADR 0013 decision lattice.
//!
//! App adapters own operation vocabulary. This module only combines a
//! versioned built-in baseline with an authenticated overlay; it never
//! interprets Messaging or any other app action name.

use std::collections::BTreeMap;

use anyhow::{Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};

use crate::tools::{BoundToolInvocation, CapabilityClass};

pub const BUILT_IN_POLICY_VERSION_V1: &str = "built-in-policy/v1";
const POLICY_SOURCE_DIGEST_DOMAIN: &[u8] = b"sumi-route-policy-source/v1\0";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NormalPolicyDecision {
    Allow,
    Deny { reason: String },
    Unmatched,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case", deny_unknown_fields)]
pub enum PolicySourceState {
    BaselineOnly {
        baseline_version: String,
    },
    VerifiedOverlay {
        baseline_version: String,
        bundle_version: u64,
        bundle_digest: String,
        expires_at: DateTime<Utc>,
        required_minimum_version: Option<u64>,
    },
    RequiredUnavailable {
        baseline_version: String,
        reason: String,
        minimum_version: u64,
    },
}

impl PolicySourceState {
    pub fn baseline_only_v1() -> Self {
        Self::BaselineOnly {
            baseline_version: BUILT_IN_POLICY_VERSION_V1.to_owned(),
        }
    }

    /// Reconstruct a durable requirement when its verified overlay is absent.
    /// A cache miss cannot erase this authenticated requirement and silently
    /// fall back to baseline-only operation.
    #[allow(
        dead_code,
        reason = "required-source construction is exercised by policy recovery tests"
    )]
    pub fn required_unavailable_v1(reason: impl Into<String>, minimum_version: u64) -> Self {
        Self::RequiredUnavailable {
            baseline_version: BUILT_IN_POLICY_VERSION_V1.to_owned(),
            reason: reason.into(),
            minimum_version,
        }
    }

    #[allow(
        dead_code,
        reason = "verified overlays are exercised by signed policy tests"
    )]
    pub fn verified_overlay_v1(
        bundle_version: u64,
        bundle_digest: impl Into<String>,
        expires_at: DateTime<Utc>,
        required_minimum_version: Option<u64>,
        now: DateTime<Utc>,
    ) -> Result<Self, PolicySourceError> {
        let bundle_digest = bundle_digest.into();
        if bundle_digest.is_empty() {
            return Err(PolicySourceError::InvalidDigest);
        }
        if now >= expires_at {
            return Err(PolicySourceError::Expired);
        }
        if required_minimum_version.is_some_and(|minimum| bundle_version < minimum) {
            return Err(PolicySourceError::VersionBelowRequirement {
                observed: bundle_version,
                minimum: required_minimum_version.expect("checked Some"),
            });
        }
        Ok(Self::VerifiedOverlay {
            baseline_version: BUILT_IN_POLICY_VERSION_V1.to_owned(),
            bundle_version,
            bundle_digest,
            expires_at,
            required_minimum_version,
        })
    }

    /// Re-evaluate source availability at one authenticated decision time.
    /// Expiry of a required overlay stays unavailable; it never downgrades to
    /// BaselineOnly.
    pub fn at(&self, now: DateTime<Utc>) -> Self {
        match self {
            Self::VerifiedOverlay {
                baseline_version,
                bundle_version,
                expires_at,
                required_minimum_version: Some(minimum_version),
                ..
            } if now >= *expires_at => Self::RequiredUnavailable {
                baseline_version: baseline_version.clone(),
                reason: format!("required policy overlay {bundle_version} expired"),
                minimum_version: *minimum_version,
            },
            Self::VerifiedOverlay {
                baseline_version, ..
            } if self.is_expired_at(now) => Self::BaselineOnly {
                baseline_version: baseline_version.clone(),
            },
            state => state.clone(),
        }
    }

    fn is_expired_at(&self, now: DateTime<Utc>) -> bool {
        matches!(self, Self::VerifiedOverlay { expires_at, .. } if now >= *expires_at)
    }

    pub fn evaluate_normal(
        &self,
        baseline: NormalPolicyDecision,
        overlay: Option<NormalPolicyDecision>,
        now: DateTime<Utc>,
    ) -> PolicyGateDecision {
        match self.at(now) {
            Self::RequiredUnavailable {
                reason,
                minimum_version,
                ..
            } => PolicyGateDecision::Unavailable {
                reason,
                minimum_version,
            },
            Self::BaselineOnly { .. } => PolicyGateDecision::Ready(baseline),
            Self::VerifiedOverlay { .. } => {
                let overlay = overlay.unwrap_or(NormalPolicyDecision::Unmatched);
                PolicyGateDecision::Ready(combine_baseline_and_overlay(baseline, overlay))
            }
        }
    }

    /// Elevated calls do not use Normal Allow/Unmatched, but a required
    /// unavailable policy source blocks before reviewer or Human prompt.
    pub fn gate_elevated(&self, now: DateTime<Utc>) -> PolicyAvailability {
        match self.at(now) {
            Self::RequiredUnavailable {
                reason,
                minimum_version,
                ..
            } => PolicyAvailability::Unavailable {
                reason,
                minimum_version,
            },
            _ => PolicyAvailability::Ready,
        }
    }

    pub fn digest_at(&self, now: DateTime<Utc>) -> Result<String> {
        let state = self.at(now);
        let encoded = serde_json::to_vec(&state)?;
        let mut digest = Sha256::new();
        digest.update(POLICY_SOURCE_DIGEST_DOMAIN);
        digest.update((encoded.len() as u64).to_be_bytes());
        digest.update(encoded);
        Ok(hex(&digest.finalize()))
    }

    pub fn valid_until(&self) -> Option<DateTime<Utc>> {
        match self {
            Self::VerifiedOverlay { expires_at, .. } => Some(*expires_at),
            Self::BaselineOnly { .. } | Self::RequiredUnavailable { .. } => None,
        }
    }

    pub fn bundle_version(&self) -> Option<u64> {
        match self {
            Self::VerifiedOverlay { bundle_version, .. } => Some(*bundle_version),
            Self::BaselineOnly { .. } | Self::RequiredUnavailable { .. } => None,
        }
    }
}

/// Versioned, operation-agnostic default. Reading app-visible state is a
/// direct fast path; every capability that can change state or execute code is
/// Unmatched unless an authenticated overlay narrows or allows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltInPolicyV1;

impl BuiltInPolicyV1 {
    pub fn evaluate(&self, capability: &CapabilityClass) -> NormalPolicyDecision {
        match capability {
            CapabilityClass::Read => NormalPolicyDecision::Allow,
            CapabilityClass::Mutate | CapabilityClass::Administer | CapabilityClass::Execute => {
                NormalPolicyDecision::Unmatched
            }
        }
    }
}

/// Foundation policy over the app adapter's small capability vocabulary.
/// Operation names never enter this type. An overlay map is accepted only
/// together with a verified source identity and is therefore not a local
/// unsigned standing-policy escape hatch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoutePolicy {
    source: PolicySourceState,
    baseline: BuiltInPolicyV1,
    overlay: BTreeMap<CapabilityClass, NormalPolicyDecision>,
}

impl RoutePolicy {
    pub fn baseline_only_v1() -> Self {
        Self {
            source: PolicySourceState::baseline_only_v1(),
            baseline: BuiltInPolicyV1,
            overlay: BTreeMap::new(),
        }
    }

    #[allow(
        dead_code,
        reason = "required policy construction is exercised by recovery tests"
    )]
    pub fn required_unavailable_v1(reason: impl Into<String>, minimum_version: u64) -> Self {
        Self {
            source: PolicySourceState::required_unavailable_v1(reason, minimum_version),
            baseline: BuiltInPolicyV1,
            overlay: BTreeMap::new(),
        }
    }

    #[allow(
        dead_code,
        reason = "verified policy overlays are exercised by signed policy tests"
    )]
    pub fn verified_overlay_v1(
        source: PolicySourceState,
        overlay: BTreeMap<CapabilityClass, NormalPolicyDecision>,
    ) -> Result<Self> {
        if !matches!(source, PolicySourceState::VerifiedOverlay { .. }) {
            bail!("route policy overlay requires a verified source state");
        }
        Ok(Self {
            source,
            baseline: BuiltInPolicyV1,
            overlay,
        })
    }

    #[allow(
        dead_code,
        reason = "exact authenticated policy-source inspection is retained for recovery consumers"
    )]
    pub fn source(&self) -> &PolicySourceState {
        &self.source
    }

    pub fn evaluate_normal(
        &self,
        invocation: &BoundToolInvocation,
        now: DateTime<Utc>,
    ) -> PolicyEvaluation {
        let baseline = self.baseline.evaluate(&invocation.descriptor.capability);
        let overlay = self.overlay.get(&invocation.descriptor.capability).cloned();
        let source = self.source.at(now);
        match self.source.evaluate_normal(baseline, overlay, now) {
            PolicyGateDecision::Ready(decision) => PolicyEvaluation::Ready {
                snapshot: PolicySnapshot::new(source, now),
                decision,
            },
            PolicyGateDecision::Unavailable {
                reason,
                minimum_version,
            } => PolicyEvaluation::Unavailable {
                snapshot: PolicySnapshot::new(source, now),
                reason,
                minimum_version,
            },
        }
    }

    pub fn evaluate_elevated(
        &self,
        invocation: &BoundToolInvocation,
        now: DateTime<Utc>,
    ) -> ElevatedPolicyEvaluation {
        let source = self.source.at(now);
        if let PolicyAvailability::Unavailable {
            reason,
            minimum_version,
        } = self.source.gate_elevated(now)
        {
            return ElevatedPolicyEvaluation::Unavailable {
                snapshot: PolicySnapshot::new(source, now),
                reason,
                minimum_version,
            };
        }
        let baseline = self.baseline.evaluate(&invocation.descriptor.capability);
        let overlay = self.overlay.get(&invocation.descriptor.capability);
        let deny = match (baseline, overlay) {
            (NormalPolicyDecision::Deny { reason }, _) => Some(reason),
            (_, Some(NormalPolicyDecision::Deny { reason })) => Some(reason.clone()),
            _ => None,
        };
        let snapshot = PolicySnapshot::new(source, now);
        match deny {
            Some(reason) => ElevatedPolicyEvaluation::Deny { snapshot, reason },
            None => ElevatedPolicyEvaluation::Ready { snapshot },
        }
    }

    pub fn snapshot_matches(&self, snapshot: &PolicySnapshot, now: DateTime<Utc>) -> bool {
        snapshot.validate().is_ok()
            && snapshot.valid_until.is_none_or(|expiry| now < expiry)
            && self
                .source
                .digest_at(now)
                .is_ok_and(|digest| digest == snapshot.source_digest)
    }

    /// Validate a live policy-source replacement without permitting a cache
    /// deletion or an older signed bundle to widen authority. Optional
    /// overlays age out through `PolicySourceState::at`; they are not removed
    /// early by replacing them with BaselineOnly.
    #[allow(
        dead_code,
        reason = "replacement validation is exercised by monotonic policy tests"
    )]
    pub fn validate_replacement(&self, next: &Self, now: DateTime<Utc>) -> Result<()> {
        let current = self.source.at(now);
        let next_source = next.source.at(now);
        match (&current, &next_source) {
            (PolicySourceState::BaselineOnly { .. }, PolicySourceState::BaselineOnly { .. }) => {}
            (PolicySourceState::BaselineOnly { .. }, PolicySourceState::VerifiedOverlay { .. })
            | (
                PolicySourceState::BaselineOnly { .. },
                PolicySourceState::RequiredUnavailable { .. },
            ) => {}
            (
                PolicySourceState::VerifiedOverlay {
                    bundle_version: current_version,
                    bundle_digest: current_digest,
                    required_minimum_version: current_minimum,
                    ..
                },
                PolicySourceState::VerifiedOverlay {
                    bundle_version: next_version,
                    bundle_digest: next_digest,
                    required_minimum_version: next_minimum,
                    ..
                },
            ) => {
                if next_version < current_version
                    || (next_version == current_version && next_digest != current_digest)
                    || current_minimum
                        .is_some_and(|minimum| next_minimum.is_none_or(|next| next < minimum))
                {
                    bail!("replacement policy overlay is older or changes authenticated identity");
                }
            }
            (
                PolicySourceState::VerifiedOverlay {
                    required_minimum_version: Some(current_minimum),
                    ..
                },
                PolicySourceState::RequiredUnavailable {
                    minimum_version, ..
                },
            ) if minimum_version >= current_minimum => {}
            (
                PolicySourceState::RequiredUnavailable {
                    minimum_version: current_minimum,
                    ..
                },
                PolicySourceState::VerifiedOverlay { bundle_version, .. },
            ) if bundle_version >= current_minimum => {}
            (
                PolicySourceState::RequiredUnavailable {
                    minimum_version: current_minimum,
                    ..
                },
                PolicySourceState::RequiredUnavailable {
                    minimum_version: next_minimum,
                    ..
                },
            ) if next_minimum >= current_minimum => {}
            _ => bail!("replacement policy would downgrade authenticated policy state"),
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicySnapshot {
    pub source: PolicySourceState,
    pub source_digest: String,
    pub evaluated_at: DateTime<Utc>,
    pub valid_until: Option<DateTime<Utc>>,
    pub bundle_version: Option<u64>,
}

impl PolicySnapshot {
    fn new(source: PolicySourceState, evaluated_at: DateTime<Utc>) -> Self {
        let source_digest = source
            .digest_at(evaluated_at)
            .unwrap_or_else(|_| "invalid-policy-source".to_owned());
        let valid_until = source.valid_until();
        let bundle_version = source.bundle_version();
        Self {
            source,
            source_digest,
            evaluated_at,
            valid_until,
            bundle_version,
        }
    }

    pub fn validate(&self) -> Result<()> {
        if self.source_digest.trim().is_empty() {
            bail!("policy snapshot source digest is empty");
        }
        if self.source.digest_at(self.evaluated_at)? != self.source_digest {
            bail!("policy snapshot source digest does not match its source state");
        }
        if self.valid_until != self.source.valid_until()
            || self.bundle_version != self.source.bundle_version()
        {
            bail!("policy snapshot metadata does not match its source state");
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyEvaluation {
    Ready {
        snapshot: PolicySnapshot,
        decision: NormalPolicyDecision,
    },
    Unavailable {
        snapshot: PolicySnapshot,
        reason: String,
        minimum_version: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ElevatedPolicyEvaluation {
    Ready {
        snapshot: PolicySnapshot,
    },
    Deny {
        snapshot: PolicySnapshot,
        reason: String,
    },
    Unavailable {
        snapshot: PolicySnapshot,
        reason: String,
        minimum_version: u64,
    },
}

fn combine_baseline_and_overlay(
    baseline: NormalPolicyDecision,
    overlay: NormalPolicyDecision,
) -> NormalPolicyDecision {
    match (baseline, overlay) {
        (NormalPolicyDecision::Deny { reason }, _) => NormalPolicyDecision::Deny { reason },
        (_, NormalPolicyDecision::Deny { reason }) => NormalPolicyDecision::Deny { reason },
        (NormalPolicyDecision::Allow, _) => NormalPolicyDecision::Allow,
        (NormalPolicyDecision::Unmatched, NormalPolicyDecision::Allow) => {
            NormalPolicyDecision::Allow
        }
        (NormalPolicyDecision::Unmatched, NormalPolicyDecision::Unmatched) => {
            NormalPolicyDecision::Unmatched
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyGateDecision {
    Ready(NormalPolicyDecision),
    Unavailable {
        reason: String,
        minimum_version: u64,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PolicyAvailability {
    Ready,
    Unavailable {
        reason: String,
        minimum_version: u64,
    },
}

#[derive(Clone, Debug, thiserror::Error, PartialEq, Eq)]
#[allow(
    dead_code,
    reason = "source failures are constructed by signed policy tests"
)]
pub enum PolicySourceError {
    #[error("verified policy overlay digest must not be empty")]
    InvalidDigest,
    #[error("verified policy overlay is already expired")]
    Expired,
    #[error("verified policy overlay version {observed} is below required version {minimum}")]
    VersionBelowRequirement { observed: u64, minimum: u64 },
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

#[cfg(test)]
mod tests {
    use chrono::Duration;

    use super::*;

    fn now() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339("2026-08-11T00:00:00Z")
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn baseline_only_is_the_normal_default() {
        let source = PolicySourceState::baseline_only_v1();
        assert_eq!(
            source.evaluate_normal(
                NormalPolicyDecision::Unmatched,
                Some(NormalPolicyDecision::Allow),
                now(),
            ),
            PolicyGateDecision::Ready(NormalPolicyDecision::Unmatched)
        );
        assert_eq!(source.gate_elevated(now()), PolicyAvailability::Ready);
    }

    #[test]
    fn verified_overlay_obeys_non_override_precedence() {
        let source = PolicySourceState::verified_overlay_v1(
            7,
            "bundle-digest",
            now() + Duration::hours(1),
            None,
            now(),
        )
        .unwrap();
        let deny = NormalPolicyDecision::Deny {
            reason: "baseline hard deny".to_owned(),
        };
        assert_eq!(
            source.evaluate_normal(deny.clone(), Some(NormalPolicyDecision::Allow), now()),
            PolicyGateDecision::Ready(deny)
        );
        assert_eq!(
            source.evaluate_normal(
                NormalPolicyDecision::Allow,
                Some(NormalPolicyDecision::Deny {
                    reason: "signed deny".to_owned(),
                }),
                now(),
            ),
            PolicyGateDecision::Ready(NormalPolicyDecision::Deny {
                reason: "signed deny".to_owned(),
            })
        );
        assert_eq!(
            source.evaluate_normal(
                NormalPolicyDecision::Unmatched,
                Some(NormalPolicyDecision::Allow),
                now(),
            ),
            PolicyGateDecision::Ready(NormalPolicyDecision::Allow)
        );
    }

    #[test]
    fn required_overlay_cannot_downgrade_when_cache_is_missing_or_expired() {
        let missing = PolicySourceState::required_unavailable_v1("cache missing", 9);
        assert!(matches!(
            missing.evaluate_normal(NormalPolicyDecision::Allow, None, now()),
            PolicyGateDecision::Unavailable {
                minimum_version: 9,
                ..
            }
        ));
        assert!(matches!(
            missing.gate_elevated(now()),
            PolicyAvailability::Unavailable {
                minimum_version: 9,
                ..
            }
        ));

        let verified = PolicySourceState::verified_overlay_v1(
            9,
            "bundle-digest",
            now() + Duration::minutes(1),
            Some(9),
            now(),
        )
        .unwrap();
        assert!(matches!(
            verified.at(now() + Duration::minutes(2)),
            PolicySourceState::RequiredUnavailable {
                minimum_version: 9,
                ..
            }
        ));
    }

    #[test]
    fn live_policy_replacement_is_monotonic_and_required_state_cannot_disappear() {
        let current_source = PolicySourceState::verified_overlay_v1(
            9,
            "bundle-9",
            now() + Duration::hours(1),
            Some(9),
            now(),
        )
        .unwrap();
        let current = RoutePolicy::verified_overlay_v1(current_source, BTreeMap::new()).unwrap();

        assert!(
            current
                .validate_replacement(&RoutePolicy::baseline_only_v1(), now())
                .is_err()
        );
        assert!(
            current
                .validate_replacement(
                    &RoutePolicy::required_unavailable_v1("cache deleted", 9),
                    now(),
                )
                .is_ok()
        );

        let older_source = PolicySourceState::verified_overlay_v1(
            8,
            "bundle-8",
            now() + Duration::hours(1),
            Some(8),
            now(),
        )
        .unwrap();
        let older = RoutePolicy::verified_overlay_v1(older_source, BTreeMap::new()).unwrap();
        assert!(current.validate_replacement(&older, now()).is_err());

        let unavailable = RoutePolicy::required_unavailable_v1("cache missing", 9);
        assert!(
            unavailable
                .validate_replacement(&RoutePolicy::baseline_only_v1(), now())
                .is_err()
        );
    }
}
