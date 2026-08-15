//! Provider-neutral policy-source state and the ADR 0013 decision lattice.
//!
//! App adapters own operation vocabulary. This module only combines a
//! versioned built-in baseline with an authenticated overlay; it never
//! interprets Messaging or any other app action name.

#![allow(dead_code)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const BUILT_IN_POLICY_VERSION_V1: &str = "built-in-policy/v1";

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
    pub fn required_unavailable_v1(reason: impl Into<String>, minimum_version: u64) -> Self {
        Self::RequiredUnavailable {
            baseline_version: BUILT_IN_POLICY_VERSION_V1.to_owned(),
            reason: reason.into(),
            minimum_version,
        }
    }

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
pub enum PolicySourceError {
    #[error("verified policy overlay digest must not be empty")]
    InvalidDigest,
    #[error("verified policy overlay is already expired")]
    Expired,
    #[error("verified policy overlay version {observed} is below required version {minimum}")]
    VersionBelowRequirement { observed: u64, minimum: u64 },
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
}
