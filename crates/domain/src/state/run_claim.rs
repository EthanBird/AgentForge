//! Independent, replayable claim aggregate for one invocation run.
//!
//! A run claim is scheduler authority to execute and report one
//! [`InvocationRunId`]. It is deliberately separate from the author
//! [`crate::state::lease::Lease`]
//! and from the invocation run state machine. Each successful acquisition is a
//! new aggregate/history row with a strictly increasing generation.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        AggregateVersion, InvocationRunId, NodeId, RunClaimId, RunClaimToken, ServerInstant,
        Sha256Digest,
    },
    state::Transition,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunClaimState {
    Active,
    Completed,
    Expired,
    Revoked,
    Superseded,
}

impl RunClaimState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Completed => "completed",
            Self::Expired => "expired",
            Self::Revoked => "revoked",
            Self::Superseded => "superseded",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Active)
    }
}

/// Immutable identity copied into an InvocationRun at reservation time.
///
/// This is not a liveness proof. Callers must also load the independent
/// [`RunClaim`] and call [`RunClaim::authorize_for_run`] with server time.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunClaimBinding {
    pub claim_id: RunClaimId,
    pub claim_generation: RunClaimToken,
    pub holder_node_id: NodeId,
}

impl RunClaimBinding {
    pub fn validate(self) -> Result<(), DomainError> {
        if self.claim_id.as_uuid().is_nil() || self.holder_node_id.as_uuid().is_nil() {
            return Err(DomainError::InvocationClaimStale);
        }
        Ok(())
    }

    pub fn validate_proof(self, proof: RunClaimProof) -> Result<(), DomainError> {
        self.validate()?;
        proof.validate()?;
        if proof.claim_id != self.claim_id
            || proof.claim_generation != self.claim_generation
            || proof.holder_node_id != self.holder_node_id
        {
            return Err(DomainError::InvocationClaimStale);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunClaimProof {
    pub claim_id: RunClaimId,
    pub claim_generation: RunClaimToken,
    pub holder_node_id: NodeId,
}

impl RunClaimProof {
    pub fn validate(self) -> Result<(), DomainError> {
        RunClaimBinding::from(self).validate()
    }
}

impl From<RunClaimProof> for RunClaimBinding {
    fn from(proof: RunClaimProof) -> Self {
        Self {
            claim_id: proof.claim_id,
            claim_generation: proof.claim_generation,
            holder_node_id: proof.holder_node_id,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrantRunClaim {
    pub id: RunClaimId,
    pub run_id: InvocationRunId,
    /// None means the first generation and therefore requires generation 1.
    pub previous_generation: Option<RunClaimToken>,
    pub claim_generation: RunClaimToken,
    pub holder_node_id: NodeId,
    pub granted_at: ServerInstant,
    pub expires_at: ServerInstant,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum RunClaimCommandKind {
    Grant,
    Renew,
    Release,
    Expire,
    Revoke,
    Supersede,
}

impl RunClaimCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Grant => "grant_run_claim",
            Self::Renew => "renew_run_claim",
            Self::Release => "release_run_claim",
            Self::Expire => "expire_run_claim",
            Self::Revoke => "revoke_run_claim",
            Self::Supersede => "supersede_run_claim",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunClaimCommand {
    Grant(GrantRunClaim),
    Renew {
        expected_version: AggregateVersion,
        proof: RunClaimProof,
        now: ServerInstant,
        new_expires_at: ServerInstant,
    },
    Release {
        expected_version: AggregateVersion,
        proof: RunClaimProof,
        result_digest: Option<Sha256Digest>,
        released_at: ServerInstant,
    },
    Expire {
        expected_version: AggregateVersion,
        expired_at: ServerInstant,
    },
    Revoke {
        expected_version: AggregateVersion,
        reason_code: String,
        revoked_at: ServerInstant,
    },
    Supersede {
        expected_version: AggregateVersion,
        superseding_claim_id: RunClaimId,
        superseding_generation: RunClaimToken,
        superseded_at: ServerInstant,
    },
}

impl RunClaimCommand {
    #[must_use]
    pub const fn kind(&self) -> RunClaimCommandKind {
        match self {
            Self::Grant(_) => RunClaimCommandKind::Grant,
            Self::Renew { .. } => RunClaimCommandKind::Renew,
            Self::Release { .. } => RunClaimCommandKind::Release,
            Self::Expire { .. } => RunClaimCommandKind::Expire,
            Self::Revoke { .. } => RunClaimCommandKind::Revoke,
            Self::Supersede { .. } => RunClaimCommandKind::Supersede,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        match self {
            Self::Grant(_) => None,
            Self::Renew {
                expected_version, ..
            }
            | Self::Release {
                expected_version, ..
            }
            | Self::Expire {
                expected_version, ..
            }
            | Self::Revoke {
                expected_version, ..
            }
            | Self::Supersede {
                expected_version, ..
            } => Some(*expected_version),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RunClaimEvent {
    Granted(GrantRunClaim),
    Renewed {
        proof: RunClaimProof,
        renewed_at: ServerInstant,
        previous_expires_at: ServerInstant,
        expires_at: ServerInstant,
    },
    Released {
        proof: RunClaimProof,
        result_digest: Option<Sha256Digest>,
        released_at: ServerInstant,
    },
    Expired {
        expired_at: ServerInstant,
    },
    Revoked {
        reason_code: String,
        revoked_at: ServerInstant,
    },
    Superseded {
        superseding_claim_id: RunClaimId,
        superseding_generation: RunClaimToken,
        superseded_at: ServerInstant,
    },
}

impl RunClaimEvent {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Granted(_) => "run_claim_granted",
            Self::Renewed { .. } => "run_claim_renewed",
            Self::Released { .. } => "run_claim_released",
            Self::Expired { .. } => "run_claim_expired",
            Self::Revoked { .. } => "run_claim_revoked",
            Self::Superseded { .. } => "run_claim_superseded",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunClaim {
    pub id: RunClaimId,
    pub run_id: InvocationRunId,
    pub claim_generation: RunClaimToken,
    pub holder_node_id: NodeId,
    pub state: RunClaimState,
    pub granted_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub updated_at: ServerInstant,
    pub terminal_at: Option<ServerInstant>,
    pub result_digest: Option<Sha256Digest>,
    pub terminal_reason: Option<String>,
    pub superseding_claim_id: Option<RunClaimId>,
    pub superseding_generation: Option<RunClaimToken>,
    pub version: AggregateVersion,
}

impl RunClaim {
    pub fn transition(
        current: Option<&Self>,
        command: &RunClaimCommand,
    ) -> Result<Transition<Self, RunClaimEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &RunClaimCommand,
    ) -> Result<RunClaimEvent, DomainError> {
        match (current, command) {
            (None, RunClaimCommand::Grant(grant)) => {
                validate_grant(grant)?;
                Ok(RunClaimEvent::Granted(grant.clone()))
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "run_claim",
            }),
            (Some(claim), RunClaimCommand::Grant(_)) => {
                Err(invalid_transition(claim.state, command.kind()))
            }
            (Some(claim), command) => {
                // Receipt lookup belongs before this boundary. On a receipt
                // miss, terminality wins over CAS so an old key cannot make a
                // terminal claim appear retryable merely via a stale version.
                if claim.state.is_terminal() {
                    return Err(invalid_transition(claim.state, command.kind()));
                }
                if command.expected_version() != Some(claim.version) {
                    return Err(DomainError::InvocationClaimStale);
                }
                match command {
                    RunClaimCommand::Renew {
                        proof,
                        now,
                        new_expires_at,
                        ..
                    } => {
                        claim.authorize_for_run(claim.run_id, *proof, *now)?;
                        validate_renewal(claim, *now, *new_expires_at)?;
                        Ok(RunClaimEvent::Renewed {
                            proof: *proof,
                            renewed_at: *now,
                            previous_expires_at: claim.expires_at,
                            expires_at: *new_expires_at,
                        })
                    }
                    RunClaimCommand::Release {
                        proof,
                        result_digest,
                        released_at,
                        ..
                    } => {
                        claim.authorize_for_run(claim.run_id, *proof, *released_at)?;
                        claim.validate_active_time(*released_at)?;
                        Ok(RunClaimEvent::Released {
                            proof: *proof,
                            result_digest: *result_digest,
                            released_at: *released_at,
                        })
                    }
                    RunClaimCommand::Expire { expired_at, .. } => {
                        if *expired_at < claim.expires_at || *expired_at < claim.updated_at {
                            return Err(invalid_transition(claim.state, command.kind()));
                        }
                        Ok(RunClaimEvent::Expired {
                            expired_at: *expired_at,
                        })
                    }
                    RunClaimCommand::Revoke {
                        reason_code,
                        revoked_at,
                        ..
                    } if valid_reason(reason_code) => {
                        claim.validate_active_time(*revoked_at)?;
                        Ok(RunClaimEvent::Revoked {
                            reason_code: reason_code.clone(),
                            revoked_at: *revoked_at,
                        })
                    }
                    RunClaimCommand::Supersede {
                        superseding_claim_id,
                        superseding_generation,
                        superseded_at,
                        ..
                    } => {
                        validate_supersession(
                            claim,
                            *superseding_claim_id,
                            *superseding_generation,
                            *superseded_at,
                        )?;
                        Ok(RunClaimEvent::Superseded {
                            superseding_claim_id: *superseding_claim_id,
                            superseding_generation: *superseding_generation,
                            superseded_at: *superseded_at,
                        })
                    }
                    _ => Err(invalid_transition(claim.state, command.kind())),
                }
            }
        }
    }

    pub fn apply_event(current: Option<&Self>, event: &RunClaimEvent) -> Result<Self, DomainError> {
        match (current, event) {
            (None, RunClaimEvent::Granted(grant)) => {
                validate_grant(grant)?;
                Ok(Self {
                    id: grant.id,
                    run_id: grant.run_id,
                    claim_generation: grant.claim_generation,
                    holder_node_id: grant.holder_node_id,
                    state: RunClaimState::Active,
                    granted_at: grant.granted_at,
                    expires_at: grant.expires_at,
                    updated_at: grant.granted_at,
                    terminal_at: None,
                    result_digest: None,
                    terminal_reason: None,
                    superseding_claim_id: None,
                    superseding_generation: None,
                    version: AggregateVersion::new(1),
                })
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "run_claim",
            }),
            (Some(claim), RunClaimEvent::Granted(_)) => Err(invalid_event(claim.state, event)),
            (Some(claim), event) if claim.state.is_terminal() => {
                Err(invalid_event(claim.state, event))
            }
            (
                Some(claim),
                RunClaimEvent::Renewed {
                    proof,
                    renewed_at,
                    previous_expires_at,
                    expires_at,
                },
            ) => {
                claim.authorize_for_run(claim.run_id, *proof, *renewed_at)?;
                if *previous_expires_at != claim.expires_at {
                    return Err(DomainError::InvocationClaimStale);
                }
                validate_renewal(claim, *renewed_at, *expires_at)?;
                let mut next = claim.next_version(*renewed_at)?;
                next.expires_at = *expires_at;
                Ok(next)
            }
            (
                Some(claim),
                RunClaimEvent::Released {
                    proof,
                    result_digest,
                    released_at,
                },
            ) => {
                claim.authorize_for_run(claim.run_id, *proof, *released_at)?;
                claim.terminalize(
                    RunClaimState::Completed,
                    *released_at,
                    *result_digest,
                    None,
                    None,
                    None,
                )
            }
            (Some(claim), RunClaimEvent::Expired { expired_at })
                if *expired_at >= claim.expires_at && *expired_at >= claim.updated_at =>
            {
                claim.terminalize(RunClaimState::Expired, *expired_at, None, None, None, None)
            }
            (
                Some(claim),
                RunClaimEvent::Revoked {
                    reason_code,
                    revoked_at,
                },
            ) if valid_reason(reason_code) => claim.terminalize(
                RunClaimState::Revoked,
                *revoked_at,
                None,
                Some(reason_code.clone()),
                None,
                None,
            ),
            (
                Some(claim),
                RunClaimEvent::Superseded {
                    superseding_claim_id,
                    superseding_generation,
                    superseded_at,
                },
            ) => {
                validate_supersession(
                    claim,
                    *superseding_claim_id,
                    *superseding_generation,
                    *superseded_at,
                )?;
                claim.terminalize(
                    RunClaimState::Superseded,
                    *superseded_at,
                    None,
                    None,
                    Some(*superseding_claim_id),
                    Some(*superseding_generation),
                )
            }
            (Some(claim), event) => Err(invalid_event(claim.state, event)),
        }
    }

    pub fn replay(events: &[RunClaimEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "run_claim",
        })
    }

    #[must_use]
    pub const fn binding(&self) -> RunClaimBinding {
        RunClaimBinding {
            claim_id: self.id,
            claim_generation: self.claim_generation,
            holder_node_id: self.holder_node_id,
        }
    }

    #[must_use]
    pub const fn proof(&self) -> RunClaimProof {
        RunClaimProof {
            claim_id: self.id,
            claim_generation: self.claim_generation,
            holder_node_id: self.holder_node_id,
        }
    }

    /// Verifies current scheduler authority using server time.
    pub fn authorize_for_run(
        &self,
        run_id: InvocationRunId,
        proof: RunClaimProof,
        now: ServerInstant,
    ) -> Result<(), DomainError> {
        proof.validate()?;
        if self.run_id != run_id
            || self.state != RunClaimState::Active
            || self.binding() != RunClaimBinding::from(proof)
            || now < self.updated_at
            || now >= self.expires_at
        {
            return Err(DomainError::InvocationClaimStale);
        }
        Ok(())
    }

    fn validate_active_time(&self, at: ServerInstant) -> Result<(), DomainError> {
        if at < self.updated_at || at >= self.expires_at {
            return Err(DomainError::InvocationClaimStale);
        }
        Ok(())
    }

    fn next_version(&self, at: ServerInstant) -> Result<Self, DomainError> {
        if at < self.updated_at {
            return Err(DomainError::InvalidArgument {
                field: "occurred_at".into(),
                reason: "must not precede the previous claim event".into(),
            });
        }
        let mut next = self.clone();
        next.version = self.version.checked_next()?;
        next.updated_at = at;
        Ok(next)
    }

    fn terminalize(
        &self,
        state: RunClaimState,
        terminal_at: ServerInstant,
        result_digest: Option<Sha256Digest>,
        terminal_reason: Option<String>,
        superseding_claim_id: Option<RunClaimId>,
        superseding_generation: Option<RunClaimToken>,
    ) -> Result<Self, DomainError> {
        if self.state != RunClaimState::Active
            || !state.is_terminal()
            || terminal_at < self.updated_at
            || (state != RunClaimState::Expired && terminal_at >= self.expires_at)
        {
            return Err(invalid_transition(self.state, terminal_command_kind(state)));
        }
        let mut next = self.next_version(terminal_at)?;
        next.state = state;
        next.terminal_at = Some(terminal_at);
        next.result_digest = result_digest;
        next.terminal_reason = terminal_reason;
        next.superseding_claim_id = superseding_claim_id;
        next.superseding_generation = superseding_generation;
        Ok(next)
    }
}

/// Transaction-level guard mirroring the one-ACTIVE-row database constraint.
pub fn validate_run_claim_history<'a>(
    claims: impl IntoIterator<Item = &'a RunClaim>,
) -> Result<(), DomainError> {
    let mut active_by_run = BTreeMap::new();
    let mut generations = BTreeSet::new();
    for claim in claims {
        if !generations.insert((claim.run_id, claim.claim_generation)) {
            return Err(DomainError::InvariantViolation {
                invariant: "run_claim_generation_must_be_unique_per_run",
            });
        }
        if claim.state == RunClaimState::Active
            && active_by_run.insert(claim.run_id, claim.id).is_some()
        {
            return Err(DomainError::InvariantViolation {
                invariant: "invocation_run_must_have_at_most_one_active_claim",
            });
        }
    }
    Ok(())
}

fn validate_grant(grant: &GrantRunClaim) -> Result<(), DomainError> {
    if grant.id.as_uuid().is_nil()
        || grant.run_id.as_uuid().is_nil()
        || grant.holder_node_id.as_uuid().is_nil()
    {
        return Err(DomainError::InvalidArgument {
            field: "run_claim_binding".into(),
            reason: "claim, run, and holder ids must be non-nil".into(),
        });
    }
    if grant.granted_at >= grant.expires_at {
        return Err(DomainError::InvalidArgument {
            field: "run_claim_window".into(),
            reason: "must satisfy granted_at < expires_at".into(),
        });
    }
    match grant.previous_generation {
        None if grant.claim_generation.get() == 1 => Ok(()),
        Some(previous) if previous.checked_next()? == grant.claim_generation => Ok(()),
        _ => Err(DomainError::InvalidArgument {
            field: "claim_generation".into(),
            reason: "must be one greater than the previous generation".into(),
        }),
    }
}

fn validate_renewal(
    claim: &RunClaim,
    renewed_at: ServerInstant,
    expires_at: ServerInstant,
) -> Result<(), DomainError> {
    if renewed_at < claim.updated_at
        || renewed_at >= claim.expires_at
        || expires_at <= claim.expires_at
        || expires_at <= renewed_at
    {
        return Err(DomainError::InvalidArgument {
            field: "new_expires_at".into(),
            reason: "must extend a live claim using monotonic server time".into(),
        });
    }
    Ok(())
}

fn validate_supersession(
    claim: &RunClaim,
    superseding_claim_id: RunClaimId,
    superseding_generation: RunClaimToken,
    superseded_at: ServerInstant,
) -> Result<(), DomainError> {
    if superseding_claim_id.as_uuid().is_nil()
        || superseding_claim_id == claim.id
        || claim.claim_generation.checked_next()? != superseding_generation
    {
        return Err(DomainError::InvocationClaimStale);
    }
    claim.validate_active_time(superseded_at)
}

fn valid_reason(reason: &str) -> bool {
    !reason.is_empty()
        && reason.len() <= 96
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn terminal_command_kind(state: RunClaimState) -> RunClaimCommandKind {
    match state {
        RunClaimState::Completed => RunClaimCommandKind::Release,
        RunClaimState::Expired => RunClaimCommandKind::Expire,
        RunClaimState::Revoked => RunClaimCommandKind::Revoke,
        RunClaimState::Superseded => RunClaimCommandKind::Supersede,
        RunClaimState::Active => RunClaimCommandKind::Renew,
    }
}

fn invalid_transition(state: RunClaimState, command: RunClaimCommandKind) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.as_str().into(),
    }
}

fn invalid_event(state: RunClaimState, event: &RunClaimEvent) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: event.name().into(),
    }
}

#[cfg(test)]
mod tests {
    use time::{Duration, macros::datetime};
    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(seconds: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + Duration::seconds(seconds))
    }

    fn grant(generation: u64, previous_generation: Option<u64>) -> GrantRunClaim {
        GrantRunClaim {
            id: id(generation as u8 + 10),
            run_id: id(1),
            previous_generation: previous_generation
                .map(|value| RunClaimToken::new(value).expect("previous generation")),
            claim_generation: RunClaimToken::new(generation).expect("generation"),
            holder_node_id: id(2),
            granted_at: at(0),
            expires_at: at(10),
        }
    }

    fn active() -> RunClaim {
        RunClaim::transition(None, &RunClaimCommand::Grant(grant(1, None)))
            .expect("grant")
            .aggregate
    }

    #[test]
    fn grant_renew_release_is_deterministic_and_replayable() {
        let granted =
            RunClaim::transition(None, &RunClaimCommand::Grant(grant(1, None))).expect("grant");
        let renewed = RunClaim::transition(
            Some(&granted.aggregate),
            &RunClaimCommand::Renew {
                expected_version: granted.aggregate.version,
                proof: granted.aggregate.proof(),
                now: at(2),
                new_expires_at: at(20),
            },
        )
        .expect("renew");
        let released = RunClaim::transition(
            Some(&renewed.aggregate),
            &RunClaimCommand::Release {
                expected_version: renewed.aggregate.version,
                proof: renewed.aggregate.proof(),
                result_digest: Some(Sha256Digest::of_bytes(b"result")),
                released_at: at(3),
            },
        )
        .expect("release");
        let events = [
            granted.events[0].clone(),
            renewed.events[0].clone(),
            released.events[0].clone(),
        ];
        assert_eq!(
            RunClaim::replay(&events).expect("replay"),
            released.aggregate
        );
        assert_eq!(released.aggregate.state, RunClaimState::Completed);
        assert_eq!(released.aggregate.version, AggregateVersion::new(3));
    }

    #[test]
    fn generations_are_strictly_sequential() {
        assert!(RunClaim::decide(None, &RunClaimCommand::Grant(grant(2, None))).is_err());
        assert!(RunClaim::decide(None, &RunClaimCommand::Grant(grant(3, Some(1)))).is_err());
        RunClaim::decide(None, &RunClaimCommand::Grant(grant(2, Some(1))))
            .expect("next generation");
    }

    #[test]
    fn old_generation_holder_mismatch_and_expiry_are_stale() {
        let claim = active();
        let stale_generation = RunClaimProof {
            claim_generation: RunClaimToken::new(2).expect("generation"),
            ..claim.proof()
        };
        let wrong_holder = RunClaimProof {
            holder_node_id: id(99),
            ..claim.proof()
        };
        for (proof, now) in [
            (stale_generation, at(1)),
            (wrong_holder, at(1)),
            (claim.proof(), at(10)),
        ] {
            assert_eq!(
                claim.authorize_for_run(claim.run_id, proof, now),
                Err(DomainError::InvocationClaimStale)
            );
        }
        assert_eq!(
            RunClaim::decide(
                Some(&claim),
                &RunClaimCommand::Renew {
                    expected_version: AggregateVersion::ZERO,
                    proof: claim.proof(),
                    now: at(1),
                    new_expires_at: at(20),
                }
            ),
            Err(DomainError::InvocationClaimStale)
        );
    }

    #[test]
    fn malicious_events_and_replay_cannot_bypass_holder_or_expiry() {
        let claim = active();
        let wrong_holder = RunClaimProof {
            holder_node_id: id(99),
            ..claim.proof()
        };
        let forged_release = RunClaimEvent::Released {
            proof: wrong_holder,
            result_digest: None,
            released_at: at(1),
        };
        assert_eq!(
            RunClaim::apply_event(Some(&claim), &forged_release),
            Err(DomainError::InvocationClaimStale)
        );
        let early_expiry = RunClaimEvent::Expired { expired_at: at(9) };
        assert!(RunClaim::apply_event(Some(&claim), &early_expiry).is_err());
        let forged_renewal = RunClaimEvent::Renewed {
            proof: claim.proof(),
            renewed_at: at(1),
            previous_expires_at: at(9),
            expires_at: at(20),
        };
        assert_eq!(
            RunClaim::apply_event(Some(&claim), &forged_renewal),
            Err(DomainError::InvocationClaimStale)
        );
        let skipped_generation = RunClaimEvent::Superseded {
            superseding_claim_id: id(22),
            superseding_generation: RunClaimToken::new(3).expect("generation"),
            superseded_at: at(1),
        };
        assert_eq!(
            RunClaim::apply_event(Some(&claim), &skipped_generation),
            Err(DomainError::InvocationClaimStale)
        );

        let prefix = RunClaimEvent::Granted(grant(1, None));
        assert_eq!(
            RunClaim::replay(&[prefix.clone(), forged_release]),
            Err(DomainError::InvocationClaimStale)
        );
        assert!(RunClaim::replay(&[prefix, early_expiry]).is_err());
        assert_eq!(
            RunClaim::replay(&[RunClaimEvent::Granted(grant(1, None)), skipped_generation,]),
            Err(DomainError::InvocationClaimStale)
        );
    }

    #[test]
    fn expire_revoke_and_supersede_are_distinct_terminal_states() {
        let claim = active();
        let expired = RunClaim::transition(
            Some(&claim),
            &RunClaimCommand::Expire {
                expected_version: claim.version,
                expired_at: at(10),
            },
        )
        .expect("expire");
        assert_eq!(expired.aggregate.state, RunClaimState::Expired);

        let claim = active();
        let revoked = RunClaim::transition(
            Some(&claim),
            &RunClaimCommand::Revoke {
                expected_version: claim.version,
                reason_code: "node_quarantined".into(),
                revoked_at: at(1),
            },
        )
        .expect("revoke");
        assert_eq!(revoked.aggregate.state, RunClaimState::Revoked);

        let claim = active();
        let superseded = RunClaim::transition(
            Some(&claim),
            &RunClaimCommand::Supersede {
                expected_version: claim.version,
                superseding_claim_id: id(22),
                superseding_generation: RunClaimToken::new(2).expect("generation"),
                superseded_at: at(1),
            },
        )
        .expect("supersede");
        assert_eq!(superseded.aggregate.state, RunClaimState::Superseded);
        assert_eq!(superseded.aggregate.superseding_claim_id, Some(id(22)));
    }

    #[test]
    fn terminality_precedes_cas_and_replay_never_revives_claim() {
        let claim = active();
        let terminal = RunClaim::transition(
            Some(&claim),
            &RunClaimCommand::Expire {
                expected_version: claim.version,
                expired_at: at(10),
            },
        )
        .expect("expire")
        .aggregate;
        let error = RunClaim::decide(
            Some(&terminal),
            &RunClaimCommand::Renew {
                expected_version: AggregateVersion::new(1),
                proof: claim.proof(),
                now: at(11),
                new_expires_at: at(20),
            },
        )
        .expect_err("terminal claim");
        assert!(matches!(error, DomainError::InvalidTransition { .. }));
        assert!(
            RunClaim::apply_event(
                Some(&terminal),
                &RunClaimEvent::Renewed {
                    proof: claim.proof(),
                    renewed_at: at(11),
                    previous_expires_at: at(10),
                    expires_at: at(20),
                }
            )
            .is_err()
        );
    }

    #[test]
    fn history_rejects_duplicate_generation_and_multiple_active_claims() {
        let first = active();
        let duplicate_generation = RunClaim {
            id: id(44),
            ..first.clone()
        };
        assert!(validate_run_claim_history([&first, &duplicate_generation]).is_err());

        let second = RunClaim::transition(None, &RunClaimCommand::Grant(grant(2, Some(1))))
            .expect("second")
            .aggregate;
        assert!(validate_run_claim_history([&first, &second]).is_err());
    }
}
