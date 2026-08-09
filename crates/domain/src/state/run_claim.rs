//! Independent, replayable claim aggregate for one invocation run.
//!
//! A run claim is scheduler authority to execute and report one
//! [`InvocationRunId`]. It is deliberately separate from the author
//! [`crate::state::lease::Lease`]
//! and from the invocation run state machine. Each successful acquisition is a
//! new aggregate/history row with a strictly increasing generation.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Deserializer, Serialize, de};

use crate::{
    error::DomainError,
    ids::{
        AggregateVersion, DecisionId, InvocationRunId, NodeId, RunClaimId, RunClaimToken,
        ServerInstant, Sha256Digest,
    },
    state::Transition,
};

/// Current wire/event schema emitted for independent run claims.
pub const RUN_CLAIM_EVENT_SCHEMA_VERSION: u16 = 2;

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
    pub predecessor_claim_id: Option<RunClaimId>,
    pub takeover_authorization: Option<RunClaimTakeoverAuthorization>,
    pub claim_generation: RunClaimToken,
    pub holder_node_id: NodeId,
    pub granted_at: ServerInstant,
    pub expires_at: ServerInstant,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SchedulerPreemptionReason {
    ClaimExpired,
    NodeRevoked,
    HolderUnavailable,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "authority", rename_all = "snake_case")]
pub enum RunClaimTakeoverAuthorization {
    Scheduler {
        reason: SchedulerPreemptionReason,
        authorized_at: ServerInstant,
    },
    Governance {
        decision_id: DecisionId,
        action_digest: Sha256Digest,
        authorized_at: ServerInstant,
    },
}

impl RunClaimTakeoverAuthorization {
    #[must_use]
    pub const fn authorized_at(&self) -> ServerInstant {
        match self {
            Self::Scheduler { authorized_at, .. } | Self::Governance { authorized_at, .. } => {
                *authorized_at
            }
        }
    }
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
    pub const fn schema_version(&self) -> u16 {
        RUN_CLAIM_EVENT_SCHEMA_VERSION
    }

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

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RunClaim {
    id: RunClaimId,
    run_id: InvocationRunId,
    previous_generation: Option<RunClaimToken>,
    predecessor_claim_id: Option<RunClaimId>,
    takeover_authorization: Option<RunClaimTakeoverAuthorization>,
    claim_generation: RunClaimToken,
    holder_node_id: NodeId,
    state: RunClaimState,
    granted_at: ServerInstant,
    expires_at: ServerInstant,
    updated_at: ServerInstant,
    terminal_at: Option<ServerInstant>,
    result_digest: Option<Sha256Digest>,
    terminal_reason: Option<String>,
    superseding_claim_id: Option<RunClaimId>,
    superseding_generation: Option<RunClaimToken>,
    version: AggregateVersion,
}

#[derive(Clone, Debug)]
pub struct VerifiedRunClaimHistory {
    claims: Vec<RunClaim>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RunClaimSnapshot {
    pub id: RunClaimId,
    pub run_id: InvocationRunId,
    pub previous_generation: Option<RunClaimToken>,
    pub predecessor_claim_id: Option<RunClaimId>,
    pub takeover_authorization: Option<RunClaimTakeoverAuthorization>,
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
    fn transition(
        current: Option<&Self>,
        command: &RunClaimCommand,
    ) -> Result<Transition<Self, RunClaimEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    fn decide(
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

    fn apply_event(current: Option<&Self>, event: &RunClaimEvent) -> Result<Self, DomainError> {
        match (current, event) {
            (None, RunClaimEvent::Granted(grant)) => {
                validate_grant(grant)?;
                Ok(Self {
                    id: grant.id,
                    run_id: grant.run_id,
                    previous_generation: grant.previous_generation,
                    predecessor_claim_id: grant.predecessor_claim_id,
                    takeover_authorization: grant.takeover_authorization.clone(),
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

    fn replay(events: &[RunClaimEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "run_claim",
        })
    }

    /// Creates generation one. Successor claims are only created by the
    /// invocation run's atomic takeover boundary.
    pub fn grant_initial(
        grant: GrantRunClaim,
    ) -> Result<Transition<Self, RunClaimEvent>, DomainError> {
        if grant.previous_generation.is_some() || grant.claim_generation.get() != 1 {
            return Err(DomainError::InvocationClaimStale);
        }
        Self::transition(None, &RunClaimCommand::Grant(grant))
    }

    /// Applies a non-creation command to this claim.
    pub fn execute(
        &self,
        command: &RunClaimCommand,
    ) -> Result<Transition<Self, RunClaimEvent>, DomainError> {
        if !matches!(
            command,
            RunClaimCommand::Renew { .. }
                | RunClaimCommand::Expire { .. }
                | RunClaimCommand::Revoke { .. }
        ) {
            return Err(invalid_transition(self.state, command.kind()));
        }
        Self::transition(Some(self), command)
    }

    /// Supersedes this claim and returns the only context accepted for creating
    /// its declared successor.
    #[allow(dead_code)] // Used only by the crate-private atomic takeover decision.
    pub(super) fn supersede_for_successor(
        &self,
        command: &RunClaimCommand,
    ) -> Result<Transition<Self, RunClaimEvent>, DomainError> {
        if !matches!(command, RunClaimCommand::Supersede { .. }) {
            return Err(invalid_transition(self.state, command.kind()));
        }
        Self::transition(Some(self), command)
    }

    #[allow(dead_code)] // Used only by the crate-private atomic takeover decision.
    pub(super) fn grant_successor_from_predecessor(
        predecessor: &RunClaim,
        grant: GrantRunClaim,
    ) -> Result<Transition<Self, RunClaimEvent>, DomainError> {
        validate_successor(predecessor, &grant)?;
        Self::transition(None, &RunClaimCommand::Grant(grant))
    }

    #[allow(dead_code)] // Used only by the crate-private atomic terminal decision.
    pub(super) fn release_for_terminal(
        &self,
        proof: RunClaimProof,
        result_digest: Option<Sha256Digest>,
        released_at: ServerInstant,
    ) -> Result<Transition<Self, RunClaimEvent>, DomainError> {
        Self::transition(
            Some(self),
            &RunClaimCommand::Release {
                expected_version: self.version,
                proof,
                result_digest,
                released_at,
            },
        )
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
    pub const fn id(&self) -> RunClaimId {
        self.id
    }
    #[must_use]
    pub const fn run_id(&self) -> InvocationRunId {
        self.run_id
    }
    #[must_use]
    pub const fn generation(&self) -> RunClaimToken {
        self.claim_generation
    }
    #[must_use]
    pub const fn holder_node_id(&self) -> NodeId {
        self.holder_node_id
    }
    #[must_use]
    pub const fn state(&self) -> RunClaimState {
        self.state
    }
    #[must_use]
    pub const fn granted_at(&self) -> ServerInstant {
        self.granted_at
    }
    #[must_use]
    pub const fn expires_at(&self) -> ServerInstant {
        self.expires_at
    }
    #[must_use]
    pub const fn updated_at(&self) -> ServerInstant {
        self.updated_at
    }
    #[must_use]
    pub const fn terminal_at(&self) -> Option<ServerInstant> {
        self.terminal_at
    }
    #[must_use]
    pub const fn result_digest(&self) -> Option<Sha256Digest> {
        self.result_digest
    }
    #[must_use]
    pub const fn superseding_claim_id(&self) -> Option<RunClaimId> {
        self.superseding_claim_id
    }
    #[must_use]
    pub const fn superseding_generation(&self) -> Option<RunClaimToken> {
        self.superseding_generation
    }
    #[must_use]
    pub const fn version(&self) -> AggregateVersion {
        self.version
    }

    #[must_use]
    pub fn snapshot(&self) -> RunClaimSnapshot {
        RunClaimSnapshot {
            id: self.id,
            run_id: self.run_id,
            previous_generation: self.previous_generation,
            predecessor_claim_id: self.predecessor_claim_id,
            takeover_authorization: self.takeover_authorization.clone(),
            claim_generation: self.claim_generation,
            holder_node_id: self.holder_node_id,
            state: self.state,
            granted_at: self.granted_at,
            expires_at: self.expires_at,
            updated_at: self.updated_at,
            terminal_at: self.terminal_at,
            result_digest: self.result_digest,
            terminal_reason: self.terminal_reason.clone(),
            superseding_claim_id: self.superseding_claim_id,
            superseding_generation: self.superseding_generation,
            version: self.version,
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

    fn as_grant(&self) -> GrantRunClaim {
        GrantRunClaim {
            id: self.id,
            run_id: self.run_id,
            previous_generation: self.previous_generation,
            predecessor_claim_id: self.predecessor_claim_id,
            takeover_authorization: self.takeover_authorization.clone(),
            claim_generation: self.claim_generation,
            holder_node_id: self.holder_node_id,
            granted_at: self.granted_at,
            expires_at: self.expires_at,
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

impl VerifiedRunClaimHistory {
    /// Replays every aggregate stream and validates the complete per-run
    /// generation/linkage chain before exposing claim heads.
    pub fn replay(streams: &[Vec<RunClaimEvent>]) -> Result<Self, DomainError> {
        let claims = streams
            .iter()
            .map(|events| RunClaim::replay(events))
            .collect::<Result<Vec<_>, _>>()?;
        validate_run_claim_history(&claims)?;
        Ok(Self { claims })
    }

    pub fn from_validated_snapshots(snapshots: Vec<RunClaimSnapshot>) -> Result<Self, DomainError> {
        let claims = snapshots
            .into_iter()
            .map(RunClaim::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        validate_run_claim_history(&claims)?;
        Ok(Self { claims })
    }

    #[must_use]
    pub fn get(&self, id: RunClaimId) -> Option<&RunClaim> {
        self.claims.iter().find(|claim| claim.id == id)
    }

    #[must_use]
    pub fn claims(&self) -> &[RunClaim] {
        &self.claims
    }
}

impl TryFrom<RunClaimSnapshot> for RunClaim {
    type Error = DomainError;

    fn try_from(snapshot: RunClaimSnapshot) -> Result<Self, Self::Error> {
        validate_snapshot(&snapshot)?;
        Ok(Self {
            id: snapshot.id,
            run_id: snapshot.run_id,
            previous_generation: snapshot.previous_generation,
            predecessor_claim_id: snapshot.predecessor_claim_id,
            takeover_authorization: snapshot.takeover_authorization,
            claim_generation: snapshot.claim_generation,
            holder_node_id: snapshot.holder_node_id,
            state: snapshot.state,
            granted_at: snapshot.granted_at,
            expires_at: snapshot.expires_at,
            updated_at: snapshot.updated_at,
            terminal_at: snapshot.terminal_at,
            result_digest: snapshot.result_digest,
            terminal_reason: snapshot.terminal_reason,
            superseding_claim_id: snapshot.superseding_claim_id,
            superseding_generation: snapshot.superseding_generation,
            version: snapshot.version,
        })
    }
}

impl<'de> Deserialize<'de> for RunClaim {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let snapshot = RunClaimSnapshot::deserialize(deserializer)?;
        Self::try_from(snapshot).map_err(de::Error::custom)
    }
}

fn validate_snapshot(snapshot: &RunClaimSnapshot) -> Result<(), DomainError> {
    if snapshot.id.as_uuid().is_nil()
        || snapshot.run_id.as_uuid().is_nil()
        || snapshot.holder_node_id.as_uuid().is_nil()
        || snapshot.version == AggregateVersion::ZERO
        || snapshot.granted_at >= snapshot.expires_at
        || snapshot.updated_at < snapshot.granted_at
        || (snapshot.state == RunClaimState::Active && snapshot.updated_at >= snapshot.expires_at)
    {
        return Err(DomainError::InvocationClaimStale);
    }
    let generation_shape = if snapshot.claim_generation.get() == 1 {
        snapshot.previous_generation.is_none()
            && snapshot.predecessor_claim_id.is_none()
            && snapshot.takeover_authorization.is_none()
    } else {
        snapshot
            .previous_generation
            .and_then(|value| value.checked_next().ok())
            == Some(snapshot.claim_generation)
            && snapshot.predecessor_claim_id.is_some()
            && snapshot.takeover_authorization.is_some()
            && snapshot.predecessor_claim_id != Some(snapshot.id)
            && snapshot
                .takeover_authorization
                .as_ref()
                .is_some_and(|authorization| {
                    takeover_authorization_shape_valid(authorization, snapshot.granted_at)
                })
    };
    if !generation_shape {
        return Err(DomainError::InvocationClaimStale);
    }
    let terminal_shape = match snapshot.state {
        RunClaimState::Active => {
            snapshot.terminal_at.is_none()
                && snapshot.result_digest.is_none()
                && snapshot.terminal_reason.is_none()
                && snapshot.superseding_claim_id.is_none()
                && snapshot.superseding_generation.is_none()
        }
        RunClaimState::Completed => {
            snapshot.version.get() >= 2
                && snapshot.terminal_at == Some(snapshot.updated_at)
                && snapshot.updated_at < snapshot.expires_at
                && snapshot.terminal_reason.is_none()
                && snapshot.superseding_claim_id.is_none()
                && snapshot.superseding_generation.is_none()
        }
        RunClaimState::Expired => {
            snapshot.version.get() >= 2
                && snapshot.terminal_at == Some(snapshot.updated_at)
                && snapshot.updated_at >= snapshot.expires_at
                && snapshot.result_digest.is_none()
                && snapshot.terminal_reason.is_none()
                && snapshot.superseding_claim_id.is_none()
                && snapshot.superseding_generation.is_none()
        }
        RunClaimState::Revoked => {
            snapshot.version.get() >= 2
                && snapshot.terminal_at == Some(snapshot.updated_at)
                && snapshot.updated_at < snapshot.expires_at
                && snapshot.result_digest.is_none()
                && snapshot
                    .terminal_reason
                    .as_deref()
                    .is_some_and(valid_reason)
                && snapshot.superseding_claim_id.is_none()
                && snapshot.superseding_generation.is_none()
        }
        RunClaimState::Superseded => {
            snapshot.version.get() >= 2
                && snapshot.terminal_at == Some(snapshot.updated_at)
                && snapshot.updated_at < snapshot.expires_at
                && snapshot.result_digest.is_none()
                && snapshot.terminal_reason.is_none()
                && snapshot
                    .superseding_claim_id
                    .is_some_and(|id| !id.as_uuid().is_nil() && id != snapshot.id)
                && snapshot.superseding_generation.is_some_and(|generation| {
                    snapshot.claim_generation.checked_next().ok() == Some(generation)
                })
        }
    };
    if !terminal_shape {
        return Err(DomainError::InvocationClaimStale);
    }
    Ok(())
}

/// Transaction-level guard mirroring the one-ACTIVE-row database constraint.
pub fn validate_run_claim_history<'a>(
    claims: impl IntoIterator<Item = &'a RunClaim>,
) -> Result<(), DomainError> {
    let mut by_run: BTreeMap<InvocationRunId, Vec<&RunClaim>> = BTreeMap::new();
    let mut claim_ids = BTreeSet::new();
    for claim in claims {
        if !claim_ids.insert(claim.id) {
            return Err(DomainError::InvariantViolation {
                invariant: "run_claim_id_must_be_unique",
            });
        }
        by_run.entry(claim.run_id).or_default().push(claim);
    }
    for run_claims in by_run.values_mut() {
        run_claims.sort_by_key(|claim| claim.claim_generation);
        let Some(first) = run_claims.first() else {
            continue;
        };
        if first.claim_generation.get() != 1 || first.previous_generation.is_some() {
            return Err(DomainError::InvariantViolation {
                invariant: "run_claim_history_must_start_at_generation_one",
            });
        }
        for pair in run_claims.windows(2) {
            let predecessor = pair[0];
            let successor = pair[1];
            validate_successor(predecessor, &successor.as_grant())?;
        }
        if run_claims
            .last()
            .is_some_and(|claim| claim.state == RunClaimState::Superseded)
        {
            return Err(DomainError::InvariantViolation {
                invariant: "superseded_run_claim_must_have_its_declared_successor",
            });
        }
    }
    Ok(())
}

fn validate_successor(
    predecessor: &RunClaim,
    successor: &GrantRunClaim,
) -> Result<(), DomainError> {
    let predecessor_allows_successor = match predecessor.state {
        RunClaimState::Superseded => {
            predecessor.superseding_claim_id == Some(successor.id)
                && predecessor.superseding_generation == Some(successor.claim_generation)
        }
        RunClaimState::Expired | RunClaimState::Revoked => true,
        RunClaimState::Active | RunClaimState::Completed => false,
    };
    let authorization_matches = match successor.takeover_authorization.as_ref() {
        Some(RunClaimTakeoverAuthorization::Scheduler { reason, .. }) => match reason {
            SchedulerPreemptionReason::ClaimExpired => predecessor.state == RunClaimState::Expired,
            SchedulerPreemptionReason::NodeRevoked => predecessor.state == RunClaimState::Revoked,
            SchedulerPreemptionReason::HolderUnavailable => {
                predecessor.state == RunClaimState::Superseded
            }
        },
        Some(RunClaimTakeoverAuthorization::Governance { decision_id, .. }) => {
            !decision_id.as_uuid().is_nil()
        }
        None => false,
    };
    if !predecessor_allows_successor
        || !authorization_matches
        || predecessor.run_id != successor.run_id
        || predecessor.claim_generation.checked_next()? != successor.claim_generation
        || successor.previous_generation != Some(predecessor.claim_generation)
        || successor.predecessor_claim_id != Some(predecessor.id)
        || successor.takeover_authorization.is_none()
        || successor
            .takeover_authorization
            .as_ref()
            .is_some_and(|authorization| authorization.authorized_at() != successor.granted_at)
        || predecessor
            .terminal_at
            .is_none_or(|terminal_at| successor.granted_at < terminal_at)
    {
        return Err(DomainError::InvariantViolation {
            invariant: "run_claim_successor_must_match_superseded_predecessor",
        });
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
        None if grant.claim_generation.get() == 1
            && grant.predecessor_claim_id.is_none()
            && grant.takeover_authorization.is_none() =>
        {
            Ok(())
        }
        Some(previous)
            if previous.checked_next()? == grant.claim_generation
                && grant.predecessor_claim_id.is_some()
                && grant
                    .takeover_authorization
                    .as_ref()
                    .is_some_and(|authorization| {
                        takeover_authorization_shape_valid(authorization, grant.granted_at)
                    }) =>
        {
            Ok(())
        }
        _ => Err(DomainError::InvalidArgument {
            field: "claim_generation".into(),
            reason: "must be one greater than the previous generation".into(),
        }),
    }
}

fn takeover_authorization_shape_valid(
    authorization: &RunClaimTakeoverAuthorization,
    granted_at: ServerInstant,
) -> bool {
    authorization.authorized_at() == granted_at
        && match authorization {
            RunClaimTakeoverAuthorization::Scheduler { .. } => true,
            RunClaimTakeoverAuthorization::Governance { decision_id, .. } => {
                !decision_id.as_uuid().is_nil()
            }
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
            predecessor_claim_id: previous_generation.map(|value| id(value as u8 + 10)),
            takeover_authorization: previous_generation.map(|_| {
                RunClaimTakeoverAuthorization::Scheduler {
                    reason: SchedulerPreemptionReason::HolderUnavailable,
                    authorized_at: at(1),
                }
            }),
            claim_generation: RunClaimToken::new(generation).expect("generation"),
            holder_node_id: id(2),
            granted_at: previous_generation.map_or_else(|| at(0), |_| at(1)),
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
    fn public_execute_cannot_release_supersede_or_create_a_successor() {
        let claim = active();
        assert!(matches!(
            claim.execute(&RunClaimCommand::Release {
                expected_version: claim.version(),
                proof: claim.proof(),
                result_digest: None,
                released_at: at(1),
            }),
            Err(DomainError::InvalidTransition { .. })
        ));
        assert!(matches!(
            claim.execute(&RunClaimCommand::Supersede {
                expected_version: claim.version(),
                superseding_claim_id: id(22),
                superseding_generation: RunClaimToken::new(2).expect("generation"),
                superseded_at: at(1),
            }),
            Err(DomainError::InvalidTransition { .. })
        ));
        assert_eq!(
            RunClaim::grant_initial(grant(2, Some(1))),
            Err(DomainError::InvocationClaimStale)
        );
    }

    #[test]
    fn validated_snapshot_rejects_malicious_terminal_and_generation_shapes() {
        let claim = active();
        let mut active_at_expiry = claim.snapshot();
        active_at_expiry.updated_at = active_at_expiry.expires_at;
        assert_eq!(
            RunClaim::try_from(active_at_expiry),
            Err(DomainError::InvocationClaimStale)
        );

        let completed = claim
            .release_for_terminal(claim.proof(), None, at(1))
            .expect("release")
            .aggregate;
        let mut forged_completed = completed.snapshot();
        forged_completed.terminal_reason = Some("forged_reason".into());
        assert_eq!(
            RunClaim::try_from(forged_completed),
            Err(DomainError::InvocationClaimStale)
        );

        let superseded = claim
            .supersede_for_successor(&RunClaimCommand::Supersede {
                expected_version: claim.version(),
                superseding_claim_id: id(22),
                superseding_generation: RunClaimToken::new(2).expect("generation"),
                superseded_at: at(1),
            })
            .expect("supersede")
            .aggregate;
        let mut forged_superseded = superseded.snapshot();
        forged_superseded.superseding_generation = Some(RunClaimToken::new(3).expect("generation"));
        assert_eq!(
            RunClaim::try_from(forged_superseded),
            Err(DomainError::InvocationClaimStale)
        );
    }

    #[test]
    fn authoritative_deserialization_cannot_bypass_snapshot_validation() {
        let claim = active();
        let encoded = serde_json::to_value(&claim).expect("serialize claim");
        assert_eq!(
            serde_json::from_value::<RunClaim>(encoded.clone()).expect("validated deserialize"),
            claim
        );

        let mut active_at_expiry = encoded;
        active_at_expiry["updated_at"] =
            serde_json::to_value(claim.expires_at()).expect("serialize expiry");
        assert!(serde_json::from_value::<RunClaim>(active_at_expiry).is_err());
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

    #[test]
    fn successor_requires_verified_supersession_and_history_is_contiguous() {
        let first = active();
        let superseded = first
            .supersede_for_successor(&RunClaimCommand::Supersede {
                expected_version: first.version,
                superseding_claim_id: id(12),
                superseding_generation: RunClaimToken::new(2).expect("generation"),
                superseded_at: at(1),
            })
            .expect("supersede");
        let mut successor_grant = grant(2, Some(1));
        successor_grant.granted_at = at(1);
        let second =
            RunClaim::grant_successor_from_predecessor(&superseded.aggregate, successor_grant)
                .expect("verified successor")
                .aggregate;
        validate_run_claim_history([&superseded.aggregate, &second]).expect("valid history");

        let mut gap_predecessor = superseded.aggregate.clone();
        gap_predecessor.superseding_generation = Some(RunClaimToken::new(3).expect("generation"));
        let mut gap = second.clone();
        gap.claim_generation = RunClaimToken::new(3).expect("generation");
        assert!(validate_run_claim_history([&gap_predecessor, &gap]).is_err());

        let mut wrong_terminal = superseded.aggregate;
        wrong_terminal.state = RunClaimState::Revoked;
        assert!(validate_run_claim_history([&wrong_terminal, &second]).is_err());
        assert!(RunClaim::grant_initial(grant(2, Some(1))).is_err());
    }
}
