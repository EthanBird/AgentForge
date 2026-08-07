//! Lease aggregate, expiry semantics, and fencing proof guard.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        AggregateVersion, AttemptId, FencingToken, LeaseId, NodeId, PackageId, PackageRevisionId,
        ServerInstant,
    },
    state::Transition,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaseState {
    Active,
    Released,
    Revoked,
    Expired,
}

impl LeaseState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Released => "released",
            Self::Revoked => "revoked",
            Self::Expired => "expired",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Active)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum LeaseCommandKind {
    GrantLease,
    RenewLease,
    ReleaseLease,
    RevokeLease,
    ExpireLease,
}

impl LeaseCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::GrantLease => "grant_lease",
            Self::RenewLease => "renew_lease",
            Self::ReleaseLease => "release_lease",
            Self::RevokeLease => "revoke_lease",
            Self::ExpireLease => "expire_lease",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GrantLease {
    pub id: LeaseId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub attempt_id: AttemptId,
    pub holder_node_id: NodeId,
    pub previous_fencing_token: Option<FencingToken>,
    pub fencing_token: FencingToken,
    pub granted_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub max_expires_at: ServerInstant,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LeaseCommand {
    GrantLease(GrantLease),
    RenewLease {
        expected_version: AggregateVersion,
        holder_node_id: NodeId,
        fencing_token: FencingToken,
        now: ServerInstant,
        new_expires_at: ServerInstant,
    },
    ReleaseLease {
        expected_version: AggregateVersion,
        holder_node_id: NodeId,
        fencing_token: FencingToken,
        now: ServerInstant,
    },
    RevokeLease {
        expected_version: AggregateVersion,
        policy_authorized: bool,
        reason_code: String,
    },
    ExpireLease {
        expected_version: AggregateVersion,
        now: ServerInstant,
    },
}

impl LeaseCommand {
    #[must_use]
    pub const fn kind(&self) -> LeaseCommandKind {
        match self {
            Self::GrantLease(_) => LeaseCommandKind::GrantLease,
            Self::RenewLease { .. } => LeaseCommandKind::RenewLease,
            Self::ReleaseLease { .. } => LeaseCommandKind::ReleaseLease,
            Self::RevokeLease { .. } => LeaseCommandKind::RevokeLease,
            Self::ExpireLease { .. } => LeaseCommandKind::ExpireLease,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        match self {
            Self::GrantLease(_) => None,
            Self::RenewLease {
                expected_version, ..
            }
            | Self::ReleaseLease {
                expected_version, ..
            }
            | Self::RevokeLease {
                expected_version, ..
            }
            | Self::ExpireLease {
                expected_version, ..
            } => Some(*expected_version),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum LeaseEvent {
    LeaseGranted(GrantLease),
    LeaseRenewed { expires_at: ServerInstant },
    LeaseReleased,
    LeaseRevoked { reason_code: String },
    LeaseExpired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LeaseTransitionRule {
    pub from: LeaseState,
    pub command: LeaseCommandKind,
    pub to: LeaseState,
}

pub const TRANSITION_TABLE: &[LeaseTransitionRule] = &[
    LeaseTransitionRule {
        from: LeaseState::Active,
        command: LeaseCommandKind::RenewLease,
        to: LeaseState::Active,
    },
    LeaseTransitionRule {
        from: LeaseState::Active,
        command: LeaseCommandKind::ReleaseLease,
        to: LeaseState::Released,
    },
    LeaseTransitionRule {
        from: LeaseState::Active,
        command: LeaseCommandKind::RevokeLease,
        to: LeaseState::Revoked,
    },
    LeaseTransitionRule {
        from: LeaseState::Active,
        command: LeaseCommandKind::ExpireLease,
        to: LeaseState::Expired,
    },
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Lease {
    pub id: LeaseId,
    pub package_id: PackageId,
    pub revision_id: PackageRevisionId,
    pub attempt_id: AttemptId,
    pub holder_node_id: NodeId,
    pub fencing_token: FencingToken,
    pub state: LeaseState,
    pub granted_at: ServerInstant,
    pub expires_at: ServerInstant,
    pub max_expires_at: ServerInstant,
    pub version: AggregateVersion,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseProof {
    pub lease_id: LeaseId,
    pub attempt_id: AttemptId,
    pub fencing_token: FencingToken,
    pub expected_lease_version: AggregateVersion,
}

impl Lease {
    pub fn transition(
        current: Option<&Self>,
        command: &LeaseCommand,
    ) -> Result<Transition<Self, LeaseEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &LeaseCommand,
    ) -> Result<LeaseEvent, DomainError> {
        match (current, command) {
            (None, LeaseCommand::GrantLease(grant)) => {
                validate_grant(grant)?;
                Ok(LeaseEvent::LeaseGranted(grant.clone()))
            }
            (None, _) => Err(DomainError::NotFound { resource: "lease" }),
            (Some(lease), LeaseCommand::GrantLease(_)) => Err(invalid_transition(
                lease.state,
                LeaseCommandKind::GrantLease.as_str(),
            )),
            (Some(lease), command) => {
                // Terminality wins over CAS. A receipt miss caused by a new
                // idempotency key must return AF_TRANSITION_INVALID even when
                // the caller repeats the pre-terminal expected version.
                if lease.state.is_terminal() {
                    return Err(invalid_transition(lease.state, command.kind().as_str()));
                }
                if command.expected_version() != Some(lease.version) {
                    return Err(DomainError::StaleVersion);
                }
                if !TRANSITION_TABLE
                    .iter()
                    .any(|rule| rule.from == lease.state && rule.command == command.kind())
                {
                    return Err(invalid_transition(lease.state, command.kind().as_str()));
                }
                match command {
                    LeaseCommand::RenewLease {
                        holder_node_id,
                        fencing_token,
                        now,
                        new_expires_at,
                        ..
                    } => {
                        lease.authorize_holder(*holder_node_id, *fencing_token, *now)?;
                        if *now >= lease.max_expires_at {
                            return Err(DomainError::LeaseExpired);
                        }
                        if *new_expires_at <= lease.expires_at
                            || *new_expires_at > lease.max_expires_at
                            || *new_expires_at <= *now
                        {
                            return Err(DomainError::InvalidArgument {
                                field: "new_expires_at".into(),
                                reason: "must extend the lease without exceeding its maximum"
                                    .into(),
                            });
                        }
                        Ok(LeaseEvent::LeaseRenewed {
                            expires_at: *new_expires_at,
                        })
                    }
                    LeaseCommand::ReleaseLease {
                        holder_node_id,
                        fencing_token,
                        now,
                        ..
                    } => {
                        lease.authorize_holder(*holder_node_id, *fencing_token, *now)?;
                        Ok(LeaseEvent::LeaseReleased)
                    }
                    LeaseCommand::RevokeLease {
                        policy_authorized,
                        reason_code,
                        ..
                    } => {
                        if !policy_authorized || reason_code.trim().is_empty() {
                            return Err(DomainError::PolicyDenied);
                        }
                        Ok(LeaseEvent::LeaseRevoked {
                            reason_code: reason_code.clone(),
                        })
                    }
                    LeaseCommand::ExpireLease { now, .. } => {
                        if *now < lease.expires_at {
                            return Err(DomainError::InvalidTransition {
                                from: lease.state.as_str().into(),
                                command: command.kind().as_str().into(),
                            });
                        }
                        Ok(LeaseEvent::LeaseExpired)
                    }
                    LeaseCommand::GrantLease(_) => unreachable!("handled above"),
                }
            }
        }
    }

    pub fn apply_event(current: Option<&Self>, event: &LeaseEvent) -> Result<Self, DomainError> {
        match (current, event) {
            (None, LeaseEvent::LeaseGranted(grant)) => {
                validate_grant(grant)?;
                Ok(Self {
                    id: grant.id,
                    package_id: grant.package_id,
                    revision_id: grant.revision_id,
                    attempt_id: grant.attempt_id,
                    holder_node_id: grant.holder_node_id,
                    fencing_token: grant.fencing_token,
                    state: LeaseState::Active,
                    granted_at: grant.granted_at,
                    expires_at: grant.expires_at,
                    max_expires_at: grant.max_expires_at,
                    version: AggregateVersion::new(1),
                })
            }
            (None, _) => Err(DomainError::NotFound { resource: "lease" }),
            (Some(lease), LeaseEvent::LeaseGranted(_)) => {
                Err(invalid_transition(lease.state, event.name()))
            }
            (Some(lease), event) if lease.state.is_terminal() => {
                Err(invalid_transition(lease.state, event.name()))
            }
            (Some(lease), LeaseEvent::LeaseRenewed { expires_at })
                if *expires_at > lease.expires_at && *expires_at <= lease.max_expires_at =>
            {
                let mut next = lease.clone();
                next.expires_at = *expires_at;
                next.version = lease.version.checked_next()?;
                Ok(next)
            }
            (Some(lease), LeaseEvent::LeaseReleased) => lease.terminalize(LeaseState::Released),
            (Some(lease), LeaseEvent::LeaseRevoked { reason_code })
                if !reason_code.trim().is_empty() =>
            {
                lease.terminalize(LeaseState::Revoked)
            }
            (Some(lease), LeaseEvent::LeaseExpired) => lease.terminalize(LeaseState::Expired),
            (Some(lease), event) => Err(invalid_transition(lease.state, event.name())),
        }
    }

    pub fn replay(events: &[LeaseEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound { resource: "lease" })
    }

    fn authorize_holder(
        &self,
        holder_node_id: NodeId,
        fencing_token: FencingToken,
        now: ServerInstant,
    ) -> Result<(), DomainError> {
        if self.state != LeaseState::Active
            || self.holder_node_id != holder_node_id
            || self.fencing_token != fencing_token
        {
            return Err(DomainError::StaleLease);
        }
        if now >= self.expires_at {
            return Err(DomainError::LeaseExpired);
        }
        Ok(())
    }

    fn terminalize(&self, target: LeaseState) -> Result<Self, DomainError> {
        if self.state != LeaseState::Active || !target.is_terminal() {
            return Err(invalid_transition(self.state, target.as_str()));
        }
        let mut next = self.clone();
        next.state = target;
        next.version = self.version.checked_next()?;
        Ok(next)
    }
}

impl LeaseEvent {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::LeaseGranted(_) => "lease_granted",
            Self::LeaseRenewed { .. } => "lease_renewed",
            Self::LeaseReleased => "lease_released",
            Self::LeaseRevoked { .. } => "lease_revoked",
            Self::LeaseExpired => "lease_expired",
        }
    }
}

pub fn authorize_attempt_write(
    lease: &Lease,
    proof: &LeaseProof,
    now: ServerInstant,
) -> Result<(), DomainError> {
    if lease.id != proof.lease_id
        || lease.attempt_id != proof.attempt_id
        || lease.fencing_token != proof.fencing_token
        || lease.state != LeaseState::Active
    {
        return Err(DomainError::StaleLease);
    }
    if now >= lease.expires_at {
        return Err(DomainError::LeaseExpired);
    }
    if lease.version != proof.expected_lease_version {
        return Err(DomainError::StaleVersion);
    }
    Ok(())
}

/// Cross-aggregate invariant checked by ClaimPackage transaction projections.
pub fn validate_single_active_generation<'a>(
    leases: impl IntoIterator<Item = &'a Lease>,
) -> Result<(), DomainError> {
    let mut active: BTreeMap<PackageRevisionId, (LeaseId, FencingToken)> = BTreeMap::new();
    for lease in leases {
        if lease.state != LeaseState::Active {
            continue;
        }
        if active
            .insert(lease.revision_id, (lease.id, lease.fencing_token))
            .is_some()
        {
            return Err(DomainError::InvariantViolation {
                invariant: "package_revision_must_have_at_most_one_active_generation",
            });
        }
    }
    Ok(())
}

fn validate_grant(grant: &GrantLease) -> Result<(), DomainError> {
    let expected = match grant.previous_fencing_token {
        Some(previous) => previous.checked_next()?,
        None => FencingToken::new(1)?,
    };
    if grant.fencing_token != expected {
        return Err(DomainError::StaleLease);
    }
    if grant.granted_at >= grant.expires_at || grant.expires_at > grant.max_expires_at {
        return Err(DomainError::InvalidArgument {
            field: "lease_window".into(),
            reason: "must satisfy granted_at < expires_at <= max_expires_at".into(),
        });
    }
    Ok(())
}

fn invalid_transition(state: LeaseState, command: &str) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.into(),
    }
}
