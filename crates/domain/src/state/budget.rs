//! Hierarchical budget reservations with conservation-safe accounting.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        AggregateVersion, AttemptId, BudgetAccountId, BudgetReservationId, IntegrationId,
        InvocationRunId, ServerInstant, Sha256Digest, VerificationRunId,
    },
    state::Transition,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "snake_case")]
pub enum BudgetPurpose {
    Attempt(AttemptId),
    InvocationRun(InvocationRunId),
    VerificationRun(VerificationRunId),
    Integration(IntegrationId),
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ReserveBudget {
    pub id: BudgetReservationId,
    pub account_id: BudgetAccountId,
    pub parent_reservation_id: Option<BudgetReservationId>,
    pub purpose: BudgetPurpose,
    pub amount_units: u64,
    /// Root: unreserved account units. Child: unallocated parent units.
    pub source_available_units: u64,
    pub source_version: AggregateVersion,
    pub reserved_at: ServerInstant,
    pub expires_at: Option<ServerInstant>,
}

impl ReserveBudget {
    fn validate(&self) -> Result<(), DomainError> {
        if self.amount_units == 0 || self.source_version == AggregateVersion::ZERO {
            return Err(DomainError::InvalidArgument {
                field: "amount_units".into(),
                reason: "must be non-zero".into(),
            });
        }
        if self.amount_units > self.source_available_units {
            return Err(DomainError::BudgetReservationFailed);
        }
        if self
            .expires_at
            .is_some_and(|expires| expires <= self.reserved_at)
        {
            return Err(DomainError::InvalidArgument {
                field: "expires_at".into(),
                reason: "must be later than reserved_at".into(),
            });
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetReservationState {
    Active,
    Settled,
    Released,
    Expired,
    Cancelled,
}

impl BudgetReservationState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Settled => "settled",
            Self::Released => "released",
            Self::Expired => "expired",
            Self::Cancelled => "cancelled",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        !matches!(self, Self::Active)
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum BudgetReservationCommandKind {
    Reserve,
    AllocateChild,
    ReturnChildAllocation,
    Settle,
    Release,
    Expire,
    Cancel,
}

impl BudgetReservationCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Reserve => "reserve",
            Self::AllocateChild => "allocate_child",
            Self::ReturnChildAllocation => "return_child_allocation",
            Self::Settle => "settle",
            Self::Release => "release",
            Self::Expire => "expire",
            Self::Cancel => "cancel",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BudgetReservationCommand {
    Reserve(ReserveBudget),
    AllocateChild {
        expected_version: AggregateVersion,
        child_id: BudgetReservationId,
        amount_units: u64,
    },
    ReturnChildAllocation {
        expected_version: AggregateVersion,
        child_id: BudgetReservationId,
        amount_units: u64,
        child_is_terminal: bool,
    },
    Settle {
        expected_version: AggregateVersion,
        spent_units: u64,
        signed_usage_digest: Sha256Digest,
        children_terminal: bool,
        settled_at: ServerInstant,
    },
    Release {
        expected_version: AggregateVersion,
        reason_code: String,
        children_terminal: bool,
        released_at: ServerInstant,
    },
    Expire {
        expected_version: AggregateVersion,
        children_terminal: bool,
        now: ServerInstant,
    },
    Cancel {
        expected_version: AggregateVersion,
        reason_code: String,
        children_terminal: bool,
        cancelled_at: ServerInstant,
    },
}

impl BudgetReservationCommand {
    #[must_use]
    pub const fn kind(&self) -> BudgetReservationCommandKind {
        match self {
            Self::Reserve(_) => BudgetReservationCommandKind::Reserve,
            Self::AllocateChild { .. } => BudgetReservationCommandKind::AllocateChild,
            Self::ReturnChildAllocation { .. } => {
                BudgetReservationCommandKind::ReturnChildAllocation
            }
            Self::Settle { .. } => BudgetReservationCommandKind::Settle,
            Self::Release { .. } => BudgetReservationCommandKind::Release,
            Self::Expire { .. } => BudgetReservationCommandKind::Expire,
            Self::Cancel { .. } => BudgetReservationCommandKind::Cancel,
        }
    }

    #[must_use]
    pub const fn expected_version(&self) -> Option<AggregateVersion> {
        match self {
            Self::Reserve(_) => None,
            Self::AllocateChild {
                expected_version, ..
            }
            | Self::ReturnChildAllocation {
                expected_version, ..
            }
            | Self::Settle {
                expected_version, ..
            }
            | Self::Release {
                expected_version, ..
            }
            | Self::Expire {
                expected_version, ..
            }
            | Self::Cancel {
                expected_version, ..
            } => Some(*expected_version),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum BudgetReservationEvent {
    Reserved(ReserveBudget),
    ChildAllocated {
        child_id: BudgetReservationId,
        amount_units: u64,
    },
    ChildAllocationReturned {
        child_id: BudgetReservationId,
        amount_units: u64,
        child_is_terminal: bool,
    },
    Settled {
        spent_units: u64,
        released_units: u64,
        signed_usage_digest: Sha256Digest,
        settled_at: ServerInstant,
    },
    Released {
        released_units: u64,
        reason_code: String,
        released_at: ServerInstant,
    },
    Expired {
        released_units: u64,
        expired_at: ServerInstant,
    },
    Cancelled {
        released_units: u64,
        reason_code: String,
        cancelled_at: ServerInstant,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct BudgetReservation {
    pub id: BudgetReservationId,
    pub account_id: BudgetAccountId,
    pub parent_reservation_id: Option<BudgetReservationId>,
    pub purpose: BudgetPurpose,
    pub amount_units: u64,
    /// Original allocation granted to each child. Entries are never removed or overwritten.
    #[serde(default)]
    pub child_allocations: BTreeMap<BudgetReservationId, u64>,
    /// Cumulative allocation returned by each terminal child.
    #[serde(default)]
    pub child_allocation_returns: BTreeMap<BudgetReservationId, u64>,
    pub allocated_units: u64,
    pub spent_units: u64,
    pub released_units: u64,
    pub state: BudgetReservationState,
    pub reserved_at: ServerInstant,
    pub expires_at: Option<ServerInstant>,
    pub terminal_at: Option<ServerInstant>,
    pub terminal_reason: Option<String>,
    pub usage_digest: Option<Sha256Digest>,
    pub version: AggregateVersion,
}

impl BudgetReservation {
    pub fn transition(
        current: Option<&Self>,
        command: &BudgetReservationCommand,
    ) -> Result<Transition<Self, BudgetReservationEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    #[must_use]
    pub const fn unallocated_units(&self) -> u64 {
        self.amount_units - self.allocated_units - self.spent_units
    }

    #[must_use]
    pub fn child_outstanding_units(&self, child_id: BudgetReservationId) -> Option<u64> {
        let allocated = *self.child_allocations.get(&child_id)?;
        let returned = self
            .child_allocation_returns
            .get(&child_id)
            .copied()
            .unwrap_or(0);
        allocated.checked_sub(returned)
    }

    fn all_child_allocations_returned(&self) -> bool {
        self.child_allocations.keys().all(|child_id| {
            self.child_outstanding_units(*child_id)
                .is_some_and(|outstanding| outstanding == 0)
        })
    }

    fn validate_child_ledger(&self) -> Result<(), DomainError> {
        for (child_id, returned_units) in &self.child_allocation_returns {
            let allocated_units =
                self.child_allocations
                    .get(child_id)
                    .ok_or(DomainError::InvariantViolation {
                        invariant: "budget_return_requires_known_child_allocation",
                    })?;
            if returned_units > allocated_units {
                return Err(DomainError::InvariantViolation {
                    invariant: "budget_child_return_must_not_exceed_allocation",
                });
            }
        }

        let outstanding = self.child_allocations.iter().try_fold(
            0_u64,
            |total, (child_id, allocated_units)| {
                let returned_units = self
                    .child_allocation_returns
                    .get(child_id)
                    .copied()
                    .unwrap_or(0);
                let child_outstanding = allocated_units.checked_sub(returned_units).ok_or(
                    DomainError::InvariantViolation {
                        invariant: "budget_child_return_must_not_exceed_allocation",
                    },
                )?;
                total
                    .checked_add(child_outstanding)
                    .ok_or(DomainError::InvariantViolation {
                        invariant: "budget_child_allocation_must_not_overflow",
                    })
            },
        )?;
        if outstanding != self.allocated_units
            || self
                .allocated_units
                .checked_add(self.spent_units)
                .is_none_or(|used| used > self.amount_units)
        {
            return Err(DomainError::InvariantViolation {
                invariant: "budget_child_ledger_must_match_allocated_units",
            });
        }
        Ok(())
    }

    pub fn decide(
        current: Option<&Self>,
        command: &BudgetReservationCommand,
    ) -> Result<BudgetReservationEvent, DomainError> {
        match (current, command) {
            (None, BudgetReservationCommand::Reserve(reserve)) => {
                reserve.validate()?;
                Ok(BudgetReservationEvent::Reserved(reserve.clone()))
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "budget_reservation",
            }),
            (Some(reservation), BudgetReservationCommand::Reserve(_)) => {
                Err(invalid_transition(reservation.state, command.kind()))
            }
            (Some(reservation), command) => {
                if reservation.state.is_terminal() {
                    return Err(invalid_transition(reservation.state, command.kind()));
                }
                if command.expected_version() != Some(reservation.version) {
                    return Err(DomainError::BudgetReservationStale);
                }
                reservation.validate_child_ledger()?;
                match command {
                    BudgetReservationCommand::AllocateChild {
                        child_id,
                        amount_units,
                        ..
                    } if *child_id != reservation.id
                        && *amount_units > 0
                        && !reservation.child_allocations.contains_key(child_id)
                        && *amount_units <= reservation.unallocated_units() =>
                    {
                        Ok(BudgetReservationEvent::ChildAllocated {
                            child_id: *child_id,
                            amount_units: *amount_units,
                        })
                    }
                    BudgetReservationCommand::ReturnChildAllocation {
                        child_id,
                        amount_units,
                        child_is_terminal,
                        ..
                    } if *child_id != reservation.id
                        && *child_is_terminal
                        && *amount_units > 0
                        && reservation
                            .child_outstanding_units(*child_id)
                            .is_some_and(|outstanding| *amount_units <= outstanding) =>
                    {
                        Ok(BudgetReservationEvent::ChildAllocationReturned {
                            child_id: *child_id,
                            amount_units: *amount_units,
                            child_is_terminal: *child_is_terminal,
                        })
                    }
                    BudgetReservationCommand::Settle {
                        spent_units,
                        signed_usage_digest,
                        children_terminal,
                        settled_at,
                        ..
                    } if *children_terminal
                        && reservation.allocated_units == 0
                        && reservation.all_child_allocations_returned()
                        && *spent_units <= reservation.amount_units =>
                    {
                        reservation.validate_time(*settled_at)?;
                        Ok(BudgetReservationEvent::Settled {
                            spent_units: *spent_units,
                            released_units: reservation.amount_units - *spent_units,
                            signed_usage_digest: *signed_usage_digest,
                            settled_at: *settled_at,
                        })
                    }
                    BudgetReservationCommand::Release {
                        reason_code,
                        children_terminal,
                        released_at,
                        ..
                    } if *children_terminal
                        && reservation.allocated_units == 0
                        && reservation.all_child_allocations_returned()
                        && valid_reason(reason_code) =>
                    {
                        reservation.validate_time(*released_at)?;
                        Ok(BudgetReservationEvent::Released {
                            released_units: reservation.amount_units,
                            reason_code: reason_code.clone(),
                            released_at: *released_at,
                        })
                    }
                    BudgetReservationCommand::Expire {
                        children_terminal,
                        now,
                        ..
                    } if *children_terminal
                        && reservation.allocated_units == 0
                        && reservation.all_child_allocations_returned()
                        && reservation
                            .expires_at
                            .is_some_and(|expires| *now >= expires) =>
                    {
                        Ok(BudgetReservationEvent::Expired {
                            released_units: reservation.amount_units,
                            expired_at: *now,
                        })
                    }
                    BudgetReservationCommand::Cancel {
                        reason_code,
                        children_terminal,
                        cancelled_at,
                        ..
                    } if *children_terminal
                        && reservation.allocated_units == 0
                        && reservation.all_child_allocations_returned()
                        && valid_reason(reason_code) =>
                    {
                        reservation.validate_time(*cancelled_at)?;
                        Ok(BudgetReservationEvent::Cancelled {
                            released_units: reservation.amount_units,
                            reason_code: reason_code.clone(),
                            cancelled_at: *cancelled_at,
                        })
                    }
                    _ => Err(invalid_transition(reservation.state, command.kind())),
                }
            }
        }
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &BudgetReservationEvent,
    ) -> Result<Self, DomainError> {
        if let Some(reservation) = current {
            if reservation.state.is_terminal() {
                return Err(invalid_event(reservation.state, event));
            }
            reservation.validate_child_ledger()?;
        }
        match (current, event) {
            (None, BudgetReservationEvent::Reserved(reserve)) => {
                reserve.validate()?;
                Ok(Self {
                    id: reserve.id,
                    account_id: reserve.account_id,
                    parent_reservation_id: reserve.parent_reservation_id,
                    purpose: reserve.purpose,
                    amount_units: reserve.amount_units,
                    child_allocations: BTreeMap::new(),
                    child_allocation_returns: BTreeMap::new(),
                    allocated_units: 0,
                    spent_units: 0,
                    released_units: 0,
                    state: BudgetReservationState::Active,
                    reserved_at: reserve.reserved_at,
                    expires_at: reserve.expires_at,
                    terminal_at: None,
                    terminal_reason: None,
                    usage_digest: None,
                    version: AggregateVersion::new(1),
                })
            }
            (None, _) => Err(DomainError::NotFound {
                resource: "budget_reservation",
            }),
            (
                Some(reservation),
                BudgetReservationEvent::ChildAllocated {
                    child_id,
                    amount_units,
                },
            ) if *child_id != reservation.id
                && *amount_units > 0
                && !reservation.child_allocations.contains_key(child_id)
                && *amount_units <= reservation.unallocated_units() =>
            {
                let mut next = reservation.next_version()?;
                next.child_allocations.insert(*child_id, *amount_units);
                next.allocated_units = next.allocated_units.checked_add(*amount_units).ok_or(
                    DomainError::InvariantViolation {
                        invariant: "budget_allocated_units_must_not_overflow",
                    },
                )?;
                next.validate_child_ledger()?;
                Ok(next)
            }
            (
                Some(reservation),
                BudgetReservationEvent::ChildAllocationReturned {
                    child_id,
                    amount_units,
                    child_is_terminal,
                },
            ) if *child_id != reservation.id
                && *amount_units > 0
                && *child_is_terminal
                && reservation
                    .child_outstanding_units(*child_id)
                    .is_some_and(|outstanding| *amount_units <= outstanding) =>
            {
                let mut next = reservation.next_version()?;
                let returned = next.child_allocation_returns.entry(*child_id).or_default();
                *returned =
                    returned
                        .checked_add(*amount_units)
                        .ok_or(DomainError::InvariantViolation {
                            invariant: "budget_child_return_must_not_overflow",
                        })?;
                next.allocated_units = next.allocated_units.checked_sub(*amount_units).ok_or(
                    DomainError::InvariantViolation {
                        invariant: "budget_allocated_units_must_not_underflow",
                    },
                )?;
                next.validate_child_ledger()?;
                Ok(next)
            }
            (
                Some(reservation),
                BudgetReservationEvent::Settled {
                    spent_units,
                    released_units,
                    signed_usage_digest,
                    settled_at,
                },
            ) if reservation.allocated_units == 0
                && reservation.all_child_allocations_returned()
                && spent_units
                    .checked_add(*released_units)
                    .is_some_and(|total| total == reservation.amount_units)
                && *settled_at >= reservation.reserved_at =>
            {
                let mut next =
                    reservation.terminalize(BudgetReservationState::Settled, *settled_at, None)?;
                next.spent_units = *spent_units;
                next.released_units = *released_units;
                next.usage_digest = Some(*signed_usage_digest);
                Ok(next)
            }
            (
                Some(reservation),
                BudgetReservationEvent::Released {
                    released_units,
                    reason_code,
                    released_at,
                },
            ) if reservation.allocated_units == 0
                && reservation.all_child_allocations_returned()
                && *released_units == reservation.amount_units
                && valid_reason(reason_code)
                && *released_at >= reservation.reserved_at =>
            {
                let mut next = reservation.terminalize(
                    BudgetReservationState::Released,
                    *released_at,
                    Some(reason_code.clone()),
                )?;
                next.released_units = *released_units;
                Ok(next)
            }
            (
                Some(reservation),
                BudgetReservationEvent::Expired {
                    released_units,
                    expired_at,
                },
            ) if reservation.allocated_units == 0
                && reservation.all_child_allocations_returned()
                && *released_units == reservation.amount_units
                && reservation
                    .expires_at
                    .is_some_and(|expires| *expired_at >= expires) =>
            {
                let mut next =
                    reservation.terminalize(BudgetReservationState::Expired, *expired_at, None)?;
                next.released_units = *released_units;
                Ok(next)
            }
            (
                Some(reservation),
                BudgetReservationEvent::Cancelled {
                    released_units,
                    reason_code,
                    cancelled_at,
                },
            ) if reservation.allocated_units == 0
                && reservation.all_child_allocations_returned()
                && *released_units == reservation.amount_units
                && valid_reason(reason_code)
                && *cancelled_at >= reservation.reserved_at =>
            {
                let mut next = reservation.terminalize(
                    BudgetReservationState::Cancelled,
                    *cancelled_at,
                    Some(reason_code.clone()),
                )?;
                next.released_units = *released_units;
                Ok(next)
            }
            (Some(reservation), _) => Err(invalid_event(reservation.state, event)),
        }
    }

    pub fn replay(events: &[BudgetReservationEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "budget_reservation",
        })
    }

    fn validate_time(&self, time: ServerInstant) -> Result<(), DomainError> {
        if time < self.reserved_at {
            Err(DomainError::InvalidArgument {
                field: "terminal_at".into(),
                reason: "must not precede reservation".into(),
            })
        } else {
            Ok(())
        }
    }

    fn next_version(&self) -> Result<Self, DomainError> {
        let mut next = self.clone();
        next.version = self.version.checked_next()?;
        Ok(next)
    }

    fn terminalize(
        &self,
        state: BudgetReservationState,
        terminal_at: ServerInstant,
        reason: Option<String>,
    ) -> Result<Self, DomainError> {
        if self.state != BudgetReservationState::Active || !state.is_terminal() {
            return Err(invalid_transition(
                self.state,
                BudgetReservationCommandKind::Settle,
            ));
        }
        let mut next = self.next_version()?;
        next.state = state;
        next.terminal_at = Some(terminal_at);
        next.terminal_reason = reason;
        Ok(next)
    }
}

/// Checks the cross-row invariants that a repository transaction must preserve.
pub fn validate_reservation_tree<'a>(
    reservations: impl IntoIterator<Item = &'a BudgetReservation>,
) -> Result<(), DomainError> {
    let reservations: Vec<_> = reservations.into_iter().collect();
    let by_id: BTreeMap<_, _> = reservations
        .iter()
        .map(|reservation| (reservation.id, *reservation))
        .collect();
    if by_id.len() != reservations.len() {
        return Err(DomainError::InvariantViolation {
            invariant: "budget_reservation_ids_must_be_unique",
        });
    }

    for reservation in &reservations {
        reservation.validate_child_ledger()?;
    }

    let mut active_children: BTreeMap<BudgetReservationId, BTreeMap<BudgetReservationId, u64>> =
        BTreeMap::new();
    for reservation in &reservations {
        let Some(parent_id) = reservation.parent_reservation_id else {
            continue;
        };
        let parent = by_id
            .get(&parent_id)
            .ok_or(DomainError::InvariantViolation {
                invariant: "budget_parent_reservation_must_exist",
            })?;
        if parent.account_id != reservation.account_id {
            return Err(DomainError::InvariantViolation {
                invariant: "budget_child_must_share_parent_account",
            });
        }
        if reservation.state == BudgetReservationState::Active {
            active_children
                .entry(parent_id)
                .or_default()
                .insert(reservation.id, reservation.amount_units);
        }

        let mut cursor = Some(reservation.id);
        let mut visited = BTreeSet::new();
        while let Some(id) = cursor {
            if !visited.insert(id) {
                return Err(DomainError::InvariantViolation {
                    invariant: "budget_reservation_tree_must_be_acyclic",
                });
            }
            cursor = by_id
                .get(&id)
                .and_then(|current| current.parent_reservation_id);
        }
    }
    for reservation in reservations {
        let expected_active_children: BTreeMap<_, _> = reservation
            .child_allocations
            .keys()
            .filter_map(|child_id| {
                reservation
                    .child_outstanding_units(*child_id)
                    .filter(|outstanding| *outstanding > 0)
                    .map(|outstanding| (*child_id, outstanding))
            })
            .collect();
        let actual_active_children = active_children
            .get(&reservation.id)
            .cloned()
            .unwrap_or_default();
        if actual_active_children != expected_active_children {
            return Err(DomainError::InvariantViolation {
                invariant: "budget_parent_ledger_must_match_active_children",
            });
        }
    }
    Ok(())
}

fn valid_reason(reason: &str) -> bool {
    !reason.is_empty()
        && reason.len() <= 96
        && reason
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
}

fn invalid_transition(
    state: BudgetReservationState,
    command: BudgetReservationCommandKind,
) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.as_str().into(),
    }
}

fn invalid_event(state: BudgetReservationState, event: &BudgetReservationEvent) -> DomainError {
    let command = match event {
        BudgetReservationEvent::Reserved(_) => "reserved",
        BudgetReservationEvent::ChildAllocated { .. } => "child_allocated",
        BudgetReservationEvent::ChildAllocationReturned { .. } => "child_allocation_returned",
        BudgetReservationEvent::Settled { .. } => "settled",
        BudgetReservationEvent::Released { .. } => "released",
        BudgetReservationEvent::Expired { .. } => "expired",
        BudgetReservationEvent::Cancelled { .. } => "cancelled",
    };
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.into(),
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;
    use time::{Duration, macros::datetime};
    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(seconds: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-08 00:00 UTC) + Duration::seconds(seconds))
    }

    fn reserve(amount: u64) -> ReserveBudget {
        ReserveBudget {
            id: id(1),
            account_id: id(2),
            parent_reservation_id: None,
            purpose: BudgetPurpose::InvocationRun(id(3)),
            amount_units: amount,
            source_available_units: amount + 1,
            source_version: AggregateVersion::new(1),
            reserved_at: at(0),
            expires_at: Some(at(100)),
        }
    }

    proptest! {
        #[test]
        fn settlement_conserves_reserved_units(amount in 1_u64..1_000_000, raw_spent in any::<u64>()) {
            let spent = raw_spent % (amount + 1);
            let created = BudgetReservation::transition(
                None,
                &BudgetReservationCommand::Reserve(reserve(amount)),
            ).expect("reserve");
            let settled = BudgetReservation::transition(
                Some(&created.aggregate),
                &BudgetReservationCommand::Settle {
                    expected_version: created.aggregate.version,
                    spent_units: spent,
                    signed_usage_digest: Sha256Digest::of_bytes(b"usage"),
                    children_terminal: true,
                    settled_at: at(1),
                },
            ).expect("settle");
            prop_assert_eq!(
                settled.aggregate.spent_units + settled.aggregate.released_units,
                amount,
            );
            prop_assert_eq!(
                BudgetReservation::replay(&[
                    created.events[0].clone(), settled.events[0].clone()
                ]).expect("replay"),
                settled.aggregate,
            );
        }
    }

    #[test]
    fn child_allocation_does_not_double_count_account_reservation() {
        let created =
            BudgetReservation::transition(None, &BudgetReservationCommand::Reserve(reserve(100)))
                .expect("reserve");
        let allocated = BudgetReservation::transition(
            Some(&created.aggregate),
            &BudgetReservationCommand::AllocateChild {
                expected_version: created.aggregate.version,
                child_id: id(4),
                amount_units: 40,
            },
        )
        .expect("allocate");
        assert_eq!(allocated.aggregate.amount_units, 100);
        assert_eq!(allocated.aggregate.allocated_units, 40);
        assert_eq!(allocated.aggregate.unallocated_units(), 60);

        let child = BudgetReservation::transition(
            None,
            &BudgetReservationCommand::Reserve(ReserveBudget {
                id: id(4),
                account_id: allocated.aggregate.account_id,
                parent_reservation_id: Some(allocated.aggregate.id),
                purpose: BudgetPurpose::VerificationRun(id(5)),
                amount_units: 40,
                source_available_units: 40,
                source_version: allocated.aggregate.version,
                reserved_at: at(1),
                expires_at: Some(at(90)),
            }),
        )
        .expect("child")
        .aggregate;
        validate_reservation_tree([&allocated.aggregate, &child]).expect("valid tree");
    }

    #[test]
    fn unknown_child_allocation_cannot_be_returned_by_command_apply_or_replay() {
        let created =
            BudgetReservation::transition(None, &BudgetReservationCommand::Reserve(reserve(100)))
                .expect("reserve");
        let unknown_child = id(99);
        let command = BudgetReservationCommand::ReturnChildAllocation {
            expected_version: created.aggregate.version,
            child_id: unknown_child,
            amount_units: 40,
            child_is_terminal: true,
        };
        assert!(BudgetReservation::decide(Some(&created.aggregate), &command).is_err());

        let malicious = BudgetReservationEvent::ChildAllocationReturned {
            child_id: unknown_child,
            amount_units: 40,
            child_is_terminal: true,
        };
        assert!(BudgetReservation::apply_event(Some(&created.aggregate), &malicious).is_err());
        assert!(
            BudgetReservation::replay(&[created.events[0].clone(), malicious]).is_err(),
            "replay must not trust a child id absent from the aggregate ledger"
        );
    }

    #[test]
    fn child_allocation_return_is_bounded_and_cannot_be_replayed_twice() {
        let created =
            BudgetReservation::transition(None, &BudgetReservationCommand::Reserve(reserve(100)))
                .expect("reserve");
        let child_id = id(4);
        let allocated = BudgetReservation::transition(
            Some(&created.aggregate),
            &BudgetReservationCommand::AllocateChild {
                expected_version: created.aggregate.version,
                child_id,
                amount_units: 40,
            },
        )
        .expect("allocate");
        assert!(
            BudgetReservation::decide(
                Some(&allocated.aggregate),
                &BudgetReservationCommand::ReturnChildAllocation {
                    expected_version: allocated.aggregate.version,
                    child_id,
                    amount_units: 41,
                    child_is_terminal: true,
                }
            )
            .is_err()
        );

        let returned = BudgetReservation::transition(
            Some(&allocated.aggregate),
            &BudgetReservationCommand::ReturnChildAllocation {
                expected_version: allocated.aggregate.version,
                child_id,
                amount_units: 40,
                child_is_terminal: true,
            },
        )
        .expect("return");
        assert_eq!(
            returned.aggregate.child_allocations.get(&child_id),
            Some(&40)
        );
        assert_eq!(
            returned.aggregate.child_allocation_returns.get(&child_id),
            Some(&40)
        );
        let duplicate = returned.events[0].clone();
        assert!(BudgetReservation::apply_event(Some(&returned.aggregate), &duplicate).is_err());
        assert!(
            BudgetReservation::decide(
                Some(&returned.aggregate),
                &BudgetReservationCommand::ReturnChildAllocation {
                    expected_version: returned.aggregate.version,
                    child_id,
                    amount_units: 1,
                    child_is_terminal: true,
                }
            )
            .is_err()
        );
        assert!(
            BudgetReservation::replay(&[
                created.events[0].clone(),
                allocated.events[0].clone(),
                returned.events[0].clone(),
                duplicate,
            ])
            .is_err()
        );
    }

    #[test]
    fn parent_cannot_settle_while_a_child_allocation_is_outstanding() {
        let created =
            BudgetReservation::transition(None, &BudgetReservationCommand::Reserve(reserve(100)))
                .expect("reserve");
        let allocated = BudgetReservation::transition(
            Some(&created.aggregate),
            &BudgetReservationCommand::AllocateChild {
                expected_version: created.aggregate.version,
                child_id: id(4),
                amount_units: 40,
            },
        )
        .expect("allocate");
        assert!(
            BudgetReservation::decide(
                Some(&allocated.aggregate),
                &BudgetReservationCommand::Settle {
                    expected_version: allocated.aggregate.version,
                    spent_units: 60,
                    signed_usage_digest: Sha256Digest::of_bytes(b"usage"),
                    children_terminal: true,
                    settled_at: at(2),
                }
            )
            .is_err()
        );

        let malicious = BudgetReservationEvent::Settled {
            spent_units: 60,
            released_units: 40,
            signed_usage_digest: Sha256Digest::of_bytes(b"usage"),
            settled_at: at(2),
        };
        assert!(BudgetReservation::apply_event(Some(&allocated.aggregate), &malicious).is_err());
        assert!(
            BudgetReservation::replay(&[
                created.events[0].clone(),
                allocated.events[0].clone(),
                malicious,
            ])
            .is_err()
        );
    }
}
