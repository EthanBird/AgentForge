//! Immutable terminal Submission aggregate.

use serde::{Deserialize, Serialize};

use crate::{
    error::DomainError,
    ids::{
        AggregateVersion, AttemptId, CandidateArtifactId, CandidateId, GitObjectId,
        PackageRevisionId, ProtocolKey, Sha256Digest, SubmissionId, VerificationRunId,
    },
    state::Transition,
};

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SubmissionState {
    Pass,
    Fail,
    Inconclusive,
    Quarantined,
}

impl SubmissionState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pass => "pass",
            Self::Fail => "fail",
            Self::Inconclusive => "inconclusive",
            Self::Quarantined => "quarantined",
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CriterionOutcome {
    Pass,
    Fail,
    Inconclusive,
    Skipped,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompletedStage {
    ProvenanceCheck,
    Reviewing,
    Reproducing,
    CandidateReady,
    SalvageRegistration,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FailureDossier {
    pub evidence_digest: Sha256Digest,
    /// Stable machine finding codes, never raw model output or source snippets.
    pub finding_codes: Vec<ProtocolKey>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AcceptanceFacts {
    pub hard_criteria: Vec<CriterionOutcome>,
    /// `None` means Reviewing has not completed and the fact must stay absent.
    pub unresolved_high_risk_findings: Option<bool>,
    /// `None` means Clean Reproduction has not run. This prevents an early
    /// failure from fabricating a future-stage negative or positive result.
    pub clean_reproduce_passed: Option<bool>,
    pub signature_valid: bool,
    pub lineage_matches: bool,
    pub lease_was_current_at_registration: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubmissionRecord {
    pub id: SubmissionId,
    pub protocol_key: ProtocolKey,
    pub attempt_id: AttemptId,
    pub package_revision_id: PackageRevisionId,
    pub candidate_id: Option<CandidateId>,
    pub candidate_artifact_id: Option<CandidateArtifactId>,
    pub verification_run_id: Option<VerificationRunId>,
    /// Immutable commit from the sealed Candidate lineage.
    pub candidate_commit: Option<GitObjectId>,
    pub submitted_head: Option<GitObjectId>,
    pub tested_head: Option<GitObjectId>,
    pub reviewed_head: Option<GitObjectId>,
    pub manifest_digest: Sha256Digest,
    pub evidence_digest: Option<Sha256Digest>,
    pub lease_fencing_token_hash: Sha256Digest,
    pub state: SubmissionState,
    pub completed_stage: CompletedStage,
    pub failure_dossier: Option<FailureDossier>,
    pub acceptance_facts: Option<AcceptanceFacts>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SubmissionCommandKind {
    FinalizeCandidateSubmission,
    RegisterSalvage,
}

impl SubmissionCommandKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::FinalizeCandidateSubmission => "finalize_candidate_submission",
            Self::RegisterSalvage => "register_salvage",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SubmissionCommand {
    FinalizeCandidateSubmission { record: SubmissionRecord },
    RegisterSalvage { record: SubmissionRecord },
}

impl SubmissionCommand {
    #[must_use]
    pub const fn kind(&self) -> SubmissionCommandKind {
        match self {
            Self::FinalizeCandidateSubmission { .. } => {
                SubmissionCommandKind::FinalizeCandidateSubmission
            }
            Self::RegisterSalvage { .. } => SubmissionCommandKind::RegisterSalvage,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum SubmissionEvent {
    CandidateSubmissionFinalized { record: SubmissionRecord },
    SalvageRegistered { record: SubmissionRecord },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SubmissionTransitionRule {
    pub command: SubmissionCommandKind,
    pub to: SubmissionState,
}

/// Submission has no persisted nonterminal state. These are creation rows from
/// the conceptual `absent` state; every resulting state is terminal.
pub const TRANSITION_TABLE: &[SubmissionTransitionRule] = &[
    SubmissionTransitionRule {
        command: SubmissionCommandKind::FinalizeCandidateSubmission,
        to: SubmissionState::Pass,
    },
    SubmissionTransitionRule {
        command: SubmissionCommandKind::FinalizeCandidateSubmission,
        to: SubmissionState::Fail,
    },
    SubmissionTransitionRule {
        command: SubmissionCommandKind::FinalizeCandidateSubmission,
        to: SubmissionState::Inconclusive,
    },
    SubmissionTransitionRule {
        command: SubmissionCommandKind::RegisterSalvage,
        to: SubmissionState::Quarantined,
    },
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Submission {
    pub id: SubmissionId,
    pub protocol_key: ProtocolKey,
    pub attempt_id: AttemptId,
    pub package_revision_id: PackageRevisionId,
    pub candidate_id: Option<CandidateId>,
    pub candidate_artifact_id: Option<CandidateArtifactId>,
    pub verification_run_id: Option<VerificationRunId>,
    pub candidate_commit: Option<GitObjectId>,
    pub submitted_head: Option<GitObjectId>,
    pub tested_head: Option<GitObjectId>,
    pub reviewed_head: Option<GitObjectId>,
    pub manifest_digest: Sha256Digest,
    pub evidence_digest: Option<Sha256Digest>,
    pub lease_fencing_token_hash: Sha256Digest,
    pub state: SubmissionState,
    pub completed_stage: CompletedStage,
    pub failure_dossier: Option<FailureDossier>,
    pub acceptance_facts: Option<AcceptanceFacts>,
    pub version: AggregateVersion,
}

impl Submission {
    pub fn transition(
        current: Option<&Self>,
        command: &SubmissionCommand,
    ) -> Result<Transition<Self, SubmissionEvent>, DomainError> {
        let event = Self::decide(current, command)?;
        let aggregate = Self::apply_event(current, &event)?;
        Ok(Transition::one(aggregate, event))
    }

    pub fn decide(
        current: Option<&Self>,
        command: &SubmissionCommand,
    ) -> Result<SubmissionEvent, DomainError> {
        if let Some(submission) = current {
            return Err(invalid_transition(
                submission.state,
                command.kind().as_str(),
            ));
        }
        match command {
            SubmissionCommand::FinalizeCandidateSubmission { record } => {
                validate_candidate(record)?;
                Ok(SubmissionEvent::CandidateSubmissionFinalized {
                    record: record.clone(),
                })
            }
            SubmissionCommand::RegisterSalvage { record } => {
                validate_salvage(record)?;
                Ok(SubmissionEvent::SalvageRegistered {
                    record: record.clone(),
                })
            }
        }
    }

    pub fn apply_event(
        current: Option<&Self>,
        event: &SubmissionEvent,
    ) -> Result<Self, DomainError> {
        if let Some(submission) = current {
            return Err(invalid_transition(submission.state, event.name()));
        }
        let record = match event {
            SubmissionEvent::CandidateSubmissionFinalized { record } => {
                validate_candidate(record)?;
                record
            }
            SubmissionEvent::SalvageRegistered { record } => {
                validate_salvage(record)?;
                record
            }
        };
        Ok(Self::from_record(record.clone()))
    }

    pub fn replay(events: &[SubmissionEvent]) -> Result<Self, DomainError> {
        let mut current = None;
        for event in events {
            current = Some(Self::apply_event(current.as_ref(), event)?);
        }
        current.ok_or(DomainError::NotFound {
            resource: "submission",
        })
    }

    /// Deterministic predicate used by the Integration enqueue guard.
    #[must_use]
    pub fn candidate_ready(&self) -> bool {
        if self.state != SubmissionState::Pass
            || self.completed_stage != CompletedStage::CandidateReady
            || self.candidate_id.is_none()
            || self.candidate_artifact_id.is_none()
            || self.verification_run_id.is_none()
            || self.candidate_commit.is_none()
            || self.evidence_digest.is_none()
        {
            return false;
        }
        let heads_match = match (
            self.candidate_commit.as_ref(),
            self.submitted_head.as_ref(),
            self.tested_head.as_ref(),
            self.reviewed_head.as_ref(),
        ) {
            (Some(candidate), Some(submitted), Some(tested), Some(reviewed)) => {
                candidate == submitted && submitted == tested && tested == reviewed
            }
            _ => false,
        };
        let Some(facts) = &self.acceptance_facts else {
            return false;
        };
        heads_match
            && !facts.hard_criteria.is_empty()
            && facts
                .hard_criteria
                .iter()
                .all(|outcome| *outcome == CriterionOutcome::Pass)
            && facts.unresolved_high_risk_findings == Some(false)
            && facts.clean_reproduce_passed == Some(true)
            && facts.signature_valid
            && facts.lineage_matches
            && facts.lease_was_current_at_registration
    }

    fn from_record(record: SubmissionRecord) -> Self {
        Self {
            id: record.id,
            protocol_key: record.protocol_key,
            attempt_id: record.attempt_id,
            package_revision_id: record.package_revision_id,
            candidate_id: record.candidate_id,
            candidate_artifact_id: record.candidate_artifact_id,
            verification_run_id: record.verification_run_id,
            candidate_commit: record.candidate_commit,
            submitted_head: record.submitted_head,
            tested_head: record.tested_head,
            reviewed_head: record.reviewed_head,
            manifest_digest: record.manifest_digest,
            evidence_digest: record.evidence_digest,
            lease_fencing_token_hash: record.lease_fencing_token_hash,
            state: record.state,
            completed_stage: record.completed_stage,
            failure_dossier: record.failure_dossier,
            acceptance_facts: record.acceptance_facts,
            version: AggregateVersion::new(1),
        }
    }
}

impl SubmissionEvent {
    #[must_use]
    pub const fn name(&self) -> &'static str {
        match self {
            Self::CandidateSubmissionFinalized { .. } => "candidate_submission_finalized",
            Self::SalvageRegistered { .. } => "salvage_registered",
        }
    }
}

fn validate_candidate(record: &SubmissionRecord) -> Result<(), DomainError> {
    if record.state == SubmissionState::Quarantined
        || record.completed_stage == CompletedStage::SalvageRegistration
        || record.candidate_id.is_none()
        || record.candidate_artifact_id.is_none()
        || record.verification_run_id.is_none()
        || record.candidate_commit.is_none()
        || record.submitted_head.is_none()
    {
        return unacceptable("candidate_lineage");
    }
    let Some(facts) = &record.acceptance_facts else {
        return unacceptable("acceptance_facts");
    };
    if !facts.signature_valid || !facts.lineage_matches || !facts.lease_was_current_at_registration
    {
        return unacceptable("provenance");
    }
    if record.submitted_head != record.candidate_commit {
        return Err(DomainError::HeadMismatch);
    }
    match record.state {
        SubmissionState::Pass => {
            if record.completed_stage != CompletedStage::CandidateReady
                || record.failure_dossier.is_some()
                || record.evidence_digest.is_none()
                || facts.hard_criteria.is_empty()
                || facts
                    .hard_criteria
                    .iter()
                    .any(|outcome| *outcome != CriterionOutcome::Pass)
                || facts.unresolved_high_risk_findings != Some(false)
                || facts.clean_reproduce_passed != Some(true)
            {
                return unacceptable("candidate_ready");
            }
            match (
                record.candidate_commit.as_ref(),
                record.submitted_head.as_ref(),
                record.tested_head.as_ref(),
                record.reviewed_head.as_ref(),
            ) {
                (Some(candidate), Some(submitted), Some(tested), Some(reviewed))
                    if candidate == submitted && submitted == tested && tested == reviewed => {}
                _ => return Err(DomainError::HeadMismatch),
            }
        }
        SubmissionState::Fail | SubmissionState::Inconclusive => {
            if record.failure_dossier.is_none()
                || matches!(
                    record.completed_stage,
                    CompletedStage::CandidateReady | CompletedStage::SalvageRegistration
                )
            {
                return unacceptable("failure_dossier");
            }
            validate_failure_stage_shape(record, facts)?;
        }
        SubmissionState::Quarantined => return unacceptable("terminal_outcome"),
    }
    Ok(())
}

fn validate_salvage(record: &SubmissionRecord) -> Result<(), DomainError> {
    if record.state != SubmissionState::Quarantined
        || record.completed_stage != CompletedStage::SalvageRegistration
        || record.candidate_id.is_some()
        || record.candidate_artifact_id.is_some()
        || record.verification_run_id.is_some()
        || record.candidate_commit.is_some()
        || record.submitted_head.is_some()
        || record.tested_head.is_some()
        || record.reviewed_head.is_some()
        || record.evidence_digest.is_some()
        || record.failure_dossier.is_some()
        || record.acceptance_facts.is_some()
    {
        return unacceptable("salvage_quarantine_shape");
    }
    Ok(())
}

fn validate_failure_stage_shape(
    record: &SubmissionRecord,
    facts: &AcceptanceFacts,
) -> Result<(), DomainError> {
    let candidate =
        record
            .candidate_commit
            .as_ref()
            .ok_or_else(|| DomainError::SubmissionNotAcceptable {
                failed_checks: vec!["candidate_commit".into()],
            })?;
    match record.completed_stage {
        CompletedStage::ProvenanceCheck => {
            if record.evidence_digest.is_some()
                || record.reviewed_head.is_some()
                || record.tested_head.is_some()
                || !facts.hard_criteria.is_empty()
                || facts.unresolved_high_risk_findings.is_some()
                || facts.clean_reproduce_passed.is_some()
            {
                return unacceptable("provenance_stage_shape");
            }
        }
        CompletedStage::Reviewing => {
            if record.evidence_digest.is_some()
                || record.reviewed_head.as_ref() != Some(candidate)
                || record.tested_head.is_some()
                || !facts.hard_criteria.is_empty()
                || facts.unresolved_high_risk_findings.is_none()
                || facts.clean_reproduce_passed.is_some()
            {
                return unacceptable("reviewing_stage_shape");
            }
        }
        CompletedStage::Reproducing => {
            if record.reviewed_head.as_ref() != Some(candidate)
                || record.tested_head.as_ref() != Some(candidate)
                || facts.unresolved_high_risk_findings.is_none()
                || facts.clean_reproduce_passed != Some(false)
            {
                return unacceptable("reproducing_stage_shape");
            }
        }
        CompletedStage::CandidateReady | CompletedStage::SalvageRegistration => {
            return unacceptable("failure_stage");
        }
    }
    Ok(())
}

fn unacceptable(check: &str) -> Result<(), DomainError> {
    Err(DomainError::SubmissionNotAcceptable {
        failed_checks: vec![check.into()],
    })
}

fn invalid_transition(state: SubmissionState, command: &str) -> DomainError {
    DomainError::InvalidTransition {
        from: state.as_str().into(),
        command: command.into(),
    }
}
