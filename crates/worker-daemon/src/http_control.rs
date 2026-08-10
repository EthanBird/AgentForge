//! Bounded loopback HTTP/1 adapter for the MVP control-plane command API.

use std::{net::SocketAddr, time::Duration};

use agentforge_application::{
    AttemptProgressView, CandidateArtifactChunkReceipt, CandidateArtifactView, ClaimPackageInput,
    ClaimedWork, CompleteCandidateArtifactInput, InitCandidateArtifactInput, LeaseView,
    ListOffersQuery, MvpCommand, MvpError, MvpFuture, MvpRemoteError, MvpResult, OfferView,
    PortError, RecordCandidateInput, RecordedCandidate, ReleaseLeaseInput, RenewLeaseInput,
    ReportAttemptProgressInput, UploadCandidateArtifactChunkInput,
};
use agentforge_domain::{LeaseId, ProjectId};
use serde::{Deserialize, de::DeserializeOwned};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};

use crate::{config::WorkerDaemonConfig, lifecycle::WorkerControlPlane};

const MAX_HEADER_BYTES: usize = 32 * 1024;
const MAX_HEADERS: usize = 64;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct LoopbackHttpControlPlane {
    address: SocketAddr,
    timeout: Duration,
    max_response_bytes: usize,
}

impl LoopbackHttpControlPlane {
    pub fn from_config(config: &WorkerDaemonConfig) -> Result<Self, crate::config::ConfigError> {
        config.validate()?;
        Ok(Self {
            address: config.control_plane_address()?,
            timeout: Duration::from_secs(u64::from(config.request_timeout_seconds)),
            max_response_bytes: usize::try_from(config.max_response_bytes)
                .map_err(|_| crate::config::ConfigError::Invalid)?,
        })
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: &'static str,
        path: String,
        expected_status: u16,
        body: Option<Vec<u8>>,
        command: Option<&agentforge_application::MvpCommandContext>,
    ) -> MvpResult<T> {
        let (status, content_type, response_body) =
            self.exchange(method, path, body, command).await?;
        if !content_type.as_deref().is_some_and(is_json_content_type) {
            return Err(MvpError::Port(PortError::Serialization));
        }
        if status == expected_status {
            decode_json(&response_body)
        } else {
            decode_remote_error(status, &response_body)
        }
    }

    async fn send_empty(
        &self,
        method: &'static str,
        path: String,
        expected_status: u16,
        body: Option<Vec<u8>>,
        command: Option<&agentforge_application::MvpCommandContext>,
    ) -> MvpResult<()> {
        let (status, content_type, response_body) =
            self.exchange(method, path, body, command).await?;
        if status == expected_status {
            if response_body.is_empty() && content_type.as_deref().is_none_or(is_json_content_type)
            {
                return Ok(());
            }
            return Err(MvpError::Port(PortError::Serialization));
        }
        if !content_type.as_deref().is_some_and(is_json_content_type) {
            return Err(MvpError::Port(PortError::Serialization));
        }
        decode_remote_error(status, &response_body)
    }

    async fn exchange(
        &self,
        method: &'static str,
        path: String,
        body: Option<Vec<u8>>,
        command: Option<&agentforge_application::MvpCommandContext>,
    ) -> MvpResult<(u16, Option<String>, Vec<u8>)> {
        let request = self.build_request(method, &path, body.as_deref(), command)?;
        let operation = async {
            let mut stream = TcpStream::connect(self.address)
                .await
                .map_err(|_| MvpError::Port(PortError::Unavailable))?;
            let peer = stream
                .peer_addr()
                .map_err(|_| MvpError::Port(PortError::Unavailable))?;
            if !peer.ip().is_loopback() {
                return Err(MvpError::Port(PortError::Integrity));
            }
            stream
                .write_all(&request)
                .await
                .map_err(|_| MvpError::Port(PortError::Unavailable))?;
            stream
                .flush()
                .await
                .map_err(|_| MvpError::Port(PortError::Unavailable))?;
            read_response(&mut stream, self.max_response_bytes).await
        };
        tokio::time::timeout(self.timeout, operation)
            .await
            .map_err(|_| MvpError::Port(PortError::Unavailable))?
    }

    fn build_request(
        &self,
        method: &'static str,
        path: &str,
        body: Option<&[u8]>,
        command: Option<&agentforge_application::MvpCommandContext>,
    ) -> MvpResult<Vec<u8>> {
        if !matches!(method, "GET" | "POST" | "PUT")
            || !path.starts_with('/')
            || path.contains('\r')
            || path.contains('\n')
        {
            return Err(MvpError::Port(PortError::Serialization));
        }
        let mut request = format!(
            "{method} {path} HTTP/1.1\r\nHost: {}\r\nAccept: application/json\r\nConnection: close\r\nUser-Agent: agentforge-worker/0.1\r\n",
            self.address
        );
        if let Some(context) = command {
            let expected_version = context
                .expected_version
                .ok_or(MvpError::Port(PortError::Serialization))?;
            let idempotency_key = safe_header(context.idempotency_key.as_str())?;
            request.push_str(&format!(
                "Idempotency-Key: {idempotency_key}\r\nX-AgentForge-Command-Id: {}\r\nX-AgentForge-Correlation-Id: {}\r\nIf-Match: \"{}\"\r\n",
                context.command_id,
                context.correlation_id,
                expected_version.get()
            ));
            if let Some(causation_id) = context.causation_id {
                request.push_str(&format!("X-AgentForge-Causation-Id: {causation_id}\r\n"));
            }
        }
        let body = body.unwrap_or_default();
        if !body.is_empty() {
            request.push_str("Content-Type: application/json\r\n");
        }
        request.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));
        let mut bytes = request.into_bytes();
        bytes.extend_from_slice(body);
        Ok(bytes)
    }

    fn command_body<T: serde::Serialize>(command: &MvpCommand<T>) -> MvpResult<Vec<u8>> {
        serde_json::to_vec(&command.input).map_err(|_| MvpError::Port(PortError::Serialization))
    }
}

impl WorkerControlPlane for LoopbackHttpControlPlane {
    fn list_offers(&self, query: ListOffersQuery) -> MvpFuture<'_, Vec<OfferView>> {
        Box::pin(async move {
            self.send(
                "GET",
                format!(
                    "/api/v1/projects/{}/offers?limit={}",
                    query.project_id, query.limit
                ),
                200,
                None,
                None,
            )
            .await
        })
    }

    fn claim_package<'a>(
        &'a self,
        command: &'a MvpCommand<ClaimPackageInput>,
    ) -> MvpFuture<'a, ClaimedWork> {
        Box::pin(async move {
            self.send(
                "POST",
                format!(
                    "/api/v1/projects/{}/packages/{}/claim",
                    command.input.project_id, command.input.package_id
                ),
                201,
                Some(Self::command_body(command)?),
                Some(&command.context),
            )
            .await
        })
    }

    fn report_attempt_progress<'a>(
        &'a self,
        command: &'a MvpCommand<ReportAttemptProgressInput>,
    ) -> MvpFuture<'a, AttemptProgressView> {
        Box::pin(async move {
            self.send(
                "POST",
                format!(
                    "/api/v1/projects/{}/attempts/{}/progress",
                    command.input.project_id, command.input.attempt_id
                ),
                200,
                Some(Self::command_body(command)?),
                Some(&command.context),
            )
            .await
        })
    }

    fn init_candidate_artifact<'a>(
        &'a self,
        command: &'a MvpCommand<InitCandidateArtifactInput>,
    ) -> MvpFuture<'a, CandidateArtifactView> {
        Box::pin(async move {
            self.send(
                "POST",
                format!(
                    "/api/v1/projects/{}/attempts/{}/candidate-artifacts",
                    command.input.project_id, command.input.attempt_id
                ),
                201,
                Some(Self::command_body(command)?),
                Some(&command.context),
            )
            .await
        })
    }

    fn upload_candidate_artifact_chunk<'a>(
        &'a self,
        command: &'a MvpCommand<UploadCandidateArtifactChunkInput>,
    ) -> MvpFuture<'a, CandidateArtifactChunkReceipt> {
        Box::pin(async move {
            self.send_empty(
                "PUT",
                format!(
                    "/api/v1/projects/{}/candidate-artifacts/{}/chunks/{}",
                    command.input.project_id, command.input.artifact_id, command.input.chunk_index
                ),
                204,
                Some(Self::command_body(command)?),
                Some(&command.context),
            )
            .await?;
            Ok(CandidateArtifactChunkReceipt {
                artifact_id: command.input.artifact_id,
                chunk_index: command.input.chunk_index,
                digest: command.input.digest,
                size_bytes: u32::try_from(command.input.content.len())
                    .map_err(|_| MvpError::Port(PortError::Serialization))?,
                artifact_version: command
                    .context
                    .expected_version
                    .ok_or(MvpError::Port(PortError::Serialization))?,
            })
        })
    }

    fn complete_candidate_artifact<'a>(
        &'a self,
        command: &'a MvpCommand<CompleteCandidateArtifactInput>,
    ) -> MvpFuture<'a, CandidateArtifactView> {
        Box::pin(async move {
            self.send(
                "POST",
                format!(
                    "/api/v1/projects/{}/candidate-artifacts/{}/complete",
                    command.input.project_id, command.input.artifact_id
                ),
                200,
                Some(Self::command_body(command)?),
                Some(&command.context),
            )
            .await
        })
    }

    fn record_candidate<'a>(
        &'a self,
        command: &'a MvpCommand<RecordCandidateInput>,
    ) -> MvpFuture<'a, RecordedCandidate> {
        Box::pin(async move {
            self.send(
                "POST",
                format!(
                    "/api/v1/projects/{}/attempts/{}/candidates",
                    command.input.project_id, command.input.attempt_id
                ),
                201,
                Some(Self::command_body(command)?),
                Some(&command.context),
            )
            .await
        })
    }

    fn get_lease(&self, project_id: ProjectId, lease_id: LeaseId) -> MvpFuture<'_, LeaseView> {
        Box::pin(async move {
            self.send(
                "GET",
                format!("/api/v1/projects/{project_id}/leases/{lease_id}"),
                200,
                None,
                None,
            )
            .await
        })
    }

    fn renew_lease<'a>(
        &'a self,
        command: &'a MvpCommand<RenewLeaseInput>,
    ) -> MvpFuture<'a, LeaseView> {
        Box::pin(async move {
            self.send(
                "POST",
                format!(
                    "/api/v1/projects/{}/leases/{}/renew",
                    command.input.project_id, command.input.lease_id
                ),
                200,
                Some(Self::command_body(command)?),
                Some(&command.context),
            )
            .await
        })
    }

    fn release_lease<'a>(
        &'a self,
        command: &'a MvpCommand<ReleaseLeaseInput>,
    ) -> MvpFuture<'a, LeaseView> {
        Box::pin(async move {
            self.send(
                "POST",
                format!(
                    "/api/v1/projects/{}/leases/{}/release",
                    command.input.project_id, command.input.lease_id
                ),
                200,
                Some(Self::command_body(command)?),
                Some(&command.context),
            )
            .await
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RemoteErrorBody {
    code: String,
    message: String,
    retryable: bool,
}

fn decode_json<T: DeserializeOwned>(bytes: &[u8]) -> MvpResult<T> {
    let value = agentforge_protocol::strict_json::from_slice(bytes)
        .map_err(|_| MvpError::Port(PortError::Serialization))?;
    serde_json::from_value(value).map_err(|_| MvpError::Port(PortError::Serialization))
}

fn decode_remote_error<T>(status: u16, bytes: &[u8]) -> MvpResult<T> {
    let body: RemoteErrorBody = decode_json(bytes)?;
    let error =
        MvpRemoteError::parse(&body.code).ok_or(MvpError::Port(PortError::Serialization))?;
    if status != remote_status(error)
        || body.retryable != error.retryable()
        || body.message.is_empty()
        || body.message.len() > 1_024
        || body.message.chars().any(char::is_control)
    {
        return Err(MvpError::Port(PortError::Serialization));
    }
    Err(MvpError::Remote(error))
}

const fn remote_status(error: MvpRemoteError) -> u16 {
    match error {
        MvpRemoteError::ResourceIdInvalid
        | MvpRemoteError::CommandHeaderInvalid
        | MvpRemoteError::PathBodyMismatch => 400,
        MvpRemoteError::ProjectAccessDenied | MvpRemoteError::PolicyDenied => 403,
        MvpRemoteError::NotFound => 404,
        MvpRemoteError::LeaseExpired => 410,
        MvpRemoteError::StaleVersion => 412,
        MvpRemoteError::Conflict
        | MvpRemoteError::LeaseStale
        | MvpRemoteError::IdempotencyKeyReused
        | MvpRemoteError::TransitionInvalid
        | MvpRemoteError::PackageNotClaimable
        | MvpRemoteError::CandidateArtifactNotComplete
        | MvpRemoteError::IdempotencyResultExpired
        | MvpRemoteError::IdempotencyResultLegacy => 409,
        MvpRemoteError::ArgumentInvalid
        | MvpRemoteError::PackageHashMismatch
        | MvpRemoteError::EvidenceInvalid => 422,
        MvpRemoteError::CommandServiceUnavailable | MvpRemoteError::Unavailable => 503,
        MvpRemoteError::StorageIntegrity | MvpRemoteError::Serialization => 500,
    }
}

fn safe_header(value: &str) -> MvpResult<&str> {
    if value.is_empty() || !value.bytes().all(|byte| (0x20..=0x7e).contains(&byte)) {
        return Err(MvpError::Port(PortError::Serialization));
    }
    Ok(value)
}

fn is_json_content_type(value: &str) -> bool {
    value
        .split(';')
        .next()
        .is_some_and(|mime| mime.trim().eq_ignore_ascii_case("application/json"))
}

async fn read_response(
    stream: &mut TcpStream,
    max_body_bytes: usize,
) -> MvpResult<(u16, Option<String>, Vec<u8>)> {
    let mut received = Vec::with_capacity(8 * 1024);
    let mut chunk = [0_u8; 8 * 1024];
    let header_end = loop {
        if let Some(position) = received.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        if received.len() >= MAX_HEADER_BYTES {
            return Err(MvpError::Port(PortError::Serialization));
        }
        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|_| MvpError::Port(PortError::Unavailable))?;
        if read == 0 {
            return Err(MvpError::Port(PortError::Serialization));
        }
        received.extend_from_slice(&chunk[..read]);
        if received.len() > MAX_HEADER_BYTES + max_body_bytes {
            return Err(MvpError::Port(PortError::Serialization));
        }
    };
    if header_end > MAX_HEADER_BYTES {
        return Err(MvpError::Port(PortError::Serialization));
    }

    let mut headers = [httparse::EMPTY_HEADER; MAX_HEADERS];
    let mut response = httparse::Response::new(&mut headers);
    let parsed = response
        .parse(&received[..header_end])
        .map_err(|_| MvpError::Port(PortError::Serialization))?;
    if !parsed.is_complete() || parsed.unwrap() != header_end || response.version != Some(1) {
        return Err(MvpError::Port(PortError::Serialization));
    }
    let status = response
        .code
        .filter(|status| (200..=599).contains(status))
        .ok_or(MvpError::Port(PortError::Serialization))?;
    let mut content_length = None;
    let mut content_type = None;
    for header in response.headers {
        if header.name.eq_ignore_ascii_case("transfer-encoding") {
            return Err(MvpError::Port(PortError::Serialization));
        }
        if header.name.eq_ignore_ascii_case("content-length") {
            if content_length.is_some() {
                return Err(MvpError::Port(PortError::Serialization));
            }
            let value = std::str::from_utf8(header.value)
                .ok()
                .and_then(|value| value.parse::<usize>().ok())
                .filter(|value| *value <= max_body_bytes)
                .ok_or(MvpError::Port(PortError::Serialization))?;
            content_length = Some(value);
        } else if header.name.eq_ignore_ascii_case("content-type") {
            if content_type.is_some() {
                return Err(MvpError::Port(PortError::Serialization));
            }
            content_type = Some(
                std::str::from_utf8(header.value)
                    .map_err(|_| MvpError::Port(PortError::Serialization))?
                    .to_owned(),
            );
        }
    }
    let content_length = match (content_length, status) {
        (Some(content_length), _) => content_length,
        (None, 204) => 0,
        (None, _) => return Err(MvpError::Port(PortError::Serialization)),
    };
    let buffered_body = received.len() - header_end;
    if buffered_body > content_length {
        return Err(MvpError::Port(PortError::Serialization));
    }
    while received.len() - header_end < content_length {
        let remaining = content_length - (received.len() - header_end);
        let read_limit = remaining.min(chunk.len());
        let read = stream
            .read(&mut chunk[..read_limit])
            .await
            .map_err(|_| MvpError::Port(PortError::Unavailable))?;
        if read == 0 {
            return Err(MvpError::Port(PortError::Serialization));
        }
        received.extend_from_slice(&chunk[..read]);
    }
    Ok((status, content_type, received.split_off(header_end)))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use agentforge_application::MvpCommandContext;
    use agentforge_domain::{
        ActorId, AggregateVersion, AttemptId, CandidateArtifactId, CandidateArtifactState,
        CandidateId, CommandId, CorrelationId, ExecutorId, FencingToken, GitObjectId,
        IdempotencyKey, NodeId, PackageRevision, PackageRevisionId, ProtocolKey, ServerInstant,
        Sha256Digest, candidate::VerificationRunState, work_package::WorkPackageState,
    };
    use axum::{
        Json, Router,
        extract::{Path, Query, State},
        http::{HeaderMap, StatusCode},
        routing::{get, post, put},
    };
    use serde::Deserialize;
    use time::macros::datetime;
    use uuid::Uuid;

    use super::*;

    fn id<T: From<Uuid>>(byte: u8) -> T {
        T::from(Uuid::from_bytes([byte; 16]))
    }

    fn at(second: i64) -> ServerInstant {
        ServerInstant(datetime!(2026-08-10 00:00 UTC) + time::Duration::seconds(second))
    }

    fn execution() -> agentforge_application::PackageExecutionSnapshot {
        let canonical_document = serde_json::json!({"package": "http-worker"});
        agentforge_application::PackageExecutionSnapshot {
            revision: PackageRevision::new(1).expect("revision"),
            package_hash: Sha256Digest::of_bytes(
                serde_json_canonicalizer::to_vec(&canonical_document).expect("JCS"),
            ),
            base_commit: GitObjectId::new("1".repeat(40)).expect("commit"),
            git_object_format: "sha1".to_owned(),
            canonical_document,
            input_snapshot: serde_json::json!({"fixtures": []}),
        }
    }

    fn offer() -> OfferView {
        OfferView {
            project_id: id(1),
            package_id: id(2),
            package_key: ProtocolKey::new("http-worker").expect("key"),
            revision_id: id(3),
            revision: PackageRevision::new(1).expect("revision"),
            state: WorkPackageState::Offered,
            priority: 10,
            attempts_started: 0,
            max_attempts: 3,
            version: AggregateVersion::new(1),
        }
    }

    fn claimed() -> ClaimedWork {
        ClaimedWork {
            project_id: id(1),
            package_id: id(2),
            revision_id: id::<PackageRevisionId>(3),
            attempt_id: id(4),
            lease_id: id(5),
            fencing_token: FencingToken::new(1).expect("generation"),
            granted_at: at(0),
            expires_at: at(60),
            max_expires_at: at(600),
            package_version: AggregateVersion::new(2),
            attempt_version: AggregateVersion::new(2),
            lease_version: AggregateVersion::new(1),
            execution: execution(),
        }
    }

    fn config(address: SocketAddr, journal_path: std::path::PathBuf) -> WorkerDaemonConfig {
        WorkerDaemonConfig {
            schema_version: 1,
            actor_id: id::<ActorId>(6),
            executor_id: id::<ExecutorId>(7),
            node_id: id::<NodeId>(8),
            runtime_fingerprint: Sha256Digest::of_bytes("http-adapter-fixture"),
            project_ids: vec![id(1)],
            control_plane_url: format!("http://{address}"),
            request_timeout_seconds: 5,
            max_response_bytes: 2_097_152,
            journal_path,
            capacity: 1,
            offer_limit: 10,
            lease_seconds: 60,
            max_lease_seconds: 600,
            renew_before_seconds: 15,
            extend_by_seconds: 30,
            tick_seconds: 5,
            driver_mode: crate::config::WorkerDriverMode::LeaseOnly,
            max_turns: 3,
            operation_timeout_seconds: 60,
        }
    }

    #[derive(Clone)]
    struct ApiState {
        captured: Arc<Mutex<Option<(HeaderMap, ClaimPackageInput)>>>,
    }

    #[derive(Deserialize)]
    struct LimitQuery {
        limit: u16,
    }

    async fn offers_handler(
        Path(project): Path<String>,
        Query(query): Query<LimitQuery>,
    ) -> Json<Vec<OfferView>> {
        assert_eq!(project, id::<ProjectId>(1).to_string());
        assert_eq!(query.limit, 10);
        Json(vec![offer()])
    }

    async fn claim_handler(
        State(state): State<ApiState>,
        Path((project, package)): Path<(String, String)>,
        headers: HeaderMap,
        Json(input): Json<ClaimPackageInput>,
    ) -> (StatusCode, Json<ClaimedWork>) {
        assert_eq!(project, id::<ProjectId>(1).to_string());
        assert_eq!(package, id::<agentforge_domain::PackageId>(2).to_string());
        *state.captured.lock().expect("capture lock") = Some((headers, input));
        (StatusCode::CREATED, Json(claimed()))
    }

    async fn attempt_progress_handler(
        Path((project, attempt)): Path<(String, String)>,
        headers: HeaderMap,
        Json(input): Json<ReportAttemptProgressInput>,
    ) -> Json<AttemptProgressView> {
        assert_eq!(project, id::<ProjectId>(1).to_string());
        assert_eq!(attempt, id::<AttemptId>(4).to_string());
        assert_eq!(
            input.stage,
            agentforge_application::AttemptProgressStage::Preparing
        );
        assert_eq!(headers["idempotency-key"], "worker-attempt-progress");
        assert_eq!(headers["if-match"], "\"2\"");
        Json(AttemptProgressView {
            project_id: id(1),
            package_id: id(2),
            attempt_id: id(4),
            lease_id: id(5),
            fencing_token: FencingToken::new(1).expect("generation"),
            state: agentforge_domain::attempt::AttemptState::Preparing,
            semantic_progress_seq: 1,
            updated_at: at(1),
            version: AggregateVersion::new(4),
        })
    }

    fn artifact_view(state: CandidateArtifactState) -> CandidateArtifactView {
        let content = b"candidate-bundle";
        CandidateArtifactView {
            project_id: id(1),
            artifact_id: id(20),
            candidate_id: id(21),
            attempt_id: id(4),
            package_id: id(2),
            revision_id: id(3),
            lease_id: id(5),
            fencing_token: FencingToken::new(1).expect("generation"),
            candidate_commit: GitObjectId::new("2".repeat(40)).expect("candidate"),
            tree_hash: GitObjectId::new("3".repeat(40)).expect("tree"),
            state,
            expected_bundle_digest: Sha256Digest::of_bytes(content),
            expected_bundle_size_bytes: u64::try_from(content.len()).expect("bundle size"),
            chunk_digests: vec![Sha256Digest::of_bytes(content)],
            bundle: None,
            created_at: at(0),
            expires_at: at(60),
            updated_at: at(if state == CandidateArtifactState::Complete {
                2
            } else {
                0
            }),
            version: AggregateVersion::new(if state == CandidateArtifactState::Complete {
                3
            } else {
                1
            }),
        }
    }

    fn recorded_candidate() -> RecordedCandidate {
        RecordedCandidate {
            project_id: id(1),
            package_id: id(2),
            revision_id: id(3),
            attempt_id: id(4),
            artifact_id: id(20),
            candidate_id: id(21),
            verification_run_id: id(22),
            candidate_commit: GitObjectId::new("2".repeat(40)).expect("candidate"),
            tree_hash: GitObjectId::new("3".repeat(40)).expect("tree"),
            branch: format!("refs/heads/agentforge/{}", id::<CandidateId>(21)),
            verification_state: VerificationRunState::Queued,
            sealed_at: at(3),
            candidate_version: AggregateVersion::new(1),
            verification_run_version: AggregateVersion::new(1),
            attempt_version: AggregateVersion::new(10),
            package_version: AggregateVersion::new(3),
            lease_version: AggregateVersion::new(2),
        }
    }

    async fn artifact_init_handler(
        Path((project, attempt)): Path<(String, String)>,
        headers: HeaderMap,
        Json(input): Json<InitCandidateArtifactInput>,
    ) -> (StatusCode, Json<CandidateArtifactView>) {
        assert_eq!(project, id::<ProjectId>(1).to_string());
        assert_eq!(attempt, id::<AttemptId>(4).to_string());
        assert_eq!(input.project_id, id(1));
        assert_eq!(input.attempt_id, id(4));
        assert_eq!(headers["idempotency-key"], "worker-artifact-init");
        assert_eq!(headers["if-match"], "\"2\"");
        (
            StatusCode::CREATED,
            Json(artifact_view(CandidateArtifactState::Uploading)),
        )
    }

    async fn artifact_chunk_handler(
        Path((project, artifact, chunk_index)): Path<(String, String, String)>,
        headers: HeaderMap,
        Json(input): Json<UploadCandidateArtifactChunkInput>,
    ) -> StatusCode {
        assert_eq!(project, id::<ProjectId>(1).to_string());
        assert_eq!(artifact, id::<CandidateArtifactId>(20).to_string());
        assert_eq!(chunk_index, "0");
        assert_eq!(input.content, b"candidate-bundle");
        assert_eq!(input.digest, Sha256Digest::of_bytes(&input.content));
        assert_eq!(headers["idempotency-key"], "worker-artifact-chunk-0");
        assert_eq!(headers["if-match"], "\"1\"");
        StatusCode::NO_CONTENT
    }

    async fn artifact_complete_handler(
        Path((project, artifact)): Path<(String, String)>,
        headers: HeaderMap,
        Json(input): Json<CompleteCandidateArtifactInput>,
    ) -> Json<CandidateArtifactView> {
        assert_eq!(project, id::<ProjectId>(1).to_string());
        assert_eq!(artifact, id::<CandidateArtifactId>(20).to_string());
        assert_eq!(input.artifact_id, id(20));
        assert_eq!(headers["idempotency-key"], "worker-artifact-complete");
        assert_eq!(headers["if-match"], "\"1\"");
        Json(artifact_view(CandidateArtifactState::Complete))
    }

    async fn candidate_record_handler(
        Path((project, attempt)): Path<(String, String)>,
        headers: HeaderMap,
        Json(input): Json<RecordCandidateInput>,
    ) -> (StatusCode, Json<RecordedCandidate>) {
        assert_eq!(project, id::<ProjectId>(1).to_string());
        assert_eq!(attempt, id::<AttemptId>(4).to_string());
        assert_eq!(input.project_id, id(1));
        assert_eq!(input.attempt_id, id(4));
        assert_eq!(input.artifact_id, id(20));
        assert_eq!(input.lease_id, id(5));
        assert_eq!(input.node_id, id(8));
        assert_eq!(
            input.fencing_token,
            FencingToken::new(1).expect("generation")
        );
        assert_eq!(
            input.branch,
            format!("refs/heads/agentforge/{}", id::<CandidateId>(21))
        );
        assert_eq!(headers["idempotency-key"], "worker-candidate-record");
        assert_eq!(headers["if-match"], "\"9\"");
        (StatusCode::CREATED, Json(recorded_candidate()))
    }

    async fn lease_error_handler() -> (StatusCode, Json<serde_json::Value>) {
        (
            StatusCode::CONFLICT,
            Json(serde_json::json!({
                "code": "AF_LEASE_STALE",
                "message": "the command conflicts with current durable state",
                "retryable": false
            })),
        )
    }

    async fn oversized_handler() -> Json<serde_json::Value> {
        Json(serde_json::json!({"padding": "x".repeat(4_096)}))
    }

    async fn slow_handler() -> Json<Vec<OfferView>> {
        tokio::time::sleep(Duration::from_millis(100)).await;
        Json(vec![offer()])
    }

    async fn spawn_server(router: Router) -> (SocketAddr, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("listener");
        let address = listener.local_addr().expect("address");
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.expect("serve fixture");
        });
        (address, task)
    }

    #[tokio::test]
    async fn adapter_matches_offer_claim_headers_paths_and_strict_json_contract() {
        let state = ApiState {
            captured: Arc::new(Mutex::new(None)),
        };
        let router = Router::new()
            .route("/api/v1/projects/{project}/offers", get(offers_handler))
            .route(
                "/api/v1/projects/{project}/packages/{package}/claim",
                post(claim_handler),
            )
            .route(
                "/api/v1/projects/{project}/attempts/{attempt}/progress",
                post(attempt_progress_handler),
            )
            .route(
                "/api/v1/projects/{project}/attempts/{attempt}/candidate-artifacts",
                post(artifact_init_handler),
            )
            .route(
                "/api/v1/projects/{project}/candidate-artifacts/{artifact}/chunks/{chunk_index}",
                put(artifact_chunk_handler),
            )
            .route(
                "/api/v1/projects/{project}/candidate-artifacts/{artifact}/complete",
                post(artifact_complete_handler),
            )
            .route(
                "/api/v1/projects/{project}/attempts/{attempt}/candidates",
                post(candidate_record_handler),
            )
            .with_state(state.clone());
        let (address, server) = spawn_server(router).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let adapter = LoopbackHttpControlPlane::from_config(&config(
            address,
            directory.path().join("worker.sqlite3"),
        ))
        .expect("adapter");

        assert_eq!(
            adapter
                .list_offers(ListOffersQuery {
                    project_id: id(1),
                    limit: 10,
                })
                .await
                .expect("offers"),
            vec![offer()]
        );
        let command = MvpCommand {
            context: MvpCommandContext {
                command_id: id::<CommandId>(10),
                actor_id: id(6),
                idempotency_key: IdempotencyKey::new("worker-http-claim").expect("key"),
                correlation_id: id::<CorrelationId>(11),
                causation_id: None,
                expected_version: Some(AggregateVersion::new(1)),
            },
            input: ClaimPackageInput {
                project_id: id(1),
                package_id: id(2),
                executor_id: id(7),
                node_id: id(8),
                lease_seconds: 60,
                max_lease_seconds: 600,
            },
        };
        assert_eq!(
            adapter.claim_package(&command).await.expect("claim"),
            claimed()
        );
        let (headers, input) = state
            .captured
            .lock()
            .expect("capture")
            .clone()
            .expect("captured request");
        assert_eq!(input, command.input);
        assert_eq!(headers["idempotency-key"], "worker-http-claim");
        assert_eq!(headers["if-match"], "\"1\"");
        assert_eq!(
            headers["x-agentforge-command-id"],
            command.context.command_id.to_string()
        );

        let progress = MvpCommand {
            context: MvpCommandContext {
                command_id: id(30),
                actor_id: id(6),
                idempotency_key: IdempotencyKey::new("worker-attempt-progress").expect("key"),
                correlation_id: id(31),
                causation_id: None,
                expected_version: Some(AggregateVersion::new(2)),
            },
            input: ReportAttemptProgressInput {
                project_id: id(1),
                attempt_id: id(4),
                lease_id: id(5),
                node_id: id(8),
                fencing_token: FencingToken::new(1).expect("generation"),
                stage: agentforge_application::AttemptProgressStage::Preparing,
                evidence_digest: Sha256Digest::of_bytes(b"preparation-evidence"),
            },
        };
        let progress_view = adapter
            .report_attempt_progress(&progress)
            .await
            .expect("Attempt progress");
        assert_eq!(
            progress_view.state,
            agentforge_domain::attempt::AttemptState::Preparing
        );
        assert_eq!(progress_view.version, AggregateVersion::new(4));

        let bundle = b"candidate-bundle".to_vec();
        let init = MvpCommand {
            context: MvpCommandContext {
                command_id: id(12),
                actor_id: id(6),
                idempotency_key: IdempotencyKey::new("worker-artifact-init").expect("key"),
                correlation_id: id(13),
                causation_id: None,
                expected_version: Some(AggregateVersion::new(2)),
            },
            input: InitCandidateArtifactInput {
                project_id: id(1),
                attempt_id: id(4),
                lease_id: id(5),
                node_id: id(8),
                fencing_token: FencingToken::new(1).expect("generation"),
                package_hash: execution().package_hash,
                base_commit: execution().base_commit,
                candidate_commit: GitObjectId::new("2".repeat(40)).expect("candidate"),
                tree_hash: GitObjectId::new("3".repeat(40)).expect("tree"),
                author_evidence_digest: Sha256Digest::of_bytes("evidence"),
                expected_bundle_digest: Sha256Digest::of_bytes(&bundle),
                expected_bundle_size_bytes: u64::try_from(bundle.len()).expect("bundle size"),
                chunk_digests: vec![Sha256Digest::of_bytes(&bundle)],
                upload_ttl_seconds: 60,
            },
        };
        let artifact = adapter
            .init_candidate_artifact(&init)
            .await
            .expect("artifact init");
        assert_eq!(artifact, artifact_view(CandidateArtifactState::Uploading));

        let chunk = MvpCommand {
            context: MvpCommandContext {
                command_id: id(14),
                actor_id: id(6),
                idempotency_key: IdempotencyKey::new("worker-artifact-chunk-0").expect("key"),
                correlation_id: id(13),
                causation_id: None,
                expected_version: Some(AggregateVersion::new(1)),
            },
            input: UploadCandidateArtifactChunkInput {
                project_id: id(1),
                artifact_id: artifact.artifact_id,
                lease_id: id(5),
                node_id: id(8),
                fencing_token: FencingToken::new(1).expect("generation"),
                chunk_index: 0,
                digest: Sha256Digest::of_bytes(&bundle),
                content: bundle,
            },
        };
        assert_eq!(
            adapter
                .upload_candidate_artifact_chunk(&chunk)
                .await
                .expect("artifact chunk"),
            CandidateArtifactChunkReceipt {
                artifact_id: id(20),
                chunk_index: 0,
                digest: chunk.input.digest,
                size_bytes: u32::try_from(chunk.input.content.len()).expect("chunk size"),
                artifact_version: AggregateVersion::new(1),
            }
        );

        let complete = MvpCommand {
            context: MvpCommandContext {
                command_id: id(15),
                actor_id: id(6),
                idempotency_key: IdempotencyKey::new("worker-artifact-complete").expect("key"),
                correlation_id: id(13),
                causation_id: None,
                expected_version: Some(AggregateVersion::new(1)),
            },
            input: CompleteCandidateArtifactInput {
                project_id: id(1),
                artifact_id: id(20),
                lease_id: id(5),
                node_id: id(8),
                fencing_token: FencingToken::new(1).expect("generation"),
                bundle_protocol_key: ProtocolKey::new("candidate-bundle").expect("key"),
                bundle_uri: format!(
                    "artifact://candidate-artifacts/{}",
                    id::<CandidateArtifactId>(20)
                ),
            },
        };
        assert_eq!(
            adapter
                .complete_candidate_artifact(&complete)
                .await
                .expect("artifact complete"),
            artifact_view(CandidateArtifactState::Complete)
        );

        let record = MvpCommand {
            context: MvpCommandContext {
                command_id: id(16),
                actor_id: id(6),
                idempotency_key: IdempotencyKey::new("worker-candidate-record").expect("key"),
                correlation_id: id(13),
                causation_id: None,
                expected_version: Some(AggregateVersion::new(9)),
            },
            input: RecordCandidateInput {
                project_id: id(1),
                attempt_id: id(4),
                artifact_id: id(20),
                lease_id: id(5),
                node_id: id(8),
                fencing_token: FencingToken::new(1).expect("generation"),
                branch: format!("refs/heads/agentforge/{}", id::<CandidateId>(21)),
            },
        };
        assert_eq!(
            adapter
                .record_candidate(&record)
                .await
                .expect("record Candidate"),
            recorded_candidate()
        );
        server.abort();
    }

    #[tokio::test]
    async fn adapter_preserves_known_remote_code_and_rejects_status_code_substitution() {
        let router = Router::new().route(
            "/api/v1/projects/{project}/leases/{lease}",
            get(lease_error_handler),
        );
        let (address, server) = spawn_server(router).await;
        let directory = tempfile::tempdir().expect("tempdir");
        let adapter = LoopbackHttpControlPlane::from_config(&config(
            address,
            directory.path().join("worker.sqlite3"),
        ))
        .expect("adapter");
        let error = adapter
            .get_lease(id(1), id(5))
            .await
            .expect_err("stale lease");
        assert_eq!(error.code(), "AF_LEASE_STALE");
        assert_eq!(
            decode_remote_error::<LeaseView>(
                500,
                br#"{"code":"AF_LEASE_STALE","message":"stale","retryable":false}"#,
            )
            .expect_err("status substitution")
            .code(),
            "AF_SERIALIZATION"
        );
        assert_eq!(
            decode_json::<serde_json::Value>(br#"{"code":1,"code":2}"#)
                .expect_err("duplicate key")
                .code(),
            "AF_SERIALIZATION"
        );
        server.abort();
    }

    #[tokio::test]
    async fn adapter_bounds_response_bytes_and_the_whole_exchange_timeout() {
        let oversized =
            Router::new().route("/api/v1/projects/{project}/offers", get(oversized_handler));
        let (address, oversized_server) = spawn_server(oversized).await;
        let adapter = LoopbackHttpControlPlane {
            address,
            timeout: Duration::from_secs(1),
            max_response_bytes: 1_024,
        };
        assert_eq!(
            adapter
                .list_offers(ListOffersQuery {
                    project_id: id(1),
                    limit: 10,
                })
                .await
                .expect_err("oversized response")
                .code(),
            "AF_SERIALIZATION"
        );
        oversized_server.abort();

        let slow = Router::new().route("/api/v1/projects/{project}/offers", get(slow_handler));
        let (address, slow_server) = spawn_server(slow).await;
        let adapter = LoopbackHttpControlPlane {
            address,
            timeout: Duration::from_millis(10),
            max_response_bytes: 1_024,
        };
        assert_eq!(
            adapter
                .list_offers(ListOffersQuery {
                    project_id: id(1),
                    limit: 10,
                })
                .await
                .expect_err("request timeout")
                .code(),
            "AF_UNAVAILABLE"
        );
        slow_server.abort();
    }

    #[test]
    fn remote_error_mapping_covers_every_accepted_code_and_retryability() {
        let errors = [
            MvpRemoteError::ResourceIdInvalid,
            MvpRemoteError::CommandHeaderInvalid,
            MvpRemoteError::PathBodyMismatch,
            MvpRemoteError::ProjectAccessDenied,
            MvpRemoteError::CommandServiceUnavailable,
            MvpRemoteError::NotFound,
            MvpRemoteError::LeaseExpired,
            MvpRemoteError::PolicyDenied,
            MvpRemoteError::Conflict,
            MvpRemoteError::StaleVersion,
            MvpRemoteError::ArgumentInvalid,
            MvpRemoteError::LeaseStale,
            MvpRemoteError::IdempotencyKeyReused,
            MvpRemoteError::TransitionInvalid,
            MvpRemoteError::PackageNotClaimable,
            MvpRemoteError::PackageHashMismatch,
            MvpRemoteError::EvidenceInvalid,
            MvpRemoteError::CandidateArtifactNotComplete,
            MvpRemoteError::Unavailable,
            MvpRemoteError::StorageIntegrity,
            MvpRemoteError::Serialization,
            MvpRemoteError::IdempotencyResultExpired,
            MvpRemoteError::IdempotencyResultLegacy,
        ];
        for error in errors {
            assert_eq!(MvpRemoteError::parse(error.code()), Some(error));
            assert!((400..=599).contains(&remote_status(error)));
        }
        assert!(MvpRemoteError::CommandServiceUnavailable.retryable());
        assert!(MvpRemoteError::PackageNotClaimable.retryable());
        assert!(MvpRemoteError::CandidateArtifactNotComplete.retryable());
        assert!(!MvpRemoteError::LeaseStale.retryable());
        assert!(MvpRemoteError::parse("AF_FUTURE_SERVER_CODE").is_none());
    }
}
