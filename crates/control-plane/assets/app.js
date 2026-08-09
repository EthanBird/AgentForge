const state = {
  projectId: new URL(location.href).searchParams.get("project") || localStorage.getItem("agentforge.project") || "",
  view: "mission",
  data: null,
  source: null,
  refreshTimer: null,
  refreshController: null,
  projectEpoch: 1,
  refreshGeneration: 0,
  selectedDecisionId: null,
  sseFailures: 0,
  etag: null,
  cursor: "",
  snapshotCursor: "",
  projectionDegraded: false,
  projectionStale: false,
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

function element(tag, attributes = {}, children = []) {
  const node = document.createElement(tag);
  for (const [key, value] of Object.entries(attributes)) {
    if (key === "class") node.className = value;
    else if (key.startsWith("data-")) node.setAttribute(key, String(value));
    else if (key === "title") node.title = String(value);
  }
  for (const child of Array.isArray(children) ? children : [children]) {
    node.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return node;
}

function projected(value, fallback = "—") {
  if (typeof value === "string") return value;
  return value?.display || fallback;
}

function entries(value) {
  if (!value) return [];
  if (Array.isArray(value)) return value;
  return Object.values(value);
}

function setConnection(kind, label) {
  const connection = $("#connection");
  connection.dataset.state = kind;
  $("span", connection).textContent = label;
}

function cursorKey(projectId) {
  return `agentforge.cursor.${projectId}`;
}

function controlRoomStreamUrl(projectId, cursor) {
  const query = new URLSearchParams({ cursor });
  return `/v1/projects/${encodeURIComponent(projectId)}/control-room-stream?${query}`;
}

function isCurrentProject(projectId, projectEpoch) {
  return state.projectId === projectId && state.projectEpoch === projectEpoch;
}

function isCurrentRefresh(projectId, projectEpoch, refreshGeneration) {
  return isCurrentProject(projectId, projectEpoch) && state.refreshGeneration === refreshGeneration;
}

function setLiveConnection(label) {
  const unhealthy = state.projectionDegraded || state.projectionStale;
  const healthLabel = state.projectionDegraded ? "投影降级" : state.projectionStale ? "投影滞后" : label;
  setConnection(unhealthy ? "degraded" : "online", healthLabel);
}

function showNotice(message) {
  const notice = $("#notice");
  notice.textContent = message;
  notice.hidden = !message;
}

function empty(message) {
  return element("div", { class: "empty" }, message);
}

function pill(label, tone = "") {
  return element("span", { class: "pill", "data-tone": tone }, label || "unknown");
}

function toneFor(value = "") {
  const normalized = String(value).toLowerCase();
  if (/critical|failed|lost|quarantined|expired|cancelled/.test(normalized)) return "danger";
  if (/risk|waiting|blocked|degraded|stale|partial|timed/.test(normalized)) return "warn";
  if (/active|running|available|passed|integrated|applied|online/.test(normalized)) return "good";
  return "";
}

function formatInstant(value) {
  if (!value) return "尚无水位";
  const instant = new Date(value);
  return Number.isNaN(instant.getTime()) ? String(value) : instant.toLocaleString();
}

function formatStaleness(value) {
  const milliseconds = Number(value);
  if (!Number.isFinite(milliseconds) || milliseconds < 0) return "未知延迟";
  if (milliseconds < 1_000) return `${milliseconds} ms`;
  if (milliseconds < 60_000) return `${(milliseconds / 1_000).toFixed(1)} s`;
  return `${(milliseconds / 60_000).toFixed(1)} min`;
}

function renderProjectionHealth(data) {
  const header = data?.project_control_room?.header;
  const host = $("#projection-health");
  if (!header) {
    state.projectionDegraded = false;
    state.projectionStale = false;
    host.dataset.state = "empty";
    $("#projection-health-label").textContent = "尚未同步";
    $("#projection-as-of").textContent = "as_of —";
    $("#projection-staleness").textContent = "staleness —";
    $("#projection-degraded").textContent = "degraded_reason —";
    $("#projection-version").textContent = "尚未同步";
    return;
  }

  const degradedReason = header.degraded_reason || "";
  const staleness = Number(header.staleness_ms);
  state.projectionDegraded = Boolean(degradedReason);
  state.projectionStale = Number.isFinite(staleness) && staleness > 0;
  host.dataset.state = state.projectionDegraded ? "degraded" : state.projectionStale ? "stale" : "healthy";
  $("#projection-health-label").textContent = state.projectionDegraded
    ? "投影降级"
    : state.projectionStale
      ? "投影滞后"
      : "投影健康";
  $("#projection-as-of").textContent = `as_of ${formatInstant(header.as_of)}`;
  $("#projection-staleness").textContent = `staleness ${formatStaleness(header.staleness_ms)}`;
  $("#projection-degraded").textContent = `degraded_reason ${degradedReason || "none"}`;
  $("#projection-version").textContent = `projection v${header.projection_version ?? "—"}`;
}

async function requestJson(path, { etag, signal }) {
  const headers = { Accept: "application/json" };
  if (etag) headers["If-None-Match"] = etag;
  const response = await fetch(path, { headers, credentials: "same-origin", signal });
  const streamCursor = response.headers.get("X-AgentForge-Stream-Cursor") || "";
  if (response.status === 304) return { data: null, etag, streamCursor };
  if (!response.ok) {
    const error = new Error(`${response.status} ${response.statusText}`);
    error.status = response.status;
    throw error;
  }
  return { data: await response.json(), etag: response.headers.get("ETag"), streamCursor };
}

async function refresh() {
  const projectId = state.projectId;
  const projectEpoch = state.projectEpoch;
  if (!projectId) {
    showNotice("请输入 Project UUID。Control Room 不会展示跨项目汇总或示例数据。");
    state.data = null;
    render();
    return;
  }
  state.refreshController?.abort();
  const controller = new AbortController();
  state.refreshController = controller;
  const refreshGeneration = ++state.refreshGeneration;
  $("#refresh").disabled = true;
  try {
    const encoded = encodeURIComponent(projectId);
    const result = await requestJson(`/v1/projects/${encoded}/control-room`, {
      etag: state.etag,
      signal: controller.signal,
    });
    if (!isCurrentRefresh(projectId, projectEpoch, refreshGeneration)) return;
    if (!result.streamCursor) throw new Error("快照响应缺少精确 SSE 交接 cursor");
    if (result.data) state.data = result.data;
    state.etag = result.etag || state.etag;
    state.snapshotCursor = result.streamCursor;
    state.cursor = result.streamCursor;
    localStorage.setItem(cursorKey(projectId), result.streamCursor);
    showNotice("");
    // A 304 still carries a new exact stream handoff cursor, but its DOM is
    // already current. Avoid replacing rows and stealing keyboard focus.
    if (result.data) render();
    setLiveConnection("已同步");
    connectEvents({ projectId, projectEpoch, refreshGeneration });
  } catch (error) {
    if (error.name === "AbortError" || !isCurrentRefresh(projectId, projectEpoch, refreshGeneration)) return;
    setConnection("offline", "读取失败");
    showNotice(`无法读取项目投影：${error.message}。已保留上次成功快照。`);
  } finally {
    if (isCurrentRefresh(projectId, projectEpoch, refreshGeneration)) {
      state.refreshController = null;
      $("#refresh").disabled = false;
    }
  }
}

function connectEvents({ projectId, projectEpoch, refreshGeneration }) {
  if (!isCurrentRefresh(projectId, projectEpoch, refreshGeneration)) return;
  state.source?.close();
  const projectCursorKey = cursorKey(projectId);
  if (!state.cursor) {
    scheduleRefresh(0, { projectId, projectEpoch });
    return;
  }
  const streamUrl = controlRoomStreamUrl(projectId, state.cursor);
  const source = new EventSource(streamUrl, { withCredentials: true });
  const sourceIsCurrent = () => isCurrentProject(projectId, projectEpoch) && state.source === source;
  source.onopen = () => {
    if (!sourceIsCurrent()) return source.close();
    state.sseFailures = 0;
    setLiveConnection("实时");
  };
  source.onerror = () => {
    if (!sourceIsCurrent()) return source.close();
    setConnection("connecting", "重连中");
    source.close();
    if (state.source === source) state.source = null;
    state.sseFailures += 1;
    inspectStreamFailure(controlRoomStreamUrl(projectId, state.cursor), { projectId, projectEpoch });
  };
  source.addEventListener("projection.invalidated", (event) => {
    if (!sourceIsCurrent()) return source.close();
    if (event.lastEventId) {
      state.cursor = event.lastEventId;
      localStorage.setItem(projectCursorKey, event.lastEventId);
    }
    scheduleRefresh(180, { projectId, projectEpoch });
  });
  source.addEventListener("projection.reset", () => {
    if (!sourceIsCurrent()) return source.close();
    source.close();
    if (state.source === source) state.source = null;
    recoverFromSnapshotCursor({ projectId, projectEpoch });
  });
  state.source = source;
}

async function inspectStreamFailure(streamUrl, context) {
  try {
    const response = await fetch(streamUrl, {
      method: "HEAD",
      headers: { Accept: "text/event-stream" },
      credentials: "same-origin",
      cache: "no-store",
    });
    if (!isCurrentProject(context.projectId, context.projectEpoch)) return;
    if (response.status === 409) {
      recoverFromSnapshotCursor(context);
      return;
    }
    if (response.status === 400 || response.status === 401 || response.status === 403) {
      setConnection("offline", "授权或 Cursor 已拒绝");
      showNotice(`实时流恢复被服务端拒绝（HTTP ${response.status}），不会丢弃现有 cursor。`);
      return;
    }
  } catch (_error) {
    // A failed probe is another transport failure. Preserve the durable cursor.
  }
  scheduleReconnect(
    Math.min(30_000, 1_000 * (2 ** Math.min(state.sseFailures, 5))),
    context,
  );
}

function recoverFromSnapshotCursor(context) {
  if (!isCurrentProject(context.projectId, context.projectEpoch)) return;
  state.snapshotCursor = "";
  state.cursor = "";
  localStorage.removeItem(cursorKey(context.projectId));
  scheduleRefresh(0, context);
}

function scheduleReconnect(delay, context) {
  clearTimeout(state.refreshTimer);
  state.refreshTimer = setTimeout(() => {
    if (document.visibilityState !== "visible" || !isCurrentProject(context.projectId, context.projectEpoch)) return;
    connectEvents({
      projectId: context.projectId,
      projectEpoch: context.projectEpoch,
      refreshGeneration: state.refreshGeneration,
    });
  }, delay);
}

function scheduleRefresh(delay = 0, context = { projectId: state.projectId, projectEpoch: state.projectEpoch }) {
  clearTimeout(state.refreshTimer);
  state.refreshTimer = setTimeout(() => {
    if (document.visibilityState === "visible" && isCurrentProject(context.projectId, context.projectEpoch)) refresh();
  }, delay);
}

function renderMetrics(data) {
  const counters = data?.project_control_room?.counters || data?.counters || {};
  const riskCount = counters.at_risk_packages ?? "—";
  const metrics = [
    ["待决策", counters.needs_decision ?? 0, "需要操作者", "danger"],
    ["风险任务", riskCount, riskCount === "—" ? "投影未提供风险计数" : "服务端策略命中", "warn"],
    ["运行中", counters.active_runs ?? 0, "有效 RunClaim", "good"],
    ["缺少推进者", counters.packages_without_action_path ?? 0, "必须有活性路径", "warn"],
    ["预算风险", counters.open_budget_incidents ?? 0, "预留与实际", "danger"],
  ];
  const grid = $("#metric-grid");
  grid.replaceChildren(...metrics.map(([name, value, note, tone]) =>
    element("article", { class: "metric", "data-tone": tone }, [
      element("span", {}, name), element("strong", {}, value), element("small", {}, note),
    ])));
  $("#nav-decision-count").textContent = String(counters.needs_decision ?? 0);
}

function packageRow(item) {
  const status = item.state || "unknown";
  const next = projected(item.action_path?.reason, describeDriver(item.action_path?.driver));
  return element("div", { class: "list-row" }, [
    element("div", {}, [element("h3", {}, projected(item.summary, item.protocol_key || item.package_id)), element("p", {}, next)]),
    element("div", { class: "row-meta" }, pill(status, toneFor(status))),
  ]);
}

function describeDriver(driver) {
  if (!driver) return "等待投影提供下一推进路径";
  const labels = {
    active_run: "InvocationRun 正在推进",
    queued_intent: "InvocationIntent 等待调度",
    governance: "等待治理决策",
    monitor: "等待可验证外部事实",
    human: "等待人工输入",
    blocker: "等待前置阻塞解除",
    recovery: "等待恢复流程",
  };
  return labels[driver.kind] || driver.kind || "已有明确推进路径";
}

function decisionRow(item, interactive = false) {
  const risk = item.risk || item.severity || "normal";
  const caseId = stableId(item.case_id);
  const row = element("div", {
    class: "list-row",
    "data-case-id": caseId,
  }, [
    element("div", {}, [element("h3", {}, projected(item.summary, item.kind || "治理事件")), element("p", {}, projected(item.why_now, "需要操作者决策"))]),
    element("div", { class: "row-meta" }, pill(risk, toneFor(risk))),
  ]);
  if (interactive) {
    row.tabIndex = 0;
    row.setAttribute("role", "button");
    row.addEventListener("click", () => selectDecision(caseId));
    row.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        if (event.key === " ") event.preventDefault();
        selectDecision(caseId);
      }
    });
  }
  return row;
}

function runRow(item) {
  const status = item.status || item.state || "unknown";
  return element("div", { class: "list-row" }, [
    element("div", {}, [element("h3", {}, projected(item.summary, item.run_id)), element("p", {}, `${projected(item.model, "model")} · ${projected(item.adapter, "adapter")}`)]),
    element("div", { class: "row-meta" }, pill(status, toneFor(status))),
  ]);
}

function renderMission(data) {
  const room = data?.project_control_room || data || {};
  const packages = entries(room.packages).slice(0, 6);
  const decisions = entries(data?.governance_inbox?.cases).filter((item) => /needs_decision|deferred/i.test(item.state || item.status || "")).slice(0, 4);
  const runs = entries(data?.runs?.runs).filter((item) => /reserved|starting|running|reconciling/i.test(item.status || item.state || "")).slice(0, 4);
  const agents = entries(data?.fleet?.agents).slice(0, 4);
  $("#package-list").replaceChildren(...(packages.length ? packages.map(packageRow) : [empty("当前没有任务包投影") ]));
  $("#decision-preview").replaceChildren(...(decisions.length ? decisions.map((item) => decisionRow(item)) : [empty("当前没有待决策事项") ]));
  $("#run-preview").replaceChildren(...(runs.length ? runs.map(runRow) : [empty("当前没有活跃 InvocationRun") ]));
  $("#action-paths").replaceChildren(...(packages.length ? packages.filter((item) => item.action_path || item.next_driver).slice(0, 4).map(packageRow) : [empty("尚无推进路径投影") ]));
  $("#fleet-preview").replaceChildren(...(agents.length ? agents.map((item) => runRow({ ...item, run_id: item.executor_id, status: item.availability, summary: item.name })) : [empty("当前没有 Executor 投影") ]));
  renderBudget(data?.budget);
}

function renderBudget(budget) {
  const envelopes = entries(budget?.envelopes).slice(0, 3);
  const host = $("#budget-summary");
  if (!envelopes.length) return host.replaceChildren(empty("尚无预算预留"));
  host.replaceChildren(...envelopes.map((entry) => {
    const usage = entries(entry.usage)[0] || { limit: 0, reserved: 0, consumed: 0 };
    const total = Number(usage.limit || 0);
    const used = Number(usage.reserved || 0) + Number(usage.consumed || 0);
    const percent = total > 0 ? Math.min(100, Math.round((used / total) * 100)) : 0;
    const meter = element("div", { class: "meter" }, element("i"));
    $("i", meter).style.width = `${percent}%`;
    return element("div", { class: "budget-line" }, [element("header", {}, [element("span", {}, String(entry.scope?.kind || "scope")), element("b", {}, `${percent}%`)]), meter]);
  }));
}

function renderTable(host, columns, rows) {
  if (!rows.length) return host.replaceChildren(empty("没有可显示的投影记录"));
  host.replaceChildren(buildTable(columns, rows));
}

function buildTable(columns, rows) {
  const head = element("tr", {}, columns.map(([label]) => element("th", {}, label)));
  const body = rows.map((row) => element("tr", {}, columns.map(([, read]) => element("td", {}, read(row)))));
  return element("table", { class: "data-table" }, [element("thead", {}, head), element("tbody", {}, body)]);
}

function stableId(value) {
  if (value === null || value === undefined) return "—";
  if (typeof value === "string" || typeof value === "number") return String(value);
  return JSON.stringify(value);
}

function renderWorkGraph(data) {
  const graph = data?.work_graph || {};
  const nodes = entries(graph.nodes);
  const edges = entries(graph.edges);
  const criticalPath = Array.isArray(graph.critical_path) ? graph.critical_path : [];
  const criticalPosition = new Map(criticalPath.map((packageId, index) => [stableId(packageId), index + 1]));
  const host = $("#work-table");
  const critical = element("section", { class: "graph-facts", "aria-label": "关键路径" }, [
    element("h2", {}, "Critical path"),
    criticalPath.length
      ? element("ol", { class: "critical-path" }, criticalPath.map((packageId) => element("li", {}, stableId(packageId))))
      : empty("投影未提供关键路径"),
  ]);

  const nodeColumns = [
    ["Package", (item) => stableId(item.package_id)],
    ["状态", (item) => item.state || "—"],
    ["依赖", (item) => item.dependency_state || "—"],
    ["Blocker", (item) => projected(item.blocker_summary, item.blocker_code || "—")],
    ["关键路径", (item) => criticalPosition.has(stableId(item.package_id)) ? `#${criticalPosition.get(stableId(item.package_id))} · criticality ${item.criticality ?? "—"}` : `否 · criticality ${item.criticality ?? "—"}`],
    ["交付链", (item) => `Attempt ${stableId(item.attempt_id)} · Candidate ${stableId(item.candidate_id)} · Integration ${stableId(item.integration_id)}`],
  ];
  const edgeColumns = [
    ["From", (item) => stableId(item.from_package_id)],
    ["关系", (item) => item.relation || "—"],
    ["To", (item) => stableId(item.to_package_id)],
  ];
  const nodesSection = element("section", { class: "graph-facts", "aria-label": "任务节点" }, [
    element("h2", {}, `Nodes (${nodes.length})`),
    nodes.length ? buildTable(nodeColumns, nodes) : empty("当前没有 WorkGraph 节点投影"),
  ]);
  const edgesSection = element("section", { class: "graph-facts", "aria-label": "任务边" }, [
    element("h2", {}, `Edges (${edges.length})`),
    edges.length ? buildTable(edgeColumns, edges) : empty("当前没有 WorkGraph 边投影"),
  ]);
  host.replaceChildren(critical, nodesSection, edgesSection);
}

function renderViews(data) {
  renderWorkGraph(data);
  const decisions = entries(data?.governance_inbox?.cases);
  $("#decision-list").replaceChildren(...(decisions.length ? decisions.map((item) => decisionRow(item, true)) : [empty("当前没有治理事件") ]));
  renderTable($("#run-table"), [["Run", (x) => x.run_id], ["状态", (x) => x.status || x.state], ["模型", (x) => projected(x.model)], ["摘要", (x) => projected(x.summary)], ["更新", (x) => String(x.updated_at || "—")]], entries(data?.runs?.runs));
  renderTable($("#fleet-table"), [["Executor", (x) => projected(x.name, x.executor_id)], ["可用性", (x) => x.availability], ["模型", (x) => projected(x.model)], ["节点", (x) => x.node_id || "—"], ["能力", (x) => [...(x.capabilities || [])].join(", ")]], entries(data?.fleet?.agents));
  renderTable($("#lineage-list"), [["实体", (x) => JSON.stringify(x.id)], ["标签", (x) => projected(x.label)], ["状态", (x) => x.status], ["Evidence", (x) => x.evidence_digest || "—"]], entries(data?.lineage?.nodes));
  renderTable($("#activity-list"), [["时间", (x) => String(x.occurred_at || "—")], ["动作", (x) => x.typed_action || "—"], ["主体", (x) => JSON.stringify(x.subject)], ["结果", (x) => x.result || "—"], ["摘要", (x) => projected(x.summary)], ["Correlation", (x) => x.correlation_id || "—"]], entries(data?.activity?.items).sort((a, b) => String(b.occurred_at).localeCompare(String(a.occurred_at))));
}

function renderDecisionInspector(item) {
  const inspector = $("#decision-inspector");
  if (!item) {
    inspector.replaceChildren(
      element("p", { class: "eyebrow" }, "IMPACT PREVIEW"),
      element("h2", {}, "选择一项决策"),
      element("p", {}, "从决策列表查看当前快照中的精确目标与影响。"),
    );
    return;
  }
  const effects = (item.effect_preview || []).map((value) => projected(value)).join("；") || "无可执行 effect preview";
  inspector.replaceChildren(
    element("p", { class: "eyebrow" }, "IMPACT PREVIEW"),
    element("h2", {}, projected(item.summary, item.kind || "治理事件")),
    element("dl", {}, [
      element("dt", {}, "为什么现在"), element("dd", {}, projected(item.why_now)),
      element("dt", {}, "目标版本"), element("dd", {}, item.target_version ?? "—"),
      element("dt", {}, "Action digest"), element("dd", {}, item.action_digest || "—"),
      element("dt", {}, "影响"), element("dd", {}, effects),
      element("dt", {}, "截止"), element("dd", {}, String(item.decide_by || "无")),
    ]),
    element("p", { class: "notice" }, "当前检查点仅开放只读影响预览；批准/拒绝将在 typed command、CAS 与幂等回执完成后启用。"),
  );
}

function rebindSelectedDecision() {
  const decisions = entries(state.data?.governance_inbox?.cases);
  const selected = state.selectedDecisionId
    ? decisions.find((item) => stableId(item.case_id) === state.selectedDecisionId)
    : null;
  if (state.selectedDecisionId && !selected) state.selectedDecisionId = null;
  $$("#decision-list [data-case-id]").forEach((row) => {
    const active = Boolean(selected) && row.dataset.caseId === state.selectedDecisionId;
    row.classList.toggle("is-selected", active);
    row.setAttribute("aria-pressed", String(active));
  });
  renderDecisionInspector(selected);
}

function selectDecision(caseId) {
  state.selectedDecisionId = caseId;
  rebindSelectedDecision();
  if (matchMedia("(max-width: 720px)").matches) {
    $("#decision-inspector").scrollIntoView({ behavior: "smooth", block: "nearest" });
  }
}

function render() {
  const data = state.data;
  renderProjectionHealth(data);
  renderMetrics(data);
  renderMission(data);
  renderViews(data);
  rebindSelectedDecision();
}

function switchView(view, moveFocus = true) {
  state.view = view;
  $$('[data-view-panel]').forEach((panel) => panel.classList.toggle("is-active", panel.dataset.viewPanel === view));
  $$('[data-view]').forEach((button) => {
    const active = button.dataset.view === view;
    button.classList.toggle("is-active", active);
    if (active) button.setAttribute("aria-current", "page");
    else button.removeAttribute("aria-current");
  });
  const titles = { mission: ["项目总控台", "所有数字均来自可重建投影，不采用 Agent 自报状态。"], work: ["任务图与推进路径", "任务包、阻塞关系与下一驱动事实。"], decide: ["治理决策台", "先看证据和影响，再执行精确 typed command。"], runs: ["Invocation Runs", "Attempt 内的调用窗口、预算和恢复轨迹。"], fleet: ["Agent 舰队", "责任层级与能力路由是两套正交视图。"], lineage: ["Evidence & Lineage", "从需求到集成提交的可验证链路。"], activity: ["活动与审计", "只显示安全摘要、typed action 与 correlation，不暴露 Prompt 或 Secret。"] };
  const [title, subtitle] = titles[view] || titles.mission;
  $("#view-title").textContent = title;
  $("#view-subtitle").textContent = subtitle;
  $("#more-switcher").hidden = !["fleet", "lineage", "activity"].includes(view);
  if (moveFocus) $("#workspace").focus({ preventScroll: true });
}

function switchProject(projectId) {
  state.projectEpoch += 1;
  state.refreshGeneration += 1;
  state.refreshController?.abort();
  state.refreshController = null;
  state.source?.close();
  state.source = null;
  clearTimeout(state.refreshTimer);
  state.refreshTimer = null;
  state.projectId = projectId;
  state.data = null;
  state.selectedDecisionId = null;
  state.etag = null;
  state.cursor = projectId ? localStorage.getItem(cursorKey(projectId)) || "" : "";
  state.snapshotCursor = "";
  state.sseFailures = 0;
  state.projectionDegraded = false;
  state.projectionStale = false;
  $("#refresh").disabled = false;
  setConnection(projectId ? "connecting" : "offline", projectId ? "连接中" : "未选择项目");
  showNotice("");
  render();

  if (projectId) localStorage.setItem("agentforge.project", projectId);
  else localStorage.removeItem("agentforge.project");
  const url = new URL(location.href);
  projectId ? url.searchParams.set("project", projectId) : url.searchParams.delete("project");
  history.replaceState(null, "", url);
  refresh();
}

function initialize() {
  state.cursor = state.projectId ? localStorage.getItem(cursorKey(state.projectId)) || "" : "";
  $("#project-id").value = state.projectId;
  $("#project-id").addEventListener("change", (event) => {
    switchProject(event.target.value.trim());
  });
  $("#refresh").addEventListener("click", refresh);
  $$('[data-view]').forEach((button) => button.addEventListener("click", () => switchView(button.dataset.view)));
  $$('[data-go]').forEach((button) => button.addEventListener("click", () => switchView(button.dataset.go)));
  $("#open-command").addEventListener("click", () => $("#command-dialog").showModal());
  document.addEventListener("visibilitychange", () => {
    if (document.visibilityState === "visible") refresh();
  });
  setInterval(() => {
    if (document.visibilityState === "visible") refresh();
  }, 45_000);
  switchView(state.view, false);
  render();
  refresh();
}

initialize();
