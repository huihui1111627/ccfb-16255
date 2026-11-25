// NagleTrace web UI: renders the two-lane packet timeline, highlights
// diagnosed wait intervals and shows the verdict panel.
const $ = (id) => document.getElementById(id);

const KIND_COLOR = {
  data: "data", ack: "ack", syn: "syn", fin: "fin", retransmit: "rex",
};

function fmtDur(ns) {
  // Go time.Duration arrives as integer nanoseconds.
  if (ns === 0) return "0";
  const us = ns / 1000;
  if (us < 1000) return us.toFixed(us < 10 ? 1 : 0) + "µs";
  const ms = us / 1000;
  if (ms < 1000) return ms.toFixed(ms < 10 ? 2 : 1) + "ms";
  return (ms / 1000).toFixed(2) + "s";
}

function flagsOf(ev) {
  const f = ev.flags || 0;
  const S = 1 << 1, A = 1 << 4, F = 1 << 0, R = 1 << 3, P = 1 << 5;
  let s = "";
  if (f & S) s += "S";
  if (f & A) s += "A";
  if (f & F) s += "F";
  if (f & R) s += "R";
  if (f & P) s += "P";
  return s || ".";
}

function pktClass(ev) {
  if (String(ev.detail || "").includes("retransmit")) return "retransmit";
  const f = ev.flags || 0;
  if (f & (1 << 1)) return "syn";
  if (f & (1 << 0)) return "fin";
  if (ev.payloadLen > 0) return "data";
  return "ack";
}

async function loadPresets() {
  const presets = await (await fetch("/api/presets")).json();
  $("preset").innerHTML = presets.map(p =>
    `<option value="${p.id}">${p.id} — ${p.name}</option>`).join("");
}

async function run() {
  const id = $("preset").value;
  const data = await (await fetch("/api/run?preset=" + encodeURIComponent(id))).json();
  $("scenarioName").textContent = data.scenario;
  renderStats(data);
  renderVerdict(data);
  renderWaits(data);
  renderTimeline(data);
  $("matrixCard").style.display = "none";
}

function renderStats(data) {
  const s = data.summary;
  const tailPct = s.count ? (100 * s.tailFraction).toFixed(1) : "0";
  const items = [
    ["样本数", s.count],
    ["P50", fmtDur(s.p50)],
    ["P90", fmtDur(s.p90)],
    ["P99", fmtDur(s.p99)],
    ["最大", fmtDur(s.max)],
    ["长尾占比", tailPct + "%"],
    ["丢包", data.dropped],
  ];
  $("stats").innerHTML = items.map(([k, v]) =>
    `<div class="stat"><div class="v">${v}</div><div class="k">${k}</div></div>`).join("");
}

function sevClass(sev) { return sev || "info"; }

function renderVerdict(data) {
  const d = data.diagnosis;
  const sug = (d.suggestions || []).map(s => `<li>${s}</li>`).join("");
  const cross = (d.crossCheck || []).map(s =>
    `<div class="crosscheck">交叉校验：${s}</div>`).join("");
  $("verdict").innerHTML = `
    <div style="margin-bottom:10px">
      <span class="badge ${sevClass(d.severity)}">${d.severity}</span>
      <strong style="margin-left:8px">${d.primaryType}</strong>
    </div>
    <p>${d.summary}</p>
    <p class="muted"><strong>根因：</strong>${d.rootCause}</p>
    ${sug ? `<ul>${sug}</ul>` : ""}
    ${cross}`;
}

function renderWaits(data) {
  const ws = data.diagnosis.waits || [];
  if (!ws.length) {
    $("waits").innerHTML = `<p class="muted">未检测到传输层互等。</p>`;
    return;
  }
  $("waits").innerHTML = ws.map(w => {
    const sev = w.type === "nagle-delayed-ack" || w.type === "rto-retransmit" ? ""
      : (w.type === "fast-retransmit" || w.type === "cork-hold" || w.type === "window-full") ? "warning" : "info";
    const ev = (w.evidence || []).map(e => `<li>${e}</li>`).join("");
    return `<div class="wait ${sev}">
      <div class="top"><span>${w.type}</span><span>${fmtDur(w.duration)}</span></div>
      <div class="meta">${w.who} 在 ${fmtDur(w.start)} → ${fmtDur(w.end)} 等待</div>
      <div class="meta">等什么：${w.waitingFor}</div>
      ${w.unblockPacket ? `<div class="meta">被谁解锁：${w.unblockPacket}</div>` : ""}
      <div class="meta">设置来源：${w.causedBy}</div>
      <ul>${ev}</ul>
      <div class="fix">建议：${w.suggestion}</div>
    </div>`;
  }).join("");
}

function renderTimeline(data) {
  const packets = data.trace.filter(e => e.kind === "packet");
  const waits = data.diagnosis.waits || [];
  if (!packets.length) { $("timeline").innerHTML = ""; return; }

  const W = 1180, laneTop = 70, laneGap = 240, H = laneTop + laneGap + 60;
  const padL = 90, padR = 40;
  const t0 = packets[0].time;
  const t1 = packets[packets.length - 1].time;
  const span = Math.max(t1 - t0, 1);
  const x = (t) => padL + (t - t0) / span * (W - padL - padR);
  const laneY = { client: laneTop, server: laneTop + laneGap };

  // tick marks
  const ticks = [];
  const N = 8;
  for (let i = 0; i <= N; i++) {
    const t = t0 + span * i / N;
    ticks.push(`<line class="tick" x1="${x(t)}" y1="${laneTop - 24}" x2="${x(t)}" y2="${laneTop + laneGap + 24}"/>
      <text class="tick-label" x="${x(t)}" y="${laneTop - 30}" text-anchor="middle">${fmtDur(t)}</text>`);
  }

  // stall bands (from diagnosis)
  const bands = waits.filter(w =>
    w.type === "nagle-delayed-ack" || w.type === "delayed-ack-only" ||
    w.type === "rto-retransmit" || w.type === "fast-retransmit").map(w => {
    const cls = w.type === "nagle-delayed-ack" || w.type === "rto-retransmit" ? "stall-band" : "stall-band delack";
    const x1 = x(w.start), x2 = x(Math.max(w.end, w.start + span * 0.004));
    return `<rect class="${cls}" x="${x1}" y="${laneTop - 18}" width="${Math.max(x2 - x1, 3)}" height="${laneGap + 36}" rx="4"/>
      <text class="pkt-label" x="${(x1 + x2) / 2}" y="${laneTop - 22}" text-anchor="middle">${w.type} ${fmtDur(w.duration)}</text>`;
  }).join("");

  // lanes
  const lanes = Object.entries(laneY).map(([name, y]) => `
    <line x1="${padL - 20}" y1="${y}" x2="${W - padR + 10}" y2="${y}" stroke="#33415f" stroke-width="2"/>
    <circle cx="${padL - 34}" cy="${y}" r="7" fill="${name === "client" ? "var(--client)" : "var(--server)"}"/>
    <text class="lane-label" x="${padL - 34}" y="${y - 14}" text-anchor="middle" fill="${name === "client" ? "var(--client)" : "var(--server)"}">${name}</text>
  `).join("");

  // arrows: send at (x, sender lane), arrowhead toward receiver lane.
  const arrows = packets.map(ev => {
    const fromClient = ev.endpoint === "client" && ev.direction === "send" ||
                       ev.endpoint === "server" && ev.direction === "recv";
    // normalize: the packet's source endpoint
    let src;
    if (ev.direction === "send") src = ev.endpoint;
    else src = ev.endpoint === "client" ? "server" : "client";
    const y1 = laneY[src];
    const y2 = laneY[src === "client" ? "server" : "client"];
    const cx = x(ev.time);
    const cls = pktClass(ev);
    const lineCls = "arrow-line arrow-" + cls;
    const dot = `<circle cx="${cx}" cy="${y1}" r="4.5" class="pkt-${KIND_COLOR[cls]}"/>`;
    const label = `${flagsOf(ev)}${ev.payloadLen ? " " + ev.payloadLen + "B" : ""} seq=${ev.seq}${ev.ack ? " ack=" + ev.ack : ""}`;
    const ly = y1 < y2 ? y2 + 14 : y2 - 8;
    const arrow = y1 < y2
      ? `<line class="${lineCls}" x1="${cx}" y1="${y1 + 6}" x2="${cx}" y2="${y2 - 8}"/>
         <path d="M ${cx - 4} ${y2 - 12} L ${cx} ${y2 - 6} L ${cx + 4} ${y2 - 12}" class="${lineCls}"/>`
      : `<line class="${lineCls}" x1="${cx}" y1="${y1 - 6}" x2="${cx}" y2="${y2 + 8}"/>
         <path d="M ${cx - 4} ${y2 + 12} L ${cx} ${y2 + 6} L ${cx + 4} ${y2 + 12}" class="${lineCls}"/>`;
    return `<g>${arrow}${dot}<text class="pkt-label" x="${cx}" y="${ly}" text-anchor="middle" transform="rotate(-18 ${cx} ${ly})">${label}</text></g>`;
  }).join("");

  $("timeline").innerHTML =
    `<svg viewBox="0 0 ${W} ${H}" role="img">${ticks.join("")}${bands}${lanes}${arrows}</svg>`;
}

async function showMatrix() {
  const rows = await (await fetch("/api/matrix")).json();
  const head = ["loss", "pattern", "mode", "options", "完成", "p50", "p90", "p99", "max", "nagle", "delack", "rto", "fast"];
  const body = rows.map(r => {
    const opts = r.serverQuick ? "srv-quickack"
      : (r.clientNoDelay && r.serverNoDelay) ? "both-nodelay"
      : r.clientNoDelay ? "cli-nodelay" : "baseline";
    const bad = r.nagleStalls > 0 || r.rtos > 0;
    return `<tr class="${bad ? "bad" : ""}">
      <td>${r.lossRate.toFixed(2)}</td><td>${r.clientPattern}</td><td>${r.clientMode}</td>
      <td>${opts}</td><td>${r.completed ? "✓" : "✗"}</td>
      <td>${fmtDur(r.p50)}</td><td>${fmtDur(r.p90)}</td><td>${fmtDur(r.p99)}</td><td>${fmtDur(r.max)}</td>
      <td>${r.nagleStalls}</td><td>${r.delackOnly}</td><td>${r.rtos}</td><td>${r.fastRetrans}</td></tr>`;
  }).join("");
  $("matrix").innerHTML = `<tr>${head.map(h => `<th>${h}</th>`).join("")}</tr>${body}`;
  $("matrixCard").style.display = "";
}

$("run").addEventListener("click", run);
$("matrixBtn").addEventListener("click", showMatrix);
$("preset").addEventListener("change", run);
loadPresets().then(run);
