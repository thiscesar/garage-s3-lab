// Painel da demo. Recebe estado (1 s) e lotes de operações (250 ms) por SSE; desenha a linha do
// tempo dos últimos 60 s com Chart.js (vendorizado). Nenhuma lógica de cluster aqui: só chamadas à API.
"use strict";

const JANELA_S = 60;
const CORES = { fresh: "#2f855a", ok: "#2f855a", stale: "#dd8a1b", error: "#c53030", anomaly: "#6b46c1" };
// Faixas de cima para baixo: leituras (maioria, minoria), depois escritas.
const FAIXAS = [
  { op: "GET", no: 2, rotulo: "g2 leitura (maioria)" },
  { op: "GET", no: 3, rotulo: "g3 leitura (minoria)" },
  { op: "GET", no: 1, rotulo: "g1 leitura" },
  { op: "PUT", no: 1, rotulo: "g1 escrita" },
  { op: "PUT", no: 3, rotulo: "g3 escrita" },
];
const indiceFaixa = new Map(FAIXAS.map((f, i) => [`${f.op}${f.no}`, FAIXAS.length - 1 - i]));

let ops = [];            // operações dos últimos 60 s
let estado = null;       // último estado do servidor
let relogio = { servidor: 0, local: performance.now() };
let ultimoModo = null;    // modo mostrado no seletor

const agora = () => relogio.servidor + (performance.now() - relogio.local) / 1000;
const $ = (id) => document.getElementById(id);

// ---------------------------------------------------------------- gráfico

const marcosPlugin = {
  id: "marcos",
  afterDatasetsDraw(chart) {
    if (!estado) return;
    const { ctx, chartArea: a, scales: { x } } = chart;
    ctx.save();
    ctx.font = "12px system-ui";
    for (const m of estado.marcos || []) {
      if (m.t < x.min || m.t > x.max) continue;
      const px = x.getPixelForValue(m.t);
      ctx.strokeStyle = "#2d3748";
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(px, a.top);
      ctx.lineTo(px, a.bottom);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.fillStyle = "#2d3748";
      ctx.fillText(m.rotulo, px + 4, a.top + 12);
    }
    ctx.restore();
  },
};

const grafico = new Chart($("linha-do-tempo"), {
  type: "scatter",
  data: {
    datasets: ["fresh", "stale", "error", "anomaly"].map((cls) => ({
      label: cls, data: [], pointRadius: 3.5, pointHoverRadius: 3.5,
      backgroundColor: CORES[cls], borderColor: CORES[cls], pointStyle: "rectRot",
    })),
  },
  options: {
    animation: false,
    maintainAspectRatio: false,
    plugins: { legend: { display: false }, tooltip: { enabled: false } },
    scales: {
      x: {
        type: "linear",
        title: { display: true, text: "tempo (s) desde que o painel abriu" },
        ticks: { stepSize: 10, callback: (v) => Math.round(v) },
      },
      y: {
        min: -0.6, max: FAIXAS.length - 0.4,
        ticks: { stepSize: 1, callback: (v) => (FAIXAS[FAIXAS.length - 1 - v] || {}).rotulo || "" },
        grid: { display: false },
      },
    },
  },
  plugins: [marcosPlugin],
});

function desenhar() {
  const t = agora();
  ops = ops.filter((o) => o.t >= t - JANELA_S);
  const porClasse = { fresh: [], stale: [], error: [], anomaly: [] };
  for (const o of ops) {
    const y = indiceFaixa.get(`${o.op}${o.no}`);
    if (y === undefined) continue;
    const cls = o.cls === "ok" ? "fresh" : o.cls;
    (porClasse[cls] || porClasse.error).push({ x: o.t, y });
  }
  grafico.data.datasets.forEach((d) => { d.data = porClasse[d.label]; });
  grafico.options.scales.x.min = t - JANELA_S;
  grafico.options.scales.x.max = t;
  grafico.update("none");
  contadores();
}

function contadores() {
  const linhas = [];
  const v = (estado && estado.carga && estado.carga.versoes) || { confirmada: null, lida: {} };
  for (const f of FAIXAS) {
    const sel = ops.filter((o) => o.op === f.op && o.no === f.no);
    if (!sel.length) continue;
    const resp = sel.filter((o) => ["fresh", "stale", "ok"].includes(o.cls)).length;
    const stale = sel.filter((o) => o.cls === "stale").length;
    const disp = (100 * resp) / sel.length;
    const taxa = f.op === "GET" ? (resp ? `${((100 * stale) / resp).toFixed(1)}%` : "n/a") : "—";
    const versao = f.op === "GET" ? (v.lida[f.no] ?? "—") : (f.no === 1 ? `${v.confirmada ?? "—"} (confirmada)` : "—");
    linhas.push(`<tr><td>${f.rotulo}</td><td>${sel.length}</td>
      <td class="${disp < 90 ? "baixo" : ""}">${disp.toFixed(1)}%</td>
      <td class="${f.op === "GET" && stale ? "alto" : ""}">${taxa}</td><td>${versao}</td></tr>`);
  }
  $("contadores").innerHTML = linhas.join("") || `<tr><td colspan="5">sem carga — escolha um preset e clique em Iniciar</td></tr>`;
}

// ---------------------------------------------------------------- estado

const fmtMiB = (b) => (b == null ? "—" : `${(b / 2 ** 20).toFixed(1)} MiB`);
const simNao = (x) => (x ? '<span class="sim">sim</span>' : '<span class="nao">NÃO</span>');

function renderEstado(e) {
  estado = e;
  relogio = { servidor: e.agora, local: performance.now() };

  const b = $("banner-modo");
  b.className = `banner ${e.modo || ""}`;
  $("modo").textContent = e.modo || "?";
  const q = e.quorum || {};
  $("quorum").textContent = `N=${q.N} · R=${q.R ?? "?"} · W=${q.W ?? "?"}`;
  $("garantia").textContent = q["R+W>N"] ? "R + W > N: leitura após escrita garantida" : "R + W ≤ N: sem garantia de leitura após escrita";

  const lento = (e.lento && e.lento.nos) || {};
  $("nos").innerHTML = [1, 2, 3].map((n) => {
    const x = (e.nos || {})[n] || {};
    const l = lento[n] || {};
    const classe = x.status !== "running" ? "parado" : (!x.na_interna ? "isolado" : "");
    return `<div class="no ${classe}">
      <h3>garage${n}<span>${x.zona || ""}</span></h3>
      <dl>
        <dt>contêiner</dt><dd>${x.status || "—"} ${x.saude ? `(${x.saude})` : ""}</dd>
        <dt>/health</dt><dd>${x.health ?? "—"}</dd>
        <dt>rede interna</dt><dd>${simNao(x.na_interna)} ${x.ip_interno || ""}</dd>
        <dt>vê como ativos</dt><dd>${x.ve_up ? x.ve_up.join(", ") : "—"}</dd>
        <dt>memória</dt><dd>${fmtMiB(l.memoria)}</dd>
        <dt>disco (data/)</dt><dd>${fmtMiB(l.disco_dados)}</dd>
        <dt>objetos (tabela)</dt><dd>${l.objetos ?? "—"}</dd>
      </dl></div>`;
  }).join("");

  if (!$("sel-modo").options.length) {
    $("sel-modo").innerHTML = e.modos.map((m) => `<option>${m}</option>`).join("");
    $("sel-preset").innerHTML = Object.keys(e.presets).map((p) => `<option>${p}</option>`).join("");
    $("desc-preset").textContent = e.presets[$("sel-preset").value];
    $("botoes-nos").innerHTML = [1, 2, 3].map((n) =>
      `<button data-acao="node" data-no="${n}" data-op="stop" class="perigo">Parar g${n}</button>
       <button data-acao="node" data-no="${n}" data-op="start">Ligar g${n}</button>`).join("");
    ligarBotoes();
  }
  // o seletor só acompanha o servidor quando o modo muda de fato (não apaga a escolha do usuário)
  if (e.modo && e.modo !== ultimoModo) { $("sel-modo").value = e.modo; ultimoModo = e.modo; }

  const a = e.acao;
  const el = $("acao");
  if (a) {
    const dur = ((a.fim ?? e.agora) - a.inicio).toFixed(1);
    el.className = `acao ${a.situacao === "erro" ? "erro" : ""}`;
    el.textContent = a.situacao === "em andamento"
      ? `⏳ ${a.nome}… ${dur} s`
      : `${a.situacao === "ok" ? "✔" : "✖"} ${a.nome} (${dur} s)${a.situacao === "erro" ? ": " + a.resultado : ""}`;
  }
  document.querySelectorAll("button[data-acao], button[data-repair], #trocar-modo, #restaurar")
    .forEach((btn) => { btn.disabled = e.ocupado; });
  $("iniciar-carga").textContent = e.carga.preset ? `Reiniciar (${e.carga.preset} ativo)` : "Iniciar";

  $("journal").innerHTML = ((e.lento && e.lento.journal) || []).slice().reverse()
    .map((j) => `<li><b>${new Date(j.ts).toLocaleTimeString("pt-BR")}</b> ${j.comando} ${escapar(JSON.stringify(j.args))}</li>`).join("");
  $("logs").innerHTML = [1, 2, 3].map((n) =>
    `<b>garage${n}</b>${((lento[n] || {}).logs || []).map((l) => escapar(l.replace(/^\S+\s+/, ""))).join("\n") || "—"}`).join("");
}

const escapar = (s) => s.replace(/[&<>]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;" }[c]));

// ---------------------------------------------------------------- ações

async function post(url, corpo) {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: corpo ? JSON.stringify(corpo) : undefined,
  });
  if (!r.ok) {
    const msg = (await r.json().catch(() => ({}))).detail || r.statusText;
    $("acao").className = "acao erro";
    $("acao").textContent = `✖ ${msg}`;
  }
  return r;
}

function ligarBotoes() {
  document.querySelectorAll("button[data-acao]").forEach((btn) => {
    btn.onclick = () => post(`/api/${btn.dataset.acao}/${btn.dataset.no}/${btn.dataset.op}`);
  });
}
document.querySelectorAll("button[data-repair]").forEach((btn) => {
  btn.onclick = () => post("/api/repair", { tipo: btn.dataset.repair });
});
// Troca de modo com confirmação no próprio botão (1º clique pede, 2º executa; expira em 5 s).
let confirmarModo = null;
$("trocar-modo").onclick = () => {
  const m = $("sel-modo").value;
  if (confirmarModo === m) {
    confirmarModo = null;
    $("trocar-modo").textContent = "Trocar modo";
    post("/api/mode", { mode: m });
    return;
  }
  confirmarModo = m;
  $("trocar-modo").textContent = `Confirmar: ${m} (reinicia os 3 nós)`;
  setTimeout(() => { if (confirmarModo === m) { confirmarModo = null; $("trocar-modo").textContent = "Trocar modo"; } }, 5000);
};
$("restaurar").onclick = () => post("/api/restore");
$("iniciar-carga").onclick = () => { ops = []; post("/api/workload/start", { preset: $("sel-preset").value }); };
$("parar-carga").onclick = () => post("/api/workload/stop");
$("sel-preset").onchange = () => { $("desc-preset").textContent = estado.presets[$("sel-preset").value]; };

// ---------------------------------------------------------------- conexão

const fonte = new EventSource("/api/stream");
fonte.addEventListener("estado", (m) => renderEstado(JSON.parse(m.data)));
fonte.addEventListener("ops", (m) => {
  const d = JSON.parse(m.data);
  relogio = { servidor: d.agora, local: performance.now() };
  ops.push(...d.ops);
});
fonte.onopen = () => { $("conexao").textContent = "ao vivo"; $("conexao").className = "pill ok"; };
fonte.onerror = () => { $("conexao").textContent = "sem conexão com o painel"; $("conexao").className = "pill falha"; };
setInterval(desenhar, 250);
