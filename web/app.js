"use strict";

const REFRESH = { home: 3000, miner: 3000, blocks: 5000, payments: 5000, connect: 5000 };
const HISTORY_EVERY = 60000;
const STALE_JOB_SECONDS = 60;
const RECENT = 10;
const ADDRESS = /^[a-z0-9]{8,128}$/;
const SVG = "http://www.w3.org/2000/svg";
const SAVED = "tenrai-pool-address";
const PAGES = ["home", "miner", "blocks", "payments", "connect"];
const STAGE_TONE = { found: "wait", registered: "registered", executed: "good", finalized: "good", expired: "bad", rejected: "bad" };
const PAYOUT_TONE = { paid: "good", failed: "bad" };

let timer = 0;
let generation = 0;
let historyAt = 0;
let historyPoints = [];
let shownMiner = "";
let explorer = null;
let resume = null;
const lists = {
  blocks: { mode: "", next: null, paged: false, token: 0, shown: new Set() },
  payments: { next: null, paged: false, token: 0, shown: new Set() },
};
const cards = {};

function route() {
  const path = location.pathname;
  if (path.startsWith("/miner/")) {
    let address = "";
    try {
      address = decodeURIComponent(path.slice(7));
    } catch {}
    return { page: "miner", address };
  }
  if (path === "/blocks") return { page: "blocks" };
  if (path === "/payments") return { page: "payments" };
  if (path === "/connect") return { page: "connect" };
  return { page: "home" };
}

function navigate(path) {
  if (path !== location.pathname) history.pushState(null, "", path);
  show();
}

function show() {
  const target = route();
  for (const name of PAGES) byId(`page-${name}`).hidden = name !== target.page;
  document.querySelectorAll(".tabs a").forEach((link) => link.classList.toggle("active", link.dataset.page === target.page));
  byId("search-note").textContent = "";
  for (const list of Object.values(lists)) {
    list.next = null;
    list.paged = false;
    list.token += 1;
  }
  byId("blocks-more").hidden = true;
  byId("payments-more").hidden = true;
  historyAt = 0;
  resume = null;
  clearTimeout(timer);
  window.scrollTo(0, 0);
  generation += 1;
  refresh(target, generation);
}

async function refresh(target, run) {
  if (document.hidden) {
    resume = () => refresh(target, run);
    return;
  }
  try {
    const pool = await load("/api/v1/pool");
    if (run !== generation) return;
    showPool(pool);
    if (target.page === "home") await showHome(pool);
    if (target.page === "miner") await showMiner(target.address, run);
    if (target.page === "blocks") await showBlocks(false);
    if (target.page === "payments") await showPayments(false);
    if (target.page === "connect") showConnect(pool);
    live(true);
  } catch {
    live(false);
  }
  if (run === generation) timer = setTimeout(() => refresh(target, run), REFRESH[target.page]);
}

function trai(amount) {
  return amount === null || amount === undefined ? "-" : `${amount} TRAI`;
}

/** Report hourly effort only after the mode has found its first block. */
function hourEffort(mode) {
  if (mode.effort_hour !== null && mode.effort_hour !== undefined) return mode.effort_hour;
  return mode.hashrate > 0 ? null : 0;
}

function effort(value) {
  return value === null || value === undefined ? "-" : `${Math.round(value)} %`;
}

/** Count blocks with unmeasured effort at 100%. */
function blockEffort(block) {
  return effort(block.effort ?? 100);
}

function reward(block) {
  if (block.reward !== null && block.reward !== undefined) return `${block.reward} TRAI`;
  return STAGES.includes(block.stage) ? "pending" : "-";
}

function outside(kind, value, text) {
  if (!explorer) {
    const plain = el("span", "mono", text);
    plain.title = String(value);
    return plain;
  }
  const anchor = el("a", "link mono", text);
  anchor.href = `${explorer}#/${kind}/${encodeURIComponent(String(value))}`;
  anchor.target = "_blank";
  anchor.rel = "noopener noreferrer";
  anchor.title = String(value);
  return anchor;
}

function copyButton(value) {
  const button = el("button", "copy");
  button.type = "button";
  button.title = "Copy";
  button.setAttribute("aria-label", "Copy");
  button.append(icon("copy"));
  button.addEventListener("click", async () => {
    try {
      await navigator.clipboard.writeText(String(value));
      button.replaceChildren(icon("check"));
      button.classList.add("done");
      setTimeout(() => {
        button.replaceChildren(icon("copy"));
        button.classList.remove("done");
      }, 1500);
    } catch {
      button.title = "Select the text to copy it";
    }
  });
  return button;
}

function copyable(value) {
  const holder = el("span", "copyable");
  holder.append(el("span", "mono", value), copyButton(value));
  return holder;
}

/** Render single-item arrays as inline code. */
function phrase(...parts) {
  const holder = el("span");
  for (const part of parts) holder.append(Array.isArray(part) ? el("code", "mono", part[0]) : part);
  return holder;
}

function link(path, text, title) {
  const anchor = el("a", "link mono", text);
  anchor.href = path;
  anchor.dataset.link = "";
  if (title) anchor.title = title;
  return anchor;
}

function minerLink(address) {
  return link(`/miner/${encodeURIComponent(address)}`, short(address), address);
}

function capital(text) {
  return text.charAt(0).toUpperCase() + text.slice(1);
}

function stage(block) {
  const node = badge(capital(block.stage), STAGE_TONE[block.stage] || "wait");
  if (block.reason) node.title = block.reason;
  return node;
}

function payoutState(item) {
  const node = badge(capital(item.status), PAYOUT_TONE[item.status] || "wait");
  if (item.reason) node.title = item.reason;
  return node;
}

function transaction(id) {
  return id ? outside("tx", id, short(id, 6)) : "-";
}

function cardsIn(holder, specs) {
  if (!cards[holder]) {
    cards[holder] = {};
    const grid = byId(holder);
    for (const [key, label, symbol] of specs) {
      cards[holder][key] = card(label, symbol);
      grid.append(cards[holder][key]);
    }
  }
  return cards[holder];
}

function showPool(pool) {
  explorer = pool.explorer || null;
  byId("network").textContent = capital(pool.network);
  document.title = `${pool.name} | Tenrai Technologies`;
}

async function showHome(pool) {
  const pplns = pool.modes.pplns;
  const solo = pool.modes.solo;
  const node = pool.node;
  const home = cardsIn("pool-cards", [
    ["hashrate", "Pool hashrate", "bolt"],
    ["miners", "Miners", "peers"],
    ["network", "Network", "target"],
    ["height", "Height", "height"],
    ["blocks", "Blocks 24 h", "block"],
    ["last", "Last block", "clock"],
    ["effort", "Effort, last hour", "spark"],
    ["paid", "Paid to miners", "coins"],
  ]);
  setCard(home.hashrate, rate(pplns.hashrate + solo.hashrate), `PPLNS ${rate(pplns.hashrate)} · SOLO ${rate(solo.hashrate)}`);
  setCard(home.miners, count(pool.miners_online), `${plural(pool.workers_online, "worker")} · ${plural(pool.connections, "connection")}`);
  setCard(home.network, rate(node.hashrate), `difficulty ${difficulty(node.difficulty)}`);
  setCard(home.height, count(node.height), node.synced ? `a cut every ${node.cut_interval_ms / 1000} s` : "node is syncing");
  const open = pplns.blocks.open + solo.blocks.open;
  setCard(home.blocks, count(pool.blocks_24h), `${count(open)} waiting for their reward`);
  const last = Math.max(pplns.last_block || 0, solo.last_block || 0);
  setCard(home.last, last ? ago(Math.round((Date.now() - last) / 1000)) : "none yet", last ? clock(last / 1000) : "");
  const measured = node.difficulty > pool.min_difficulty;
  const hour = (mode) => (measured ? hourEffort(mode) : mode.hashrate > 0 ? 100 : 0);
  const pplnsHour = hour(pplns);
  setCard(home.effort, effort(pplnsHour), `PPLNS ${effort(pplnsHour)} | SOLO ${effort(hour(solo))} · round now ${effort(measured ? pplns.effort : 100)}`);
  setCard(home.paid, trai(pool.balances.paid), `${trai(pool.balances.unpaid)} unpaid · ${count(pool.balances.payouts)} payouts`);
  keyedChildren(byId("top-rows"), pool.top, (miner) => miner.address, (miner) =>
    row([
      ["Address", minerLink(miner.address)],
      ["Hashrate", rate(miner.hashrate)],
      ["Workers", count(miner.workers)],
    ]),
  );
  byId("top-empty").hidden = pool.top.length > 0;
  const [recent, payments] = await Promise.all([load("/api/v1/blocks"), load("/api/v1/payments")]);
  const blocks = recent.blocks.slice(0, RECENT);
  keyedChildren(byId("recent-rows"), blocks, (block) => block.id, (block) =>
    row([
      ["Found", clock(block.found_at / 1000)],
      ["Height", outside("cut", block.height, count(block.height))],
      ["Finder", minerLink(block.address)],
      ["Reward", reward(block)],
      ["Status", stage(block)],
    ]),
  );
  byId("recent-empty").hidden = blocks.length > 0;
  const sent = payments.payments.slice(0, RECENT);
  keyedChildren(byId("recent-payment-rows"), sent, (payment) => payment.id, (payment) =>
    row([
      ["Time", clock(payment.created_at / 1000)],
      ["Total", trai(payment.total)],
      ["Status", payoutState(payment)],
      ["Transaction", transaction(payment.transaction)],
    ]),
  );
  byId("recent-payments-empty").hidden = sent.length > 0;
  if (Date.now() - historyAt > HISTORY_EVERY) {
    historyPoints = (await load("/api/v1/history")).points;
    historyAt = Date.now();
  }
  chart(byId("pool-chart"), withNow(historyPoints, pplns.hashrate + solo.hashrate));
}

// Extend the history line to the current sample.
function withNow(points, hashrate) {
  return Number.isFinite(hashrate) ? [...points, [Date.now(), hashrate]] : points;
}

function words(text) {
  return el("span", "verify-words", text ? text.split(" ").join(" · ") : "-");
}

function scheme(port) {
  if (port.web) return "ws";
  if (port.protocol === "v2") return "stratum2+tls";
  return port.tls ? "stratum+tls" : "stratum+tcp";
}

function fact(term, value, wide) {
  const item = el("div", wide ? "kv__item kv__item--wide" : "kv__item");
  const detail = el("dd");
  detail.append(value);
  item.append(el("dt", "", term), detail);
  return item;
}

function showConnect(pool) {
  const host = pool.host || (location.hostname.includes(":") ? `[${location.hostname}]` : location.hostname);
  const configured = (pool.urls || []).map((entry) => [entry.mode, entry.url]);
  const urls = configured.length
    ? configured
    : pool.ports.map((port) => [port.mode, `${scheme(port)}://${host}:${port.port}`]);
  showNodeHealth(pool.node);
  const list = byId("connect");
  const signature = JSON.stringify([
    urls, pool.pool_address, pool.pool_address_words, pool.fees, pool.payouts, pool.pplns_window,
    pool.pplns_lookback_minutes, pool.start_difficulty, pool.share_interval, pool.shares_per_block,
    pool.api_rate_limit,
  ]);
  if (list.dataset.signature === signature) return;
  list.dataset.signature = signature;
  const items = [];
  for (const mode of ["pplns", "solo"]) {
    const entries = urls.filter(([name]) => name === mode);
    if (!entries.length) continue;
    const value = el("span", "urls");
    for (const [, url] of entries) {
      const line = el("span", "url");
      const v2 = url.startsWith("stratum2+");
      const web = url.startsWith("ws://") || url.startsWith("wss://");
      line.append(el("code", "mono", url), badge(web ? "Web" : v2 ? "V2" : "V1", v2 ? "v2" : "wait"), copyButton(url));
      value.append(line);
    }
    items.push(fact(mode.toUpperCase(), value, true));
  }
  const payouts = pool.payouts.enabled
    ? `every ${pool.payouts.interval_minutes} min from ${trai(pool.payouts.minimum)}, network fee paid by the ${pool.payouts.fee_payer}`
    : "off";
  items.push(
    fact("Pool address", copyable(pool.pool_address), true),
    fact("Verification words", words(pool.pool_address_words), true),
    fact("Login", phrase(["<address>.<worker>"], ", or only the address")),
    fact("Password", phrase("anything; ", ["d=<difficulty>"], " asks for a fixed difficulty")),
    fact("Fees", `PPLNS ${pool.fees.pplns} % · SOLO ${pool.fees.solo} %`),
    fact("PPLNS window", `${pool.pplns_window}× the block's network difficulty, at most ${span(pool.pplns_lookback_minutes * 60)} back`),
    fact("Difficulty", `starts at ${difficulty(pool.start_difficulty)}, one share about every ${pool.share_interval} s, at least ${pool.shares_per_block} shares per block`),
    fact("Payouts", payouts),
    fact("Miner", phrase(["tenrai-miner"], " mines for a pool only with ", ["--pool"])),
  );
  list.replaceChildren(...items);
  const first = urls.find(([mode]) => mode === "pplns") || urls[0];
  const command = `tenrai-miner -a <address> --stratum ${first ? first[1] : ""} -w rig1 --pool`;
  byId("command").textContent = command;
  byId("command-copy").replaceChildren(copyButton(command));
  showApi(pool);
}

function showApi(pool) {
  const base = `${location.origin}/api/pools`;
  const endpoint = (url) => {
    const line = el("span", "url");
    line.append(el("code", "mono", `GET ${url}`), copyButton(url));
    return line;
  };
  const limits = pool.api_rate_limit > 0
    ? `${count(pool.api_rate_limit)} requests a minute per client, answers cached for 10 s, read only`
    : "answers cached for 10 s, read only";
  byId("api-facts").replaceChildren(
    fact("Pool", endpoint(base), true),
    fact("Blocks", endpoint(`${location.origin}/api/blocks?page=0&pageSize=15`), true),
    fact("Miner", endpoint(`${location.origin}/api/miners/<address>`), true),
    fact("Limits", limits),
  );
}

function showNodeHealth(node) {
  const jobAge = node.work_at ? Math.max(0, Math.round((Date.now() - node.work_at) / 1000)) : null;
  const status = !node.synced
    ? badge("Syncing", "wait")
    : jobAge === null || jobAge > STALE_JOB_SECONDS
      ? badge("No fresh work", "bad")
      : badge("Healthy", "good");
  const pace = node.cut_interval_ms ? `a cut every ${node.cut_interval_ms / 1000} s, ${count(node.block_rate)} work blocks a second` : "-";
  const facts = [
    ["Status", status.textContent],
    ["Finalized height", node.height],
    ["Last job", jobAge === null ? "none yet" : ago(jobAge)],
    ["Peers", count(node.peers)],
    ["Target", pace],
  ];
  keyedChildren(byId("node-health"), facts, ([term]) => term, ([term, value]) =>
    fact(term, term === "Status" ? status : term === "Finalized height" ? outside("cut", value, count(value)) : value),
  );
}

async function showMiner(address, run) {
  const note = byId("search-note");
  if (address !== shownMiner) {
    shownMiner = address;
    byId("address").value = address;
    byId("miner-address").replaceChildren(copyable(address));
    byId("miner-words").textContent = "";
    const head = byId("miner-head");
    head.querySelector(".qr")?.remove();
    if (ADDRESS.test(address)) head.append(window.TenraiQr.svg(address, "QR code of this payout address"));
  }
  if (!ADDRESS.test(address)) {
    note.textContent = "That is not a payout address.";
    return;
  }
  const response = await fetch(`/api/v1/miners/${encodeURIComponent(address)}`, { cache: "no-store" });
  if (run !== generation) return;
  if (response.status === 404 || response.status === 400) {
    note.textContent = response.status === 404 ? "This address has not mined here yet." : "That is not an address of this network.";
    byId("page-miner").hidden = true;
    return;
  }
  if (!response.ok) throw new Error(String(response.status));
  const miner = await response.json();
  if (run !== generation) return;
  note.textContent = "";
  byId("page-miner").hidden = false;
  remember(address);
  const view = cardsIn("miner-cards", [
    ["hashrate", "Hashrate", "bolt"],
    ["unpaid", "Unpaid", "inbox"],
    ["paid", "Paid", "coins"],
    ["day", "Earned 24 h", "clock"],
  ]);
  setCard(view.hashrate, rate(miner.hashrate), `1 h average ${rate(miner.hashrate_1h)}`);
  setCard(view.unpaid, trai(miner.unpaid), `${trai(miner.pending)} in payouts on their way`);
  setCard(view.paid, trai(miner.paid), `${trai(miner.earned)} earned in total`);
  setCard(view.day, trai(miner.earned_24h), `${count(miner.workers_online)} of ${count(miner.workers.length)} workers online`);
  setText(byId("miner-words"), miner.verification_words ? miner.verification_words.split(" ").join(" · ") : "-");
  chart(byId("miner-chart"), withNow(miner.chart, miner.hashrate));
  const lastShare = (worker) => (worker.last_share ? ago(Math.round((Date.now() - worker.last_share) / 1000)) : "-");
  keyedChildren(
    byId("worker-rows"),
    miner.workers,
    (worker) => `${worker.mode}/${worker.protocol}/${worker.name}`,
    (worker) =>
      row([
        ["Worker", worker.online ? worker.name : `${worker.name} (offline)`, worker.online ? "" : "muted"],
        ["Mode", worker.protocol === "v2" ? `${worker.mode.toUpperCase()} · V2` : worker.mode.toUpperCase()],
        ["Hashrate", rate(worker.hashrate)],
        ["1 h", rate(worker.hashrate_1h)],
        ["Difficulty", `${difficulty(worker.difficulty)}${worker.fixed_difficulty ? " fixed" : ""}`],
        ["Shares", `${count(worker.accepted)} / ${count(worker.rejected)} / ${count(worker.stale)}`],
        ["Blocks", count(worker.blocks)],
        ["Health", health(worker.accepted, worker.rejected, worker.stale)],
        ["Last share", lastShare(worker)],
      ]),
    (worker) => JSON.stringify([worker, lastShare(worker)]),
  );
  byId("workers-empty").hidden = miner.workers.length > 0;
  keyedChildren(byId("miner-block-rows"), miner.blocks, (block) => block.id, (block) =>
    row([
      ["Found", stamp(block.found_at / 1000)],
      ["Height", outside("cut", block.height, count(block.height))],
      ["Mode", block.mode.toUpperCase()],
      ["Reward", reward(block)],
      ["Status", stage(block)],
    ]),
  );
  byId("miner-blocks-empty").hidden = miner.blocks.length > 0;
  keyedChildren(byId("miner-payout-rows"), miner.payouts, (payout) => payout.id, (payout) =>
    row([
      ["Time", stamp(payout.created_at / 1000)],
      ["Amount", trai(payout.amount)],
      ["Status", payoutState(payout)],
      ["Transaction", transaction(payout.transaction)],
    ]),
  );
  byId("miner-payouts-empty").hidden = miner.payouts.length > 0;
}

async function showBlocks(append) {
  const state = lists.blocks;
  if (!append && state.paged) return;
  if (append && !state.next) return;
  const token = ++state.token;
  const query = new URLSearchParams();
  if (state.mode) query.set("mode", state.mode);
  if (append) query.set("before", state.next);
  state.paged = state.paged || append;
  const page = await load(`/api/v1/blocks?${query}`);
  if (token !== state.token) return;
  const build = (block) =>
    row([
      ["Found", dateTime(block.found_at / 1000)],
      ["Height", outside("cut", block.height, count(block.height))],
      ["Block", outside("block", block.hash, short(block.hash, 6))],
      ["Mode", block.mode.toUpperCase()],
      ["Finder", minerLink(block.address)],
      ["Reward", reward(block)],
      ["Effort", blockEffort(block)],
      ["Status", stage(block)],
    ]);
  const fresh = unseen(state, page.blocks, append);
  if (append) byId("block-rows").append(...fresh.map(build));
  else keyedChildren(byId("block-rows"), fresh, (block) => block.id, build);
  state.next = page.next;
  byId("blocks-more").hidden = !page.next;
  byId("blocks-empty").hidden = byId("block-rows").children.length > 0;
}

async function showPayments(append) {
  const state = lists.payments;
  if (!append && state.paged) return;
  if (append && !state.next) return;
  const token = ++state.token;
  const query = append ? `?before=${state.next}` : "";
  state.paged = state.paged || append;
  const page = await load(`/api/v1/payments${query}`);
  if (token !== state.token) return;
  const build = (payment) =>
    row([
      ["Time", dateTime(payment.created_at / 1000)],
      ["Recipients", count(payment.recipients)],
      ["Total", trai(payment.total)],
      ["Fee", trai(payment.fee)],
      ["Status", payoutState(payment)],
      ["Transaction", transaction(payment.transaction)],
    ]);
  const fresh = unseen(state, page.payments, append);
  if (append) byId("payment-rows").append(...fresh.map(build));
  else keyedChildren(byId("payment-rows"), fresh, (payment) => payment.id, build);
  state.next = page.next;
  byId("payments-more").hidden = !page.next;
  byId("payments-empty").hidden = byId("payment-rows").children.length > 0;
}

function unseen(state, items, append) {
  if (!append) state.shown.clear();
  const fresh = items.filter((item) => !state.shown.has(item.id));
  for (const item of fresh) state.shown.add(item.id);
  return fresh;
}

async function more(button, show) {
  if (button.disabled) return;
  button.disabled = true;
  try {
    await show(true);
  } finally {
    button.disabled = false;
  }
}

function chart(container, points) {
  if (!points || points.length < 2) {
    if (!container.querySelector(".chart__empty")) container.replaceChildren(el("div", "chart__empty", "Not enough data yet"), el("div", "chart__labels", "\u00a0"));
    return;
  }
  const width = 1000;
  const height = 200;
  const first = points[0][0];
  const last = points[points.length - 1][0];
  const peak = Math.max(...points.map((point) => point[1])) || 1;
  const x = (time) => ((time - first) / Math.max(1, last - first)) * width;
  const y = (value) => height - (value / peak) * (height - 6);
  const line = points.map((point, index) => `${index ? "L" : "M"}${x(point[0]).toFixed(1)},${y(point[1]).toFixed(1)}`).join(" ");
  let svg = container.querySelector("svg");
  if (!svg) {
    svg = document.createElementNS(SVG, "svg");
    svg.setAttribute("viewBox", `0 0 ${width} ${height}`);
    svg.setAttribute("preserveAspectRatio", "none");
    svg.setAttribute("role", "img");
    for (const name of ["chart__area", "chart__line"]) {
      const path = document.createElementNS(SVG, "path");
      path.setAttribute("class", name);
      svg.append(path);
    }
    const labels = el("div", "chart__labels");
    labels.append(el("span"), el("span"), el("span"));
    container.replaceChildren(svg, labels);
  }
  const [area, stroke] = svg.children;
  svg.setAttribute("aria-label", `Hashrate, peak ${rate(peak)}`);
  area.setAttribute("d", `${line} L${width},${height} L0,${height} Z`);
  stroke.setAttribute("d", line);
  const [start, middle, end] = container.querySelector(".chart__labels").children;
  setText(start, clock(first / 1000));
  setText(middle, `peak ${rate(peak)}`);
  setText(end, clock(last / 1000));
}

function remember(address) {
  try {
    localStorage.setItem(SAVED, address);
  } catch {}
}

function remembered() {
  try {
    return localStorage.getItem(SAVED) || "";
  } catch {
    return "";
  }
}

document.addEventListener("click", (event) => {
  const button = event.target.closest(".help__button");
  for (const help of document.querySelectorAll('.help[data-open="true"]')) {
    if (help !== button?.parentElement) help.dataset.open = "false";
  }
  if (button) button.parentElement.dataset.open = String(button.parentElement.dataset.open !== "true");
  const anchor = event.target.closest("a[data-link]");
  if (!anchor || event.metaKey || event.ctrlKey || event.shiftKey || event.button !== 0) return;
  event.preventDefault();
  navigate(anchor.getAttribute("href"));
});

document.addEventListener("keydown", (event) => {
  if (event.key !== "/" || event.target.closest("input, textarea, select")) return;
  event.preventDefault();
  byId("address").focus();
});

byId("search").addEventListener("submit", (event) => {
  event.preventDefault();
  const value = byId("address").value.trim().toLowerCase();
  if (!ADDRESS.test(value)) {
    byId("search-note").textContent = "That is not a payout address.";
    return;
  }
  navigate(`/miner/${encodeURIComponent(value)}`);
});

byId("block-filters").addEventListener("click", (event) => {
  const button = event.target.closest("button[data-mode]");
  if (!button) return;
  lists.blocks.mode = button.dataset.mode;
  lists.blocks.next = null;
  lists.blocks.paged = false;
  byId("block-filters").querySelectorAll("button").forEach((item) => item.setAttribute("aria-pressed", String(item === button)));
  showBlocks(false).catch(() => live(false));
});

byId("blocks-more").addEventListener("click", (event) => more(event.currentTarget, showBlocks).catch(() => live(false)));
byId("payments-more").addEventListener("click", (event) => more(event.currentTarget, showPayments).catch(() => live(false)));
window.addEventListener("popstate", show);
document.addEventListener("visibilitychange", () => {
  if (document.hidden || !resume) return;
  const next = resume;
  resume = null;
  next();
});

if (!byId("address").value) byId("address").value = remembered();
show();
