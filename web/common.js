"use strict";

const UNITS = ["", "k", "M", "G", "T", "P", "E"];
const SVG_NS = "http://www.w3.org/2000/svg";
const ICONS = {
  bolt: ["M13 2 5 13h6l-1 9 8-11h-6l1-9Z"],
  peers: ["M5 12a2 2 0 1 0 4 0a2 2 0 1 0-4 0", "M15 6a2 2 0 1 0 4 0a2 2 0 1 0-4 0", "M15 18a2 2 0 1 0 4 0a2 2 0 1 0-4 0", "m8.7 11 6.6-3.8", "m8.7 13 6.6 3.8"],
  target: ["M3 12a9 9 0 1 0 18 0a9 9 0 1 0-18 0", "M7 12a5 5 0 1 0 10 0a5 5 0 1 0-10 0", "M11 12a1 1 0 1 0 2 0a1 1 0 1 0-2 0"],
  height: ["M12 4 3 8.5l9 4.5 9-4.5L12 4Z", "m3 13 9 4.5 9-4.5", "m3 17.5 9 4.5 9-4.5"],
  block: ["M12 3 4 7v10l8 4 8-4V7l-8-4Z", "m4 7 8 4 8-4", "M12 11v10"],
  clock: ["M3 12a9 9 0 1 0 18 0a9 9 0 1 0-18 0", "M12 7v5l3 2"],
  spark: ["M12 3v4", "M12 17v4", "M3 12h4", "M17 12h4", "m6 6 2.5 2.5", "m15.5 15.5 2.5 2.5", "m6 18 2.5-2.5", "M15.5 8.5 18 6"],
  coins: ["M4 6c0 1.7 3.6 3 8 3s8-1.3 8-3-3.6-3-8-3-8 1.3-8 3Z", "M4 6v6c0 1.7 3.6 3 8 3s8-1.3 8-3V6", "M4 12v6c0 1.7 3.6 3 8 3s8-1.3 8-3v-6"],
  inbox: ["M4 13h4l2 3h4l2-3h4", "M4 13 6 5h12l2 8v6H4v-6Z"],
  copy: ["M9 9h11v11H9V9Z", "M5 15H4V4h11v1"],
  check: ["m5 12.5 5 4.5 9-10"],
};
/** A finalized block has been credited. */
const STAGES = ["found", "registered", "executed", "finalized"];

const byId = (id) => document.getElementById(id);

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function icon(name) {
  const svg = document.createElementNS(SVG_NS, "svg");
  svg.setAttribute("viewBox", "0 0 24 24");
  svg.setAttribute("class", "icon");
  svg.setAttribute("aria-hidden", "true");
  for (const shape of ICONS[name] || []) {
    const path = document.createElementNS(SVG_NS, "path");
    path.setAttribute("d", shape);
    svg.append(path);
  }
  return svg;
}

function card(label, symbol) {
  const node = el("article", "card");
  const heading = el("div", "label");
  if (symbol) {
    const chip = el("span", "card__icon");
    chip.append(icon(symbol));
    heading.append(chip);
  }
  heading.append(el("span", "", label));
  const value = el("div", "value");
  value.append(el("strong", "", "-"), el("small", "muted", ""));
  node.append(heading, value);
  return node;
}

function setCard(node, main, detail) {
  const value = node.querySelector(".value");
  setText(value.querySelector("strong"), main);
  setText(value.querySelector("small"), detail || "");
}

/** Preserve text selections across refreshes with identical values. */
function setText(node, text) {
  if (node.textContent !== text) node.textContent = text;
}

function badge(label, tone) {
  return el("span", tone ? `badge ${tone}` : "badge", label);
}

/** Badge thresholds for stale share percentage. */
function health(accepted, rejected, stale) {
  const total = accepted + rejected + stale;
  if (!(total > 0)) return "-";
  const part = stale / total;
  const [text, tone] = part <= 0.01 ? ["healthy", "good"] : part <= 0.02 ? ["OK", "warn"] : ["unhealthy", "bad"];
  const node = el("span", `badge ${tone}`, text);
  node.title = `${(part * 100).toFixed(2)} % stale`;
  return node;
}

function parts(value) {
  let index = 0;
  while (Math.abs(value) >= 1000 && index < UNITS.length - 1) {
    value /= 1000;
    index += 1;
  }
  return [value.toFixed(2), UNITS[index]];
}

function rate(value) {
  if (!Number.isFinite(value)) return "-";
  const [number, unit] = parts(value);
  return `${number} ${unit}H/s`;
}

function difficulty(value) {
  if (!Number.isFinite(value)) return "-";
  const [number, unit] = parts(value);
  return unit ? `${number} ${unit}` : number;
}

function count(value) {
  return Number(value || 0).toLocaleString("en-US");
}

function plural(value, noun) {
  return `${count(value)} ${Number(value) === 1 ? noun : `${noun}s`}`;
}

function ago(seconds) {
  if (!Number.isFinite(seconds)) return "-";
  if (seconds < 60) return `${seconds}s ago`;
  if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
  if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
  return `${Math.floor(seconds / 86400)}d ago`;
}

function span(seconds) {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  if (days > 0) return hours ? `${days}d ${hours}h` : `${days}d`;
  if (hours > 0) return minutes ? `${hours}h ${minutes}m` : `${hours}h`;
  return `${minutes}m`;
}

function clock(unixSeconds) {
  return new Date(unixSeconds * 1000).toLocaleTimeString();
}

function dateTime(unixSeconds) {
  return new Date(unixSeconds * 1000).toLocaleString();
}

function stamp(unixSeconds) {
  const date = new Date(unixSeconds * 1000);
  const node = el("span", "stamp", date.toLocaleTimeString());
  node.append(el("small", "", date.toLocaleDateString()));
  return node;
}

function short(text, keep = 6) {
  return text.length > keep * 2 + 8 ? `${text.slice(0, keep + 6)}…${text.slice(-keep)}` : text;
}

const keptChildren = new WeakMap();

/** Update keyed children without replacing unchanged entries. */
function keyedChildren(holder, items, keyOf, build, signatureOf = (item) => JSON.stringify(item)) {
  const previous = keptChildren.get(holder) ?? new Map();
  const current = new Map();
  const nodes = items.map((item) => {
    const key = keyOf(item);
    const signature = signatureOf(item);
    const kept = current.has(key) ? null : previous.get(key);
    const node = kept?.signature === signature ? kept.node : build(item);
    if (!current.has(key)) current.set(key, { signature, node });
    return node;
  });
  keptChildren.set(holder, current);
  nodes.forEach((node, index) => {
    if (holder.children[index] !== node) holder.insertBefore(node, holder.children[index] ?? null);
  });
  while (holder.children.length > nodes.length) holder.lastElementChild.remove();
}

function row(cells) {
  const tr = document.createElement("tr");
  for (const [label, content, className] of cells) {
    const td = el("td", className);
    td.dataset.label = label;
    if (content instanceof Node) td.append(content);
    else td.textContent = content;
    tr.append(td);
  }
  return tr;
}

async function load(path) {
  const response = await fetch(path, { cache: "no-store" });
  if (!response.ok) throw new Error(String(response.status));
  return response.json();
}

function live(ok) {
  const state = byId("state");
  state.textContent = ok ? "Live" : "Offline";
  state.className = ok ? "pill live" : "pill bad";
}
