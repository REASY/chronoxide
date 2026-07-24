---
marp: true
theme: default
size: 16:9
paginate: true
html: true
title: Chronoxide — OTLP-native metrics TSDB
description: An architecture walkthrough of Chronoxide, its interner, ingester, storage format, and query engine.
author: Chronoxide
footer: Chronoxide · architecture walkthrough · July 2026
---

<style>
  @font-face {
    font-family: "JetBrains Mono";
    src: url("./assets/JetBrainsMono-Regular.woff2") format("woff2");
    font-style: normal;
    font-weight: 400;
    font-display: swap;
  }

  @font-face {
    font-family: "JetBrains Mono";
    src: url("./assets/JetBrainsMono-Bold.woff2") format("woff2");
    font-style: normal;
    font-weight: 700 800;
    font-display: swap;
  }

  :root {
    --ink: #f4f8ff;
    --muted: #9bb0c9;
    --faint: #6f829b;
    --bg: #07111f;
    --panel: rgba(14, 30, 52, 0.88);
    --panel-2: rgba(10, 24, 43, 0.78);
    --line: rgba(147, 185, 224, 0.20);
    --cyan: #65e6e0;
    --blue: #78a9ff;
    --violet: #b995ff;
    --amber: #ffc76a;
    --green: #78e8a7;
    --red: #ff8295;
    --mono: "JetBrains Mono", "SFMono-Regular", Consolas, "Liberation Mono", monospace;
  }

  * { box-sizing: border-box; }

  section {
    width: 1280px;
    height: 720px;
    padding: 50px 62px 48px;
    overflow: hidden;
    color: var(--ink);
    font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont,
      "Segoe UI", sans-serif;
    background:
      radial-gradient(circle at 88% 8%, rgba(101, 230, 224, 0.10), transparent 30%),
      radial-gradient(circle at 8% 92%, rgba(120, 169, 255, 0.10), transparent 32%),
      linear-gradient(145deg, #081523 0%, var(--bg) 58%, #07101c 100%);
    letter-spacing: -0.01em;
  }

  section::before {
    content: "";
    position: absolute;
    inset: 0 0 auto 0;
    height: 4px;
    background: linear-gradient(90deg, var(--cyan), var(--blue), var(--violet));
  }

  section::after {
    right: 32px;
    bottom: 18px;
    color: var(--faint);
    font-size: 12px;
    font-weight: 700;
  }

  footer {
    display: none;
  }

  h1 {
    margin: 0 0 22px;
    color: var(--ink);
    font-size: 43px;
    font-weight: 760;
    letter-spacing: -0.035em;
  }

  h1 strong, h2 strong, h3 strong { color: var(--cyan); }

  h2 {
    margin: 0 0 12px;
    color: var(--ink);
    font-size: 27px;
    font-weight: 700;
    letter-spacing: -0.025em;
  }

  h3 {
    margin: 0 0 8px;
    color: var(--ink);
    font-size: 20px;
    font-weight: 720;
  }

  p, li {
    color: var(--muted);
    font-size: 19px;
    line-height: 1.38;
  }

  code {
    border: 1px solid rgba(120, 169, 255, 0.18);
    border-radius: 6px;
    padding: 0.05em 0.28em;
    color: #d8e8ff;
    background: rgba(120, 169, 255, 0.10);
    font-family: var(--mono);
    font-size: 0.86em;
  }

  .eyebrow {
    margin-bottom: 12px;
    color: var(--cyan);
    font-size: 13px;
    font-weight: 800;
    letter-spacing: 0.16em;
    text-transform: uppercase;
  }

  .lede {
    max-width: 1000px;
    margin: 0;
    color: #dbe8f8;
    font-size: 28px;
    line-height: 1.28;
    letter-spacing: -0.025em;
  }

  .small { font-size: 15px; line-height: 1.35; }
  .tiny { font-size: 12px; line-height: 1.35; }
  .muted { color: var(--muted); }
  .faint { color: var(--faint); }
  .cyan { color: var(--cyan); }
  .blue { color: var(--blue); }
  .violet { color: var(--violet); }
  .amber { color: var(--amber); }
  .green { color: var(--green); }
  .red { color: var(--red); }
  .mono { font-family: var(--mono); }

  .grid {
    display: grid;
    gap: 16px;
  }

  .grid.two { grid-template-columns: repeat(2, minmax(0, 1fr)); }
  .grid.three { grid-template-columns: repeat(3, minmax(0, 1fr)); }
  .grid.four { grid-template-columns: repeat(4, minmax(0, 1fr)); }
  .grid.wide-left { grid-template-columns: 1.18fr 0.82fr; }
  .grid.wide-right { grid-template-columns: 0.78fr 1.22fr; }

  .card {
    position: relative;
    min-width: 0;
    border: 1px solid var(--line);
    border-radius: 15px;
    padding: 18px 20px;
    background: linear-gradient(155deg, var(--panel), var(--panel-2));
    box-shadow: 0 13px 34px rgba(0, 0, 0, 0.16);
  }

  .card.compact { padding: 14px 17px; }
  .card.cyan-top { border-top: 3px solid var(--cyan); }
  .card.blue-top { border-top: 3px solid var(--blue); }
  .card.violet-top { border-top: 3px solid var(--violet); }
  .card.amber-top { border-top: 3px solid var(--amber); }
  .card.green-top { border-top: 3px solid var(--green); }
  .card.red-top { border-top: 3px solid var(--red); }

  .card p {
    margin: 0;
    font-size: 16px;
    line-height: 1.38;
  }

  .vocab-grid { align-items: stretch; }

  .vocab-card {
    min-height: 300px;
    padding: 16px 18px;
  }

  .vocab-card .eyebrow { margin-bottom: 7px; }

  .vocab-list > div {
    padding: 9px 0;
    border-top: 1px solid rgba(147, 185, 224, 0.12);
  }

  .vocab-list > div:first-child { border-top: 0; }

  .vocab-list b {
    display: block;
    margin-bottom: 3px;
    color: var(--ink);
    font-size: 14px;
  }

  .vocab-list span {
    display: block;
    color: var(--muted);
    font-size: 12.5px;
    line-height: 1.3;
  }

  .vocab-chain .flow-box {
    min-height: 68px;
    padding: 10px 12px;
  }

  .vocab-chain .flow-box b { font-size: 14px; }
  .vocab-chain .flow-box span {
    margin-top: 4px;
    font-size: 11.5px;
  }

  .vocab-notes {
    gap: 10px;
  }

  .vocab-note {
    min-height: 58px;
    border: 1px solid rgba(101, 230, 224, 0.24);
    border-left: 4px solid var(--cyan);
    border-radius: 11px;
    padding: 9px 12px;
    color: var(--muted);
    background: rgba(101, 230, 224, 0.055);
    font-size: 11px;
    line-height: 1.32;
  }

  .vocab-note.trust {
    border-color: rgba(120, 169, 255, 0.24);
    border-left-color: var(--blue);
    background: rgba(120, 169, 255, 0.055);
  }

  .vocab-note.trust.vocab-trust-note {
    overflow: hidden;
    padding: 0;
  }

  .vocab-note b {
    color: var(--ink);
  }

  .vocab-note.trust b {
    color: var(--blue);
  }

  .vocab-trust-grid {
    display: grid;
    min-height: 58px;
    grid-template-columns: 0.74fr 1.26fr;
  }

  .vocab-trust-item {
    display: flex;
    min-width: 0;
    flex-direction: column;
    justify-content: center;
    padding: 8px 11px;
  }

  .vocab-trust-item + .vocab-trust-item {
    border-left: 1px solid rgba(120, 169, 255, 0.18);
  }

  .vocab-trust-item b {
    display: block;
    margin-bottom: 3px;
    font-size: 9px;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .vocab-trust-item:first-child b {
    color: var(--cyan);
  }

  .vocab-trust-item span {
    display: block;
    color: var(--muted);
    font-size: 10px;
    line-height: 1.28;
  }

  .card ul {
    margin: 7px 0 0;
    padding-left: 19px;
  }

  .card li {
    margin: 4px 0;
    font-size: 15px;
    line-height: 1.35;
  }

  .callout {
    border: 1px solid rgba(101, 230, 224, 0.28);
    border-left: 5px solid var(--cyan);
    border-radius: 12px;
    padding: 13px 17px;
    color: #dcecf7;
    background: rgba(101, 230, 224, 0.07);
    font-size: 20px;
    line-height: 1.35;
  }

  .callout.warn {
    border-color: rgba(255, 199, 106, 0.25);
    border-left-color: var(--amber);
    background: rgba(255, 199, 106, 0.07);
  }

  .callout.redline {
    border-color: rgba(255, 130, 149, 0.24);
    border-left-color: var(--red);
    background: rgba(255, 130, 149, 0.07);
  }

  .pill {
    display: inline-flex;
    align-items: center;
    gap: 6px;
    border: 1px solid var(--line);
    border-radius: 999px;
    padding: 5px 10px;
    color: #dce8f6;
    background: rgba(255, 255, 255, 0.035);
    font-size: 12px;
    font-weight: 750;
    letter-spacing: 0.02em;
  }

  .pill.cyan-pill { border-color: rgba(101, 230, 224, 0.34); color: var(--cyan); }
  .pill.blue-pill { border-color: rgba(120, 169, 255, 0.34); color: var(--blue); }
  .pill.amber-pill { border-color: rgba(255, 199, 106, 0.34); color: var(--amber); }
  .pill.green-pill { border-color: rgba(120, 232, 167, 0.34); color: var(--green); }
  .pill.red-pill { border-color: rgba(255, 130, 149, 0.34); color: var(--red); }

  .metric {
    color: var(--ink);
    font-size: 40px;
    font-weight: 800;
    letter-spacing: -0.045em;
    line-height: 1;
  }

  .metric-label {
    margin-top: 8px;
    color: var(--muted);
    font-size: 14px;
    line-height: 1.28;
  }

  .pipeline {
    display: flex;
    align-items: stretch;
    gap: 8px;
    width: 100%;
  }

  .pipeline .node {
    display: flex;
    flex: 1 1 0;
    min-width: 0;
    min-height: 96px;
    flex-direction: column;
    justify-content: center;
    border: 1px solid var(--line);
    border-radius: 13px;
    padding: 12px 13px;
    background: rgba(11, 27, 47, 0.88);
    text-align: center;
  }

  .pipeline .node b {
    color: var(--ink);
    font-size: 15px;
    line-height: 1.18;
  }

  .pipeline .node span {
    margin-top: 6px;
    color: var(--muted);
    font-size: 11px;
    line-height: 1.22;
  }

  .pipeline .arrow {
    display: flex;
    flex: 0 0 22px;
    align-items: center;
    justify-content: center;
    color: var(--cyan);
    font-size: 22px;
    font-weight: 800;
  }

  .ingest-scopebar {
    display: flex;
    height: 42px;
    align-items: stretch;
    gap: 8px;
    margin-bottom: 13px;
  }

  .ingest-scope-node {
    display: flex;
    flex: 1 1 0;
    align-items: center;
    justify-content: center;
    border: 1px solid var(--line);
    border-radius: 10px;
    color: var(--muted);
    background: rgba(8, 22, 39, 0.82);
    font-size: 11px;
  }

  .ingest-scope-node b {
    margin-right: 6px;
    color: var(--ink);
    font-size: 11px;
  }

  .ingest-scope-arrow {
    display: flex;
    flex: 0 0 18px;
    align-items: center;
    justify-content: center;
    color: var(--cyan);
    font-size: 16px;
  }

  .ingest-grid {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 12px;
  }

  .ingest-stage {
    min-height: 120px;
    border: 1px solid var(--line);
    border-top-width: 3px;
    border-radius: 13px;
    padding: 13px 15px;
    background: linear-gradient(155deg, var(--panel), var(--panel-2));
    box-shadow: 0 11px 28px rgba(0, 0, 0, 0.14);
  }

  .ingest-stage:nth-child(1),
  .ingest-stage:nth-child(4) { border-top-color: var(--cyan); }
  .ingest-stage:nth-child(2),
  .ingest-stage:nth-child(5) { border-top-color: var(--blue); }
  .ingest-stage:nth-child(3),
  .ingest-stage:nth-child(6) { border-top-color: var(--violet); }

  .ingest-stage-head {
    display: flex;
    align-items: center;
    gap: 9px;
  }

  .ingest-step {
    display: flex;
    width: 31px;
    height: 25px;
    flex: 0 0 31px;
    align-items: center;
    justify-content: center;
    border: 1px solid rgba(101, 230, 224, 0.28);
    border-radius: 8px;
    color: var(--cyan);
    background: rgba(101, 230, 224, 0.075);
    font-family: var(--mono);
    font-size: 10px;
    font-weight: 700;
  }

  .ingest-stage h3 {
    margin: 0;
    color: var(--ink);
    font-size: 15px;
  }

  .ingest-stage p {
    margin: 9px 0 0;
    color: var(--muted);
    font-size: 11.5px;
    line-height: 1.35;
  }

  .ingest-notes {
    display: grid;
    grid-template-columns: 0.85fr 1fr 1.35fr;
    gap: 12px;
    margin-top: 14px;
  }

  .ingest-note {
    min-height: 82px;
    border: 1px solid var(--line);
    border-radius: 11px;
    padding: 11px 13px;
    background: rgba(8, 22, 39, 0.86);
  }

  .ingest-note b {
    display: block;
    margin-bottom: 5px;
    color: var(--amber);
    font-size: 12px;
  }

  .ingest-note span {
    display: block;
    color: var(--muted);
    font-size: 10.5px;
    line-height: 1.3;
  }

  .ingest-note.reliability {
    border-left: 4px solid var(--amber);
    background: rgba(255, 199, 106, 0.07);
  }

  .ingest-note.reliability b { color: var(--ink); }

  .mutation-flow {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 28px;
  }

  .mutation-phase {
    position: relative;
    display: flex;
    height: 326px;
    flex-direction: column;
    border: 1px solid var(--line);
    border-top: 3px solid var(--cyan);
    border-radius: 14px;
    padding: 15px 16px 14px;
    background: linear-gradient(155deg, var(--panel), var(--panel-2));
    box-shadow: 0 13px 32px rgba(0, 0, 0, 0.15);
  }

  .mutation-phase:nth-child(2) { border-top-color: var(--blue); }
  .mutation-phase:nth-child(3) { border-top-color: var(--violet); }

  .mutation-phase:not(:last-child)::after {
    position: absolute;
    z-index: 2;
    top: 151px;
    right: -23px;
    width: 18px;
    color: var(--cyan);
    content: "→";
    font-size: 16px;
    font-weight: 800;
    text-align: center;
  }

  .mutation-phase-head {
    display: flex;
    align-items: center;
    gap: 9px;
  }

  .mutation-phase-no {
    border: 1px solid rgba(101, 230, 224, 0.30);
    border-radius: 7px;
    padding: 4px 7px;
    color: var(--cyan);
    background: rgba(101, 230, 224, 0.075);
    font-family: var(--mono);
    font-size: 9px;
    font-weight: 700;
    letter-spacing: 0.06em;
  }

  .mutation-phase h3 {
    margin: 0;
    color: var(--ink);
    font-size: 16px;
  }

  .phase-code {
    display: flex;
    height: 212px;
    min-height: 0;
    flex: 0 0 212px;
    flex-direction: column;
    margin: 12px 0 10px;
    overflow: hidden;
    border: 1px solid rgba(120, 169, 255, 0.22);
    border-radius: 10px;
    padding: 12px 13px;
    color: #d8e8ff;
    background: rgba(4, 13, 25, 0.88);
    font-family: var(--mono);
    font-size: 12.5px;
    font-variant-ligatures: none;
    line-height: 1.43;
    letter-spacing: -0.018em;
  }

  .code-line {
    flex: 0 0 auto;
    min-height: 18px;
    white-space: nowrap;
  }

  .code-line.indent { padding-left: 24px; }
  .code-gap { flex: 0 0 9px; }

  .tok-kw { color: var(--violet); font-weight: 700; }
  .tok-fn { color: var(--cyan); }
  .tok-num { color: var(--amber); }

  .phase-effect {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 8px;
    align-items: center;
    margin-top: auto;
  }

  .phase-effect b {
    border-radius: 999px;
    padding: 4px 7px;
    color: var(--green);
    background: rgba(120, 232, 167, 0.09);
    font-size: 8px;
    letter-spacing: 0.07em;
    text-transform: uppercase;
  }

  .phase-effect span {
    color: var(--muted);
    font-size: 9px;
    line-height: 1.22;
  }

  .mutation-notes {
    display: grid;
    grid-template-columns: 1.25fr 1.15fr 0.9fr;
    gap: 12px;
    margin-top: 14px;
  }

  .mutation-note {
    min-height: 88px;
    border: 1px solid var(--line);
    border-left: 4px solid var(--blue);
    border-radius: 11px;
    padding: 11px 13px;
    background: rgba(8, 22, 39, 0.86);
  }

  .mutation-note.invariant { border-left-color: var(--red); }
  .mutation-note.shutdown { border-left-color: var(--violet); }

  .mutation-note b {
    display: block;
    margin-bottom: 5px;
    color: var(--ink);
    font-size: 11px;
  }

  .mutation-note span {
    display: block;
    color: var(--muted);
    font-size: 9.5px;
    line-height: 1.3;
  }

  .codebox {
    border: 1px solid rgba(120, 169, 255, 0.22);
    border-radius: 13px;
    padding: 15px 17px;
    color: #d8e8ff;
    background: rgba(5, 15, 28, 0.82);
    font-family: var(--mono);
    font-size: 14px;
    line-height: 1.55;
    white-space: pre-wrap;
  }

  .chunk-anatomy {
    display: flex;
    min-height: 103px;
    align-items: stretch;
    gap: 8px;
  }

  .chunk-anatomy-node {
    display: flex;
    min-width: 0;
    flex: 1 1 0;
    flex-direction: column;
    justify-content: center;
    border: 1px solid var(--line);
    border-top: 3px solid var(--blue);
    border-radius: 12px;
    padding: 10px 12px;
    background: linear-gradient(155deg, rgba(14, 30, 52, 0.92), rgba(8, 22, 39, 0.82));
  }

  .chunk-anatomy-node.frame { flex: 0.72 1 0; border-top-color: var(--faint); }
  .chunk-anatomy-node.header { flex: 1.48 1 0; border-top-color: var(--cyan); }
  .chunk-anatomy-node.scalar { flex: 1.02 1 0; border-top-color: var(--blue); }
  .chunk-anatomy-node.native { flex: 1.05 1 0; border-top-color: var(--violet); }

  .chunk-anatomy-kicker {
    margin-bottom: 5px;
    color: var(--faint);
    font-size: 7.5px;
    font-weight: 800;
    letter-spacing: 0.09em;
    text-transform: uppercase;
  }

  .chunk-anatomy-node b {
    color: var(--ink);
    font-family: var(--mono);
    font-size: 12px;
    line-height: 1.2;
  }

  .chunk-anatomy-node > span:last-child {
    margin-top: 5px;
    color: var(--muted);
    font-size: 8px;
    line-height: 1.25;
  }

  .chunk-anatomy-arrow {
    display: flex;
    flex: 0 0 18px;
    align-items: center;
    justify-content: center;
    color: var(--cyan);
    font-size: 18px;
    font-weight: 850;
  }

  .chunk-header-groups {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 4px;
    margin-top: 7px;
  }

  .chunk-header-groups span {
    overflow: hidden;
    border: 1px solid rgba(101, 230, 224, 0.14);
    border-radius: 6px;
    padding: 3px 5px;
    color: var(--muted);
    background: rgba(101, 230, 224, 0.04);
    font-family: var(--mono);
    font-size: 6.8px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .chunk-payload-grid {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 10px;
    margin-top: 13px;
  }

  .chunk-payload-card {
    min-width: 0;
    min-height: 217px;
    border: 1px solid var(--line);
    border-top: 3px solid var(--cyan);
    border-radius: 13px;
    padding: 13px 14px 11px;
    background: linear-gradient(155deg, rgba(14, 30, 52, 0.92), rgba(8, 22, 39, 0.82));
  }

  .chunk-payload-card.number { border-top-color: var(--amber); }
  .chunk-payload-card.hist { border-top-color: var(--cyan); }
  .chunk-payload-card.exphist { border-top-color: var(--violet); }
  .chunk-payload-card.summary { border-top-color: var(--blue); }

  .chunk-payload-kind {
    display: inline-block;
    border: 1px solid rgba(147, 185, 224, 0.22);
    border-radius: 999px;
    padding: 4px 7px;
    color: var(--muted);
    background: rgba(120, 169, 255, 0.055);
    font-family: var(--mono);
    font-size: 7.5px;
    font-weight: 800;
  }

  .chunk-payload-card h3 {
    margin: 10px 0 7px;
    font-size: 17px;
  }

  .chunk-payload-shape > div {
    border-top: 1px solid rgba(147, 185, 224, 0.11);
    padding: 7px 0;
  }

  .chunk-payload-shape > div:first-child {
    border-top: 0;
    padding-top: 0;
  }

  .chunk-payload-shape b {
    display: block;
    margin-bottom: 3px;
    color: var(--ink);
    font-size: 8.5px;
  }

  .chunk-payload-shape span {
    display: block;
    color: var(--muted);
    font-size: 9px;
    line-height: 1.28;
  }

  .chunk-number-gap {
    margin-top: 5px;
    border-left: 3px solid var(--amber);
    border-radius: 6px;
    padding: 6px 7px;
    color: #d9c59e;
    background: rgba(255, 199, 106, 0.055);
    font-size: 7.8px;
    line-height: 1.25;
  }

  .chunk-semantics-bottom {
    display: grid;
    grid-template-columns: 1.26fr 0.74fr;
    gap: 10px;
    margin-top: 13px;
  }

  .chunk-semantics-bottom > div {
    display: flex;
    min-width: 0;
    min-height: 57px;
    align-items: center;
    gap: 12px;
    border: 1px solid rgba(101, 230, 224, 0.22);
    border-left: 4px solid var(--cyan);
    border-radius: 11px;
    padding: 9px 12px;
    background: rgba(101, 230, 224, 0.055);
  }

  .chunk-semantics-bottom > div.stale {
    border-color: rgba(120, 169, 255, 0.22);
    border-left-color: var(--blue);
    background: rgba(120, 169, 255, 0.055);
  }

  .chunk-semantics-bottom b {
    flex: 0 0 auto;
    color: var(--cyan);
    font-size: 9px;
  }

  .chunk-semantics-bottom .stale b { color: var(--blue); }

  .chunk-semantics-bottom span {
    min-width: 0;
    color: var(--muted);
    font-size: 9px;
    line-height: 1.3;
  }

  .matcher-example-bar {
    display: flex;
    min-height: 52px;
    align-items: center;
    gap: 9px;
    border: 1px solid rgba(147, 185, 224, 0.18);
    border-radius: 12px;
    padding: 8px 12px;
    background: rgba(8, 22, 39, 0.76);
  }

  .matcher-example-label {
    margin-right: 3px;
    color: var(--faint);
    font-size: 8px;
    font-weight: 800;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }

  .matcher-selector-brace {
    color: var(--muted);
    font-family: var(--mono);
    font-size: 14px;
    font-weight: 700;
  }

  .matcher-token {
    display: flex;
    min-width: 0;
    align-items: center;
    gap: 7px;
    border: 1px solid rgba(101, 230, 224, 0.24);
    border-radius: 8px;
    padding: 6px 9px;
    color: var(--cyan);
    background: rgba(101, 230, 224, 0.055);
    font-family: var(--mono);
    font-size: 9px;
    font-weight: 750;
  }

  .matcher-token.regex {
    border-color: rgba(120, 169, 255, 0.24);
    color: var(--blue);
    background: rgba(120, 169, 255, 0.055);
  }

  .matcher-token.negative {
    border-color: rgba(255, 199, 106, 0.24);
    color: var(--amber);
    background: rgba(255, 199, 106, 0.055);
  }

  .matcher-token span {
    color: var(--faint);
    font-family: Inter, ui-sans-serif, system-ui, sans-serif;
    font-size: 7px;
    font-weight: 550;
  }

  .matcher-explain-grid {
    display: grid;
    grid-template-columns: 0.82fr 1.18fr;
    gap: 11px;
    margin-top: 12px;
  }

  .matcher-explain-card {
    min-width: 0;
    min-height: 260px;
    border: 1px solid var(--line);
    border-top: 3px solid var(--cyan);
    border-radius: 13px;
    padding: 13px 14px;
    background: linear-gradient(155deg, rgba(14, 30, 52, 0.92), rgba(8, 22, 39, 0.82));
  }

  .matcher-explain-card.regex {
    border-top-color: var(--blue);
  }

  .matcher-explain-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 11px;
  }

  .matcher-explain-head b {
    color: var(--ink);
    font-size: 14px;
  }

  .matcher-explain-head span {
    color: var(--faint);
    font-size: 7.5px;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .matcher-exact-path {
    display: grid;
    grid-template-columns: 0.86fr 22px 1.14fr;
    align-items: stretch;
    gap: 7px;
  }

  .matcher-query-node,
  .matcher-posting-node {
    display: flex;
    min-width: 0;
    min-height: 102px;
    flex-direction: column;
    justify-content: center;
    border: 1px solid rgba(147, 185, 224, 0.17);
    border-radius: 10px;
    padding: 9px 10px;
    background: rgba(7, 19, 34, 0.72);
  }

  .matcher-query-node code,
  .matcher-posting-node code {
    overflow: hidden;
    border: 0;
    padding: 0;
    color: #dce9f8;
    background: transparent;
    font-size: 9px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .matcher-query-node span,
  .matcher-posting-node span {
    margin-top: 6px;
    color: var(--faint);
    font-size: 8px;
    line-height: 1.25;
  }

  .matcher-posting-node b {
    margin-bottom: 7px;
    color: var(--cyan);
    font-family: var(--mono);
    font-size: 9px;
  }

  .matcher-path-arrow {
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--cyan);
    font-size: 18px;
    font-weight: 850;
  }

  .matcher-posting-definition {
    margin-top: 10px;
    border-left: 3px solid var(--cyan);
    border-radius: 7px;
    padding: 8px 10px;
    color: var(--muted);
    background: rgba(101, 230, 224, 0.045);
    font-size: 9.5px;
    line-height: 1.3;
  }

  .matcher-posting-definition b {
    color: var(--cyan);
  }

  .matcher-fst-flow {
    display: grid;
    grid-template-columns: 0.62fr 20px 1.38fr;
    align-items: stretch;
    gap: 7px;
  }

  .matcher-fst {
    min-width: 0;
    border: 1px solid rgba(120, 169, 255, 0.22);
    border-radius: 10px;
    padding: 8px 9px;
    background: rgba(120, 169, 255, 0.045);
  }

  .matcher-fst-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 8px;
  }

  .matcher-fst-head b {
    color: var(--blue);
    font-family: var(--mono);
    font-size: 9px;
  }

  .matcher-fst-head span {
    color: var(--faint);
    font-size: 7px;
  }

  .matcher-fst-prefix {
    display: flex;
    align-items: center;
    gap: 6px;
    margin-top: 7px;
  }

  .matcher-fst-prefix > b {
    border: 1px solid rgba(120, 169, 255, 0.28);
    border-radius: 7px;
    padding: 5px 7px;
    color: var(--blue);
    background: rgba(120, 169, 255, 0.08);
    font-family: var(--mono);
    font-size: 9px;
  }

  .matcher-fst-branches {
    display: flex;
    min-width: 0;
    flex-wrap: wrap;
    gap: 4px;
  }

  .matcher-fst-branches span {
    border: 1px solid rgba(101, 230, 224, 0.18);
    border-radius: 6px;
    padding: 4px 6px;
    color: var(--cyan);
    background: rgba(101, 230, 224, 0.045);
    font-family: var(--mono);
    font-size: 7.5px;
  }

  .matcher-fst-misses {
    margin-top: 6px;
    color: var(--faint);
    font-family: var(--mono);
    font-size: 7px;
  }

  .matcher-regex-postings {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 6px;
    margin-top: 8px;
  }

  .matcher-regex-postings > div {
    min-width: 0;
    border: 1px solid rgba(147, 185, 224, 0.14);
    border-radius: 8px;
    padding: 6px 7px;
    background: rgba(7, 19, 34, 0.64);
  }

  .matcher-regex-postings b,
  .matcher-regex-postings span {
    display: block;
    overflow: hidden;
    font-family: var(--mono);
    font-size: 7.5px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .matcher-regex-postings b { color: var(--blue); }
  .matcher-regex-postings span {
    margin-top: 3px;
    color: var(--muted);
  }

  .matcher-union {
    margin-top: 7px;
    border-radius: 7px;
    padding: 6px 8px;
    color: var(--muted);
    background: rgba(185, 149, 255, 0.065);
    font-family: var(--mono);
    font-size: 8px;
    text-align: center;
  }

  .matcher-union b { color: var(--violet); }

  .matcher-set-flow {
    display: flex;
    min-height: 67px;
    align-items: stretch;
    gap: 6px;
    margin-top: 12px;
  }

  .matcher-set-box {
    display: flex;
    min-width: 0;
    flex: 1 1 0;
    flex-direction: column;
    justify-content: center;
    border: 1px solid var(--line);
    border-radius: 9px;
    padding: 7px 9px;
    background: rgba(8, 22, 39, 0.75);
    text-align: center;
  }

  .matcher-set-box.negative { flex: 1.42 1 0; }
  .matcher-set-box.final {
    flex: 0.8 1 0;
    border-color: rgba(120, 232, 167, 0.28);
    background: rgba(120, 232, 167, 0.05);
  }

  .matcher-set-box b {
    color: var(--ink);
    font-size: 8.5px;
  }

  .matcher-set-box > code {
    display: block;
    overflow: hidden;
    margin-top: 4px;
    border: 0;
    padding: 0;
    color: var(--cyan);
    background: transparent;
    font-size: 8px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .matcher-set-box span {
    display: block;
    margin-top: 4px;
    color: var(--faint);
    font-size: 7.5px;
    line-height: 1.2;
  }

  .matcher-set-box.final > code { color: var(--green); }

  .matcher-set-op {
    display: flex;
    flex: 0 0 20px;
    align-items: center;
    justify-content: center;
    color: var(--cyan);
    font-size: 16px;
    font-weight: 850;
  }

  .matcher-regex-guardrail {
    display: flex;
    min-height: 43px;
    align-items: center;
    gap: 10px;
    margin-top: 9px;
    border: 1px solid rgba(255, 199, 106, 0.22);
    border-left: 4px solid var(--amber);
    border-radius: 10px;
    padding: 7px 11px;
    color: var(--muted);
    background: rgba(255, 199, 106, 0.05);
    font-size: 9px;
    line-height: 1.25;
  }

  .matcher-regex-guardrail b {
    flex: 0 0 auto;
    color: var(--amber);
    font-size: 9px;
  }

  .postings-deep-grid {
    display: grid;
    grid-template-columns: 1fr 72px 1fr;
    gap: 10px;
    align-items: stretch;
  }

  .postings-deep-panel {
    min-width: 0;
    min-height: 151px;
    border: 1px solid var(--line);
    border-top: 3px solid var(--cyan);
    border-radius: 13px;
    padding: 11px 13px;
    background: linear-gradient(155deg, rgba(14, 30, 52, 0.92), rgba(8, 22, 39, 0.82));
  }

  .postings-deep-panel.inverted { border-top-color: var(--blue); }

  .postings-deep-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 8px;
  }

  .postings-deep-head b {
    color: var(--ink);
    font-size: 13px;
  }

  .postings-deep-head span {
    color: var(--faint);
    font-size: 7.5px;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .postings-deep-row {
    display: grid;
    grid-template-columns: 48px minmax(0, 1fr);
    gap: 8px;
    align-items: center;
    border-top: 1px solid rgba(147, 185, 224, 0.11);
    padding: 5px 0;
  }

  .postings-deep-panel.inverted .postings-deep-row {
    grid-template-columns: minmax(0, 1.42fr) minmax(0, 0.78fr);
  }

  .postings-deep-row:first-of-type { border-top: 0; }

  .postings-deep-row code {
    min-width: 0;
    overflow: hidden;
    border: 0;
    padding: 0;
    color: var(--cyan);
    background: transparent;
    font-size: 8.5px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .postings-deep-panel.inverted .postings-deep-row code:first-child {
    color: var(--blue);
  }

  .postings-deep-row span,
  .postings-deep-row code:last-child {
    min-width: 0;
    overflow: hidden;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 8.5px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .postings-invert {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 4px;
    color: var(--cyan);
    text-align: center;
  }

  .postings-invert b {
    font-size: 25px;
    line-height: 1;
  }

  .postings-invert span {
    color: var(--faint);
    font-size: 7.5px;
    font-weight: 800;
    letter-spacing: 0.08em;
    line-height: 1.2;
    text-transform: uppercase;
  }

  .postings-query-strip {
    display: grid;
    grid-template-columns: 1.1fr 30px 1.16fr 30px 1fr;
    gap: 7px;
    min-height: 68px;
    align-items: stretch;
    margin-top: 11px;
    border: 1px solid rgba(101, 230, 224, 0.21);
    border-radius: 11px;
    padding: 8px 10px;
    background: rgba(101, 230, 224, 0.045);
  }

  .postings-query-node {
    display: flex;
    min-width: 0;
    flex-direction: column;
    justify-content: center;
    border: 1px solid rgba(147, 185, 224, 0.14);
    border-radius: 8px;
    padding: 6px 9px;
    background: rgba(7, 19, 34, 0.67);
  }

  .postings-query-node b {
    color: var(--ink);
    font-size: 9px;
  }

  .postings-query-node code {
    overflow: hidden;
    margin-top: 4px;
    border: 0;
    padding: 0;
    color: var(--cyan);
    background: transparent;
    font-size: 8.5px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .postings-query-node span {
    margin-top: 3px;
    color: var(--faint);
    font-size: 7.5px;
    line-height: 1.2;
  }

  .postings-query-node.result {
    border-color: rgba(120, 232, 167, 0.22);
    background: rgba(120, 232, 167, 0.045);
  }

  .postings-query-node.result code { color: var(--green); }

  .postings-query-arrow {
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--cyan);
    font-size: 18px;
    font-weight: 850;
  }

  .postings-deep-details {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 10px;
    margin-top: 11px;
  }

  .postings-deep-detail {
    min-width: 0;
    min-height: 95px;
    border: 1px solid var(--line);
    border-left: 4px solid var(--cyan);
    border-radius: 10px;
    padding: 9px 11px;
    background: rgba(8, 22, 39, 0.77);
  }

  .postings-deep-detail.sorted { border-left-color: var(--blue); }
  .postings-deep-detail.encoded { border-left-color: var(--amber); }

  .postings-deep-detail b {
    display: block;
    margin-bottom: 5px;
    color: var(--cyan);
    font-size: 10px;
  }

  .postings-deep-detail.sorted b { color: var(--blue); }
  .postings-deep-detail.encoded b { color: var(--amber); }

  .postings-deep-detail p {
    margin: 0;
    color: var(--muted);
    font-size: 8.5px;
    line-height: 1.3;
  }

  .postings-deep-detail code {
    border: 0;
    padding: 0;
    color: #dce9f8;
    background: transparent;
    font-size: 0.96em;
  }

  .postings-codec-example {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 3px 7px;
    align-items: baseline;
    margin-bottom: 5px;
    font-family: var(--mono);
    font-size: 7.5px;
  }

  .postings-codec-example span { color: var(--faint); }
  .postings-codec-example code { color: #dce9f8; }
  .postings-codec-example .delta { color: var(--amber); }

  .projection-map {
    display: grid;
    grid-template-columns: 0.98fr 80px 1.12fr;
    gap: 10px;
    height: 338px;
    align-items: stretch;
  }

  .projection-native-card,
  .projection-output-card {
    min-width: 0;
    overflow: hidden;
    border: 1px solid var(--line);
    border-top: 3px solid var(--violet);
    border-radius: 13px;
    padding: 12px 14px;
    background: linear-gradient(155deg, rgba(14, 30, 52, 0.94), rgba(8, 22, 39, 0.84));
  }

  .projection-card-kicker {
    color: var(--violet);
    font-size: 8px;
    font-weight: 850;
    letter-spacing: 0.13em;
    text-transform: uppercase;
  }

  .projection-metric-line {
    display: flex;
    min-width: 0;
    align-items: center;
    gap: 8px;
    margin-top: 6px;
  }

  .projection-metric-line code {
    overflow: hidden;
    border: 0;
    padding: 0;
    color: #dce9f8;
    background: transparent;
    font-size: 18px;
    font-weight: 750;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .projection-metric-line span {
    flex: 0 0 auto;
    color: var(--faint);
    font-size: 7px;
    font-weight: 750;
    letter-spacing: 0.07em;
    text-transform: uppercase;
  }

  .projection-metric-line code.normalized {
    color: var(--cyan);
    font-size: 10px;
  }

  .projection-metadata {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 6px;
    margin-top: 10px;
  }

  .projection-metadata > div {
    min-width: 0;
    border: 1px solid rgba(147, 185, 224, 0.14);
    border-radius: 8px;
    padding: 6px 7px;
    background: rgba(7, 19, 34, 0.66);
  }

  .projection-metadata span,
  .projection-metadata b {
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .projection-metadata span {
    color: var(--faint);
    font-size: 6.5px;
    font-weight: 800;
    letter-spacing: 0.07em;
    text-transform: uppercase;
  }

  .projection-metadata b {
    margin-top: 3px;
    color: var(--ink);
    font-family: var(--mono);
    font-size: 7.5px;
  }

  .projection-aggregates {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 7px;
    margin-top: 8px;
  }

  .projection-aggregate {
    display: flex;
    min-width: 0;
    align-items: baseline;
    justify-content: space-between;
    border: 1px solid rgba(185, 149, 255, 0.18);
    border-radius: 8px;
    padding: 6px 8px;
    background: rgba(185, 149, 255, 0.055);
  }

  .projection-aggregate span {
    color: var(--muted);
    font-size: 7.5px;
  }

  .projection-aggregate b {
    color: var(--ink);
    font-family: var(--mono);
    font-size: 11px;
  }

  .projection-aggregate small {
    color: var(--faint);
    font-size: 6.5px;
  }

  .projection-native-buckets {
    overflow: hidden;
    margin-top: 8px;
    border: 1px solid rgba(147, 185, 224, 0.15);
    border-radius: 9px;
    background: rgba(7, 19, 34, 0.62);
  }

  .projection-native-bucket-row {
    display: grid;
    grid-template-columns: minmax(0, 1fr) 72px;
    border-top: 1px solid rgba(147, 185, 224, 0.10);
  }

  .projection-native-bucket-row:first-child {
    border-top: 0;
    background: rgba(185, 149, 255, 0.065);
  }

  .projection-native-bucket-row span,
  .projection-native-bucket-row b {
    padding: 5px 9px;
    font-size: 7.5px;
  }

  .projection-native-bucket-row span {
    color: var(--muted);
    font-family: var(--mono);
  }

  .projection-native-bucket-row b {
    border-left: 1px solid rgba(147, 185, 224, 0.10);
    color: var(--ink);
    font-family: var(--mono);
    text-align: center;
  }

  .projection-native-bucket-row:first-child span,
  .projection-native-bucket-row:first-child b {
    color: var(--violet);
    font-family: Inter, ui-sans-serif, system-ui, sans-serif;
    font-size: 6.5px;
    font-weight: 850;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .projection-native-checks {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 6px;
    margin-top: 8px;
  }

  .projection-native-check {
    min-width: 0;
    border-left: 3px solid var(--violet);
    border-radius: 7px;
    padding: 6px 7px;
    background: rgba(185, 149, 255, 0.045);
  }

  .projection-native-check b,
  .projection-native-check code,
  .projection-native-check span {
    display: block;
    overflow: hidden;
    text-overflow: ellipsis;
  }

  .projection-native-check b {
    color: var(--violet);
    font-size: 6.5px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }

  .projection-native-check code,
  .projection-native-check span {
    margin-top: 3px;
    border: 0;
    padding: 0;
    color: var(--muted);
    background: transparent;
    font-size: 6.8px;
    line-height: 1.2;
  }

  .projection-fanout {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    color: var(--cyan);
    text-align: center;
  }

  .projection-fanout span {
    color: var(--faint);
    font-size: 7px;
    font-weight: 800;
    letter-spacing: 0.09em;
    text-transform: uppercase;
  }

  .projection-fanout b {
    margin-top: 2px;
    color: var(--cyan);
    font-size: 10px;
  }

  .projection-fanout-arrows {
    display: flex;
    flex-direction: column;
    gap: 50px;
    margin: 10px 0;
    color: var(--cyan);
    font-size: 24px;
    font-weight: 850;
    line-height: 1;
  }

  .projection-fanout small {
    color: var(--faint);
    font-size: 6.5px;
    line-height: 1.2;
  }

  .projection-output-stack {
    display: grid;
    grid-template-rows: 0.86fr 1.14fr;
    gap: 10px;
    min-width: 0;
  }

  .projection-output-card.native {
    border-top-color: var(--cyan);
  }

  .projection-output-card.classic {
    border-top-color: var(--blue);
  }

  .projection-output-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 10px;
  }

  .projection-output-head b {
    color: var(--ink);
    font-size: 12px;
  }

  .projection-output-head span {
    color: var(--faint);
    font-size: 6.5px;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .projection-native-functions {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 6px;
    margin-top: 9px;
  }

  .projection-native-function {
    min-width: 0;
    border: 1px solid rgba(101, 230, 224, 0.16);
    border-radius: 8px;
    padding: 6px 7px;
    background: rgba(101, 230, 224, 0.045);
  }

  .projection-native-function code,
  .projection-native-function b {
    display: block;
    overflow: hidden;
    border: 0;
    padding: 0;
    background: transparent;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .projection-native-function code {
    color: var(--cyan);
    font-size: 7px;
  }

  .projection-native-function b {
    margin-top: 4px;
    color: var(--ink);
    font-family: var(--mono);
    font-size: 10px;
  }

  .projection-native-shape {
    margin-top: 7px;
    border-left: 3px solid var(--cyan);
    border-radius: 6px;
    padding: 5px 8px;
    color: var(--muted);
    background: rgba(101, 230, 224, 0.04);
    font-size: 7.5px;
    line-height: 1.25;
  }

  .projection-native-shape code {
    border: 0;
    padding: 0;
    color: var(--cyan);
    background: transparent;
    font-size: 1em;
  }

  .projection-classic-scalars {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 6px;
    margin-top: 8px;
  }

  .projection-classic-scalar {
    display: grid;
    grid-template-columns: minmax(0, 1fr) auto;
    gap: 7px;
    align-items: center;
    border: 1px solid rgba(120, 169, 255, 0.16);
    border-radius: 7px;
    padding: 5px 7px;
    background: rgba(120, 169, 255, 0.04);
  }

  .projection-classic-scalar code {
    overflow: hidden;
    border: 0;
    padding: 0;
    color: var(--blue);
    background: transparent;
    font-size: 6.8px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .projection-classic-scalar b {
    color: var(--ink);
    font-family: var(--mono);
    font-size: 8.5px;
  }

  .projection-prefix-line {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 7px;
    margin-top: 7px;
    color: var(--faint);
    font-family: var(--mono);
    font-size: 7px;
  }

  .projection-prefix-line b {
    color: var(--blue);
    font-family: Inter, ui-sans-serif, system-ui, sans-serif;
    font-size: 7px;
  }

  .projection-classic-buckets {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 5px;
    margin-top: 6px;
  }

  .projection-classic-bucket {
    min-width: 0;
    border: 1px solid rgba(120, 169, 255, 0.16);
    border-radius: 7px;
    padding: 5px 6px;
    background: rgba(120, 169, 255, 0.04);
    text-align: center;
  }

  .projection-classic-bucket.inf {
    border-color: rgba(120, 232, 167, 0.24);
    background: rgba(120, 232, 167, 0.045);
  }

  .projection-classic-bucket code,
  .projection-classic-bucket b {
    display: block;
    overflow: hidden;
    border: 0;
    padding: 0;
    background: transparent;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .projection-classic-bucket code {
    color: var(--blue);
    font-size: 6.7px;
  }

  .projection-classic-bucket b {
    margin-top: 3px;
    color: var(--ink);
    font-family: var(--mono);
    font-size: 9px;
  }

  .projection-classic-bucket.inf code,
  .projection-classic-bucket.inf b {
    color: var(--green);
  }

  .projection-classic-note {
    margin-top: 6px;
    color: var(--faint);
    font-size: 6.5px;
    text-align: center;
  }

  .projection-contract {
    display: grid;
    grid-template-columns: 0.72fr 26px 1.28fr;
    gap: 8px;
    min-height: 58px;
    align-items: stretch;
    margin-top: 12px;
    border: 1px solid rgba(101, 230, 224, 0.22);
    border-left: 4px solid var(--cyan);
    border-radius: 11px;
    padding: 8px 11px;
    background: rgba(101, 230, 224, 0.05);
  }

  .projection-contract > div:not(.projection-contract-arrow) {
    display: flex;
    min-width: 0;
    flex-direction: column;
    justify-content: center;
  }

  .projection-contract b {
    color: var(--ink);
    font-size: 9.5px;
  }

  .projection-contract span {
    margin-top: 3px;
    color: var(--muted);
    font-size: 7.8px;
    line-height: 1.25;
  }

  .projection-contract-arrow {
    display: flex;
    align-items: center;
    justify-content: center;
    color: var(--cyan);
    font-size: 17px;
    font-weight: 850;
  }

  .delta-gate {
    display: grid;
    grid-template-columns: 1fr auto 1fr;
    gap: 9px;
    min-height: 48px;
    align-items: stretch;
  }

  .delta-gate-side {
    display: flex;
    min-width: 0;
    align-items: center;
    gap: 9px;
    border: 1px solid rgba(101, 230, 224, 0.18);
    border-radius: 10px;
    padding: 7px 11px;
    background: rgba(101, 230, 224, 0.045);
  }

  .delta-gate-side.stale {
    border-color: rgba(255, 199, 106, 0.20);
    background: rgba(255, 199, 106, 0.045);
  }

  .delta-gate-side b {
    flex: 0 0 auto;
    color: var(--cyan);
    font-size: 8px;
  }

  .delta-gate-side.stale b { color: var(--amber); }

  .delta-gate-side span {
    min-width: 0;
    color: var(--muted);
    font-size: 7.5px;
    line-height: 1.2;
  }

  .delta-gate-condition {
    display: flex;
    align-items: center;
    justify-content: center;
    border: 1px solid rgba(120, 169, 255, 0.22);
    border-radius: 10px;
    padding: 7px 13px;
    background: rgba(120, 169, 255, 0.055);
  }

  .delta-gate-condition code {
    border: 0;
    padding: 0;
    color: #dce9f8;
    background: transparent;
    font-size: 9px;
    font-weight: 750;
    white-space: nowrap;
  }

  .delta-domain-map {
    overflow: hidden;
    margin-top: 10px;
    border: 1px solid var(--line);
    border-radius: 13px;
    background: rgba(6, 17, 31, 0.78);
  }

  .delta-domain-axis {
    display: grid;
    grid-template-columns: 184px repeat(5, minmax(0, 1fr));
    min-height: 30px;
    align-items: center;
    border-bottom: 1px solid rgba(147, 185, 224, 0.13);
    color: var(--faint);
    background: rgba(120, 169, 255, 0.035);
    font-family: var(--mono);
    font-size: 7px;
    text-align: center;
  }

  .delta-domain-axis b {
    padding-left: 13px;
    color: var(--muted);
    font-family: Inter, ui-sans-serif, system-ui, sans-serif;
    font-size: 7px;
    letter-spacing: 0.08em;
    text-align: left;
    text-transform: uppercase;
  }

  .delta-domain-row {
    display: grid;
    grid-template-columns: 184px minmax(0, 1fr);
    min-height: 66px;
    border-top: 1px solid rgba(147, 185, 224, 0.10);
  }

  .delta-domain-row:first-of-type { border-top: 0; }

  .delta-domain-label {
    display: flex;
    flex-direction: column;
    justify-content: center;
    border-right: 1px solid rgba(147, 185, 224, 0.12);
    border-left: 4px solid var(--cyan);
    padding: 8px 11px;
    background: rgba(101, 230, 224, 0.035);
  }

  .delta-domain-row.virtual .delta-domain-label {
    border-left-color: var(--blue);
    background: rgba(120, 169, 255, 0.035);
  }

  .delta-domain-row.promql .delta-domain-label {
    border-left-color: var(--violet);
    background: rgba(185, 149, 255, 0.035);
  }

  .delta-domain-label span {
    color: var(--cyan);
    font-size: 6.5px;
    font-weight: 850;
    letter-spacing: 0.09em;
    text-transform: uppercase;
  }

  .delta-domain-row.virtual .delta-domain-label span { color: var(--blue); }
  .delta-domain-row.promql .delta-domain-label span { color: var(--violet); }

  .delta-domain-label b {
    margin-top: 4px;
    color: var(--ink);
    font-size: 9px;
  }

  .delta-domain-label small {
    margin-top: 3px;
    color: var(--faint);
    font-size: 6.5px;
    line-height: 1.2;
  }

  .delta-domain-track {
    display: grid;
    grid-template-columns: repeat(5, minmax(0, 1fr));
    min-width: 0;
    align-items: stretch;
    padding: 5px 6px;
    background:
      linear-gradient(90deg,
        transparent calc(20% - 0.5px),
        rgba(147, 185, 224, 0.08) calc(20% - 0.5px),
        rgba(147, 185, 224, 0.08) calc(20% + 0.5px),
        transparent calc(20% + 0.5px),
        transparent calc(40% - 0.5px),
        rgba(147, 185, 224, 0.08) calc(40% - 0.5px),
        rgba(147, 185, 224, 0.08) calc(40% + 0.5px),
        transparent calc(40% + 0.5px),
        transparent calc(60% - 0.5px),
        rgba(147, 185, 224, 0.08) calc(60% - 0.5px),
        rgba(147, 185, 224, 0.08) calc(60% + 0.5px),
        transparent calc(60% + 0.5px),
        transparent calc(80% - 0.5px),
        rgba(147, 185, 224, 0.08) calc(80% - 0.5px),
        rgba(147, 185, 224, 0.08) calc(80% + 0.5px),
        transparent calc(80% + 0.5px));
  }

  .delta-domain-cell {
    display: flex;
    min-width: 0;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    margin: 0 4px;
    border: 1px solid rgba(101, 230, 224, 0.18);
    border-radius: 8px;
    padding: 5px 7px;
    background: rgba(101, 230, 224, 0.045);
    text-align: center;
  }

  .delta-domain-cell b {
    color: var(--cyan);
    font-family: var(--mono);
    font-size: 9px;
  }

  .delta-domain-cell span {
    margin-top: 3px;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 6.8px;
    line-height: 1.15;
  }

  .delta-domain-cell.empty {
    border-style: dashed;
    border-color: rgba(147, 185, 224, 0.10);
    color: var(--faint);
    background: transparent;
    font-size: 6.5px;
  }

  .delta-domain-cell.empty b,
  .delta-domain-cell.empty span {
    color: var(--faint);
  }

  .delta-domain-cell.stale {
    border-color: rgba(255, 199, 106, 0.24);
    background: rgba(255, 199, 106, 0.055);
  }

  .delta-domain-cell.stale b { color: var(--amber); }

  .delta-domain-row.virtual .delta-domain-cell {
    border-color: rgba(120, 169, 255, 0.20);
    background: rgba(120, 169, 255, 0.045);
  }

  .delta-domain-row.virtual .delta-domain-cell b { color: var(--blue); }

  .delta-domain-cell.seed {
    border-style: dashed;
    border-color: rgba(185, 149, 255, 0.22);
    background: rgba(185, 149, 255, 0.035);
  }

  .delta-domain-cell.seed b { color: var(--violet); }

  .delta-domain-cell.stale-marker {
    border-color: rgba(255, 199, 106, 0.24);
    background: rgba(255, 199, 106, 0.055);
  }

  .delta-domain-cell.stale-marker b { color: var(--amber); }

  .delta-range-seed {
    display: flex;
    min-width: 0;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    margin: 0 4px;
    border: 1px dashed rgba(185, 149, 255, 0.22);
    border-radius: 8px;
    color: var(--faint);
    background: rgba(185, 149, 255, 0.025);
    font-size: 6.5px;
    line-height: 1.2;
    text-align: center;
  }

  .delta-range-seed b {
    margin-bottom: 3px;
    color: var(--violet);
    font-family: var(--mono);
    font-size: 8px;
  }

  .delta-range-band {
    position: relative;
    display: flex;
    grid-column: 2 / 6;
    min-width: 0;
    align-items: center;
    justify-content: center;
    margin: 0 4px;
    border: 1px solid rgba(185, 149, 255, 0.26);
    border-radius: 8px;
    padding: 6px 32px;
    color: var(--muted);
    background: linear-gradient(90deg, rgba(185, 149, 255, 0.07), rgba(120, 169, 255, 0.05));
    font-size: 7.5px;
    line-height: 1.2;
    text-align: center;
  }

  .delta-range-band::before,
  .delta-range-band::after {
    position: absolute;
    top: 50%;
    color: var(--violet);
    font-family: var(--mono);
    font-size: 6.5px;
    font-weight: 800;
    transform: translateY(-50%);
  }

  .delta-range-band::before {
    left: 8px;
    content: "( start";
  }

  .delta-range-band::after {
    right: 8px;
    content: "end ]";
  }

  .delta-range-band b {
    color: var(--ink);
    font-size: 8px;
  }

  .delta-domain-contracts {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 10px;
    margin-top: 11px;
  }

  .delta-domain-contract {
    min-width: 0;
    min-height: 78px;
    border: 1px solid var(--line);
    border-left: 4px solid var(--cyan);
    border-radius: 10px;
    padding: 9px 11px;
    background: rgba(8, 22, 39, 0.77);
  }

  .delta-domain-contract.reset { border-left-color: var(--blue); }
  .delta-domain-contract.sum { border-left-color: var(--amber); }

  .delta-domain-contract b {
    display: block;
    margin-bottom: 5px;
    color: var(--cyan);
    font-size: 9.5px;
  }

  .delta-domain-contract.reset b { color: var(--blue); }
  .delta-domain-contract.sum b { color: var(--amber); }

  .delta-domain-contract p {
    margin: 0;
    color: var(--muted);
    font-size: 7.8px;
    line-height: 1.3;
  }

  .delta-domain-contract code {
    border: 0;
    padding: 0;
    color: #dce9f8;
    background: transparent;
    font-size: 0.96em;
  }

  .head-time-viz {
    border: 1px solid rgba(120, 169, 255, 0.22);
    border-radius: 13px;
    padding: 12px 14px 11px;
    background: rgba(5, 15, 28, 0.82);
  }

  .head-time-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
  }

  .head-time-head b {
    color: var(--ink);
    font-size: 11px;
    letter-spacing: 0.07em;
    text-transform: uppercase;
  }

  .head-time-head b code {
    border: 0;
    padding: 0;
    color: var(--cyan);
    background: transparent;
    font-size: 1em;
    letter-spacing: 0;
    text-transform: none;
  }

  .head-time-head span {
    color: var(--faint);
    font-size: 8.5px;
  }

  .head-time-ticks {
    display: flex;
    justify-content: space-between;
    margin-top: 10px;
    color: var(--faint);
    font-family: var(--mono);
    font-size: 8px;
  }

  .head-time-track {
    position: relative;
    display: grid;
    height: 91px;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    margin-top: 3px;
  }

  .head-time-window {
    position: relative;
    min-width: 0;
    overflow: hidden;
    border: 1px solid rgba(147, 185, 224, 0.23);
    padding: 8px 10px;
    background: rgba(120, 169, 255, 0.035);
  }

  .head-time-window + .head-time-window { border-left: 0; }
  .head-time-window:first-child { border-radius: 9px 0 0 9px; }
  .head-time-window:last-child { border-radius: 0 9px 9px 0; }

  .head-time-window.previous {
    background: rgba(120, 169, 255, 0.025);
  }

  .head-time-window.current {
    border-color: rgba(101, 230, 224, 0.50);
    background: linear-gradient(90deg, rgba(101, 230, 224, 0.13), rgba(120, 169, 255, 0.10));
  }

  .head-time-window.next {
    border-style: dashed;
    background: rgba(120, 169, 255, 0.055);
  }

  .head-time-window > b {
    display: block;
    color: var(--ink);
    font-size: 9px;
  }

  .head-time-window.previous > b { color: var(--faint); }
  .head-time-window.current > b { color: var(--cyan); }
  .head-time-window.next > b { color: var(--blue); }

  .head-time-window > span {
    display: block;
    margin-top: 2px;
    color: var(--muted);
    font-family: var(--mono);
    font-size: 7.5px;
  }

  .head-time-event {
    position: absolute;
    bottom: 7px;
    display: flex;
    flex-direction: column;
    align-items: center;
    transform: translateX(-50%);
    z-index: 2;
  }

  .head-time-event.at-start {
    left: 8px;
    transform: none;
  }

  .head-time-event i {
    display: flex;
    width: 22px;
    height: 22px;
    align-items: center;
    justify-content: center;
    border: 2px solid var(--blue);
    border-radius: 50%;
    color: var(--ink);
    background: #102a46;
    font-family: var(--mono);
    font-size: 9px;
    font-style: normal;
    font-weight: 800;
    box-shadow: 0 0 0 3px rgba(120, 169, 255, 0.08);
  }

  .head-time-window.current .head-time-event.late i {
    border-color: var(--amber);
    background: #34281d;
    box-shadow: 0 0 0 3px rgba(255, 200, 109, 0.08);
  }

  .head-time-event.late small { color: var(--amber); }

  .head-time-window.current .head-time-event i {
    border-color: var(--cyan);
    background: #10323b;
    box-shadow: 0 0 0 3px rgba(101, 230, 224, 0.08);
  }

  .head-time-event small {
    margin-top: 2px;
    color: #d8e8ff;
    font-family: var(--mono);
    font-size: 7px;
  }

  .head-time-rotation {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    margin-top: 8px;
    border-radius: 7px;
    padding: 6px 9px;
    color: var(--muted);
    background: rgba(101, 230, 224, 0.065);
    font-size: 8.5px;
  }

  .head-time-rotation code {
    color: var(--cyan);
    font-size: 1em;
  }

  .head-time-caption {
    display: flex;
    justify-content: space-between;
    gap: 12px;
    margin-top: 7px;
    color: var(--muted);
    font-size: 8.2px;
    line-height: 1.25;
  }

  .head-time-caption strong { color: var(--ink); }
  .head-time-caption code {
    flex: 0 0 auto;
    color: var(--faint);
    font-size: 1em;
    white-space: nowrap;
  }

  .clock-stack {
    display: grid;
    gap: 8px;
  }

  .clock-lane {
    display: grid;
    grid-template-columns: 235px 1fr;
    min-height: 112px;
    overflow: hidden;
    border: 1px solid var(--line);
    border-radius: 15px;
    background: linear-gradient(155deg, var(--panel), var(--panel-2));
    box-shadow: 0 13px 34px rgba(0, 0, 0, 0.14);
  }

  .clock-name {
    display: flex;
    flex-direction: column;
    justify-content: center;
    padding: 15px 18px;
    border-right: 1px solid var(--line);
  }

  .clock-name.control { border-left: 4px solid var(--blue); }
  .clock-name.data { border-left: 4px solid var(--cyan); }

  .clock-kicker {
    margin-bottom: 5px;
    color: var(--faint);
    font-size: 10px;
    font-weight: 800;
    letter-spacing: 0.13em;
  }

  .clock-name strong {
    color: var(--ink);
    font-family: var(--mono);
    font-size: 18px;
    letter-spacing: -0.035em;
  }

  .clock-name small {
    margin-top: 5px;
    color: var(--muted);
    font-size: 11px;
    line-height: 1.3;
  }

  .policy-window {
    display: flex;
    flex-direction: column;
    justify-content: center;
    padding: 15px 20px 13px;
  }

  .policy-labels {
    display: flex;
    justify-content: space-between;
    margin: 0 18.5% 7px;
    color: #bcd0e8;
    font-family: var(--mono);
    font-size: 10px;
    font-weight: 700;
  }

  .policy-labels span:nth-child(2) { color: var(--blue); }

  .policy-track {
    display: grid;
    grid-template-columns: 18.5% 63% 18.5%;
    height: 53px;
    overflow: hidden;
    border: 1px solid rgba(147, 185, 224, 0.25);
    border-radius: 9px;
  }

  .policy-zone {
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    color: var(--muted);
    font-size: 10px;
    line-height: 1.15;
    text-align: center;
  }

  .policy-zone b {
    color: var(--red);
    font-size: 10px;
    letter-spacing: 0.08em;
  }

  .policy-zone.accept {
    border-right: 1px solid rgba(101, 230, 224, 0.34);
    border-left: 1px solid rgba(101, 230, 224, 0.34);
    color: #d8e8ff;
    background: linear-gradient(90deg, rgba(101, 230, 224, 0.13), rgba(120, 169, 255, 0.16));
  }

  .policy-zone.accept b { color: var(--green); }
  .policy-zone.reject { background: rgba(255, 130, 149, 0.07); }

  .clock-compare {
    display: flex;
    height: 27px;
    align-items: center;
    justify-content: center;
    gap: 9px;
    color: var(--faint);
    font-size: 10px;
    font-weight: 800;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .clock-compare::before,
  .clock-compare::after {
    width: 84px;
    height: 1px;
    content: "";
    background: linear-gradient(90deg, transparent, rgba(120, 169, 255, 0.40));
  }

  .clock-compare::after {
    background: linear-gradient(90deg, rgba(120, 169, 255, 0.40), transparent);
  }

  .clock-compare code {
    color: var(--cyan);
    font-size: 11px;
    text-transform: none;
  }

  .event-effects {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 9px;
    align-items: stretch;
    padding: 14px;
  }

  .event-effect {
    display: flex;
    flex-direction: column;
    justify-content: center;
    border: 1px solid rgba(101, 230, 224, 0.16);
    border-radius: 10px;
    padding: 10px 11px;
    background: rgba(101, 230, 224, 0.055);
  }

  .event-effect b {
    margin-bottom: 3px;
    color: var(--cyan);
    font-size: 12px;
  }

  .event-effect span {
    color: var(--muted);
    font-size: 10px;
    line-height: 1.25;
  }

  .clock-guards {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 10px;
    margin-top: 12px;
  }

  .clock-guard {
    display: grid;
    grid-template-columns: auto 1fr;
    gap: 6px 12px;
    min-height: 65px;
    align-items: center;
    border: 1px solid var(--line);
    border-radius: 11px;
    padding: 10px 13px;
    background: rgba(8, 22, 39, 0.86);
  }

  .clock-guard b {
    grid-row: 1 / span 2;
    border-radius: 999px;
    padding: 5px 8px;
    color: var(--red);
    background: rgba(255, 130, 149, 0.10);
    font-size: 9px;
    letter-spacing: 0.08em;
  }

  .clock-guard.diagnostic b {
    color: var(--amber);
    background: rgba(255, 199, 106, 0.10);
  }

  .clock-guard code {
    width: max-content;
    color: #dce9f8;
    font-size: 10px;
  }

  .clock-guard span {
    color: var(--muted);
    font-size: 10px;
    line-height: 1.25;
  }

  .clock-summary {
    margin-top: 11px;
    padding: 11px 15px;
    font-size: 14px;
    text-align: center;
  }

  .source {
    position: absolute;
    left: 62px;
    right: 78px;
    bottom: 17px;
    overflow: hidden;
    color: var(--faint);
    font-size: 9px;
    letter-spacing: 0.01em;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .split-line {
    height: 1px;
    margin: 17px 0;
    background: linear-gradient(90deg, transparent, var(--line), transparent);
  }

  .flow-row {
    display: flex;
    align-items: center;
    gap: 12px;
  }

  .flow-row .flow-box {
    flex: 1 1 0;
    border: 1px solid var(--line);
    border-radius: 13px;
    padding: 15px;
    background: rgba(10, 25, 44, 0.78);
    text-align: center;
  }

  .flow-box b {
    display: block;
    color: var(--ink);
    font-size: 17px;
  }

  .flow-box span {
    display: block;
    margin-top: 6px;
    color: var(--muted);
    font-size: 13px;
    line-height: 1.28;
  }

  .flow-arrow {
    color: var(--cyan);
    font-size: 24px;
    font-weight: 800;
  }

  .intern-scope-head {
    display: flex;
    align-items: center;
    gap: 10px;
    margin-bottom: 8px;
  }

  .intern-scope-chip {
    border-radius: 999px;
    padding: 5px 9px;
    color: var(--cyan);
    background: rgba(101, 230, 224, 0.09);
    font-size: 9px;
    font-weight: 800;
    letter-spacing: 0.09em;
    text-transform: uppercase;
  }

  .intern-scope-head span:last-child {
    color: var(--muted);
    font-size: 11px;
  }

  .trace-metrics {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 10px;
  }

  .trace-metric {
    height: 75px;
    border: 1px solid var(--line);
    border-top: 3px solid var(--cyan);
    border-radius: 11px;
    padding: 9px 12px;
    background: linear-gradient(155deg, var(--panel), var(--panel-2));
  }

  .trace-metric:nth-child(2) { border-top-color: var(--blue); }
  .trace-metric:nth-child(3) { border-top-color: var(--violet); }
  .trace-metric:nth-child(4) { border-top-color: var(--amber); }

  .trace-metric strong {
    display: block;
    color: var(--ink);
    font-size: 24px;
    line-height: 1;
    letter-spacing: -0.035em;
  }

  .trace-metric span {
    display: block;
    margin-top: 6px;
    color: var(--muted);
    font-size: 9.5px;
    line-height: 1.2;
  }

  .intern-bench-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    margin: 13px 0 8px;
  }

  .intern-bench-head b {
    color: var(--ink);
    font-size: 13px;
  }

  .intern-bench-head span {
    color: var(--faint);
    font-size: 10px;
  }

  .intern-shapes {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 12px;
  }

  .intern-shape {
    height: 102px;
    border: 1px solid var(--line);
    border-left: 4px solid var(--red);
    border-radius: 11px;
    padding: 11px 14px;
    background: rgba(8, 22, 39, 0.88);
  }

  .intern-shape.arena-shape { border-left-color: var(--green); }

  .intern-shape h3 {
    margin: 0 0 6px;
    font-size: 14px;
  }

  .intern-shape code {
    font-size: 10px;
  }

  .intern-shape p {
    margin: 7px 0 0;
    color: var(--muted);
    font-size: 10px;
    line-height: 1.25;
  }

  section table.benchmark-table {
    display: table !important;
    width: 100%;
    margin-top: 9px;
    overflow: hidden;
    border: 1px solid var(--line);
    border-collapse: separate;
    border-spacing: 0;
    border-radius: 10px;
    background: rgba(7, 19, 34, 0.92);
    table-layout: fixed;
  }

  .benchmark-table th:nth-child(1) { width: 27%; }
  .benchmark-table th:nth-child(2) { width: 18%; }
  .benchmark-table th:nth-child(3) { width: 23%; }
  .benchmark-table th:nth-child(4) { width: 32%; }

  .benchmark-table th {
    padding: 6px 10px;
    border-bottom: 1px solid rgba(101, 230, 224, 0.24);
    color: var(--cyan);
    background: rgba(16, 43, 61, 0.76);
    font-size: 10px;
    letter-spacing: 0.01em;
    text-align: left;
    text-transform: none;
  }

  .benchmark-table th.type-header {
    color: #e4eefb;
    font-family: var(--mono);
    font-weight: 700;
    letter-spacing: -0.025em;
  }

  .benchmark-table td {
    padding: 5px 10px;
    border-bottom: 1px solid rgba(147, 185, 224, 0.10);
    color: var(--muted);
    background: rgba(7, 19, 34, 0.92);
    font-size: 10px;
    line-height: 1.2;
  }

  .benchmark-table tr:last-child td { border-bottom: 0; }
  .benchmark-table tr.cpu-start td {
    border-top: 2px solid rgba(120, 169, 255, 0.28);
  }
  .benchmark-table td:first-child { color: #dce9f8; }

  .benchmark-table .number {
    color: var(--ink);
    font-family: var(--mono);
    font-weight: 700;
  }

  .benchmark-table .batch-count {
    margin-left: 4px;
    color: var(--faint);
    font-size: 7px;
    white-space: nowrap;
  }

  .benchmark-table .improvement { color: var(--green); }
  .benchmark-table .tradeoff { color: var(--amber); }

  .intern-clarifier {
    margin-top: 9px;
    padding: 9px 13px;
    font-size: 11px;
    text-align: center;
  }

  .arena-viz-path {
    display: grid;
    grid-template-columns: 1fr 20px 1fr 20px 0.88fr 20px 1fr 20px 1.08fr;
    align-items: center;
  }

  .arena-viz-node {
    display: flex;
    height: 82px;
    min-width: 0;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 10px 12px;
    background: linear-gradient(155deg, var(--panel), var(--panel-2));
    text-align: center;
  }

  .arena-viz-node.active {
    border-color: rgba(101, 230, 224, 0.38);
    background: linear-gradient(155deg, rgba(18, 52, 68, 0.94), rgba(9, 31, 48, 0.90));
  }

  .arena-viz-node b {
    color: var(--ink);
    font-family: var(--mono);
    font-size: 13px;
    line-height: 1.2;
  }

  .arena-viz-node span {
    margin-top: 6px;
    color: var(--muted);
    font-size: 9px;
    line-height: 1.2;
  }

  .labelset-viz-input span {
    width: 100%;
    font-family: var(--mono);
    font-size: 7.3px;
    line-height: 1.42;
    text-align: left;
    white-space: nowrap;
  }

  .labelset-viz-input span code {
    border: 0;
    padding: 0;
    color: var(--cyan);
    background: transparent;
    font-size: 1em;
  }

  .arena-viz-arrow {
    color: var(--cyan);
    font-size: 17px;
    font-weight: 800;
    text-align: center;
  }

  .arena-viz-layout {
    display: grid;
    grid-template-columns: 1.46fr 0.54fr;
    gap: 12px;
    margin-top: 12px;
  }

  .arena-viz-buffer,
  .arena-viz-locs {
    height: 198px;
    border: 1px solid var(--line);
    border-radius: 13px;
    padding: 13px 15px;
    background: rgba(5, 16, 29, 0.88);
  }

  .arena-viz-header {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
  }

  .arena-viz-header b {
    color: var(--ink);
    font-family: var(--mono);
    font-size: 13px;
  }

  .arena-viz-header span {
    color: var(--faint);
    font-size: 9px;
  }

  .arena-viz-axis,
  .arena-viz-bytes {
    display: grid;
    grid-template-columns: 8fr 12fr 4fr 6fr;
  }

  .arena-viz-axis {
    margin-top: 13px;
    color: var(--faint);
    font-family: var(--mono);
    font-size: 9px;
  }

  .arena-viz-byte {
    display: flex;
    height: 61px;
    min-width: 0;
    align-items: center;
    justify-content: center;
    border: 1px solid rgba(147, 185, 224, 0.23);
    border-right: 0;
    color: #dce9f8;
    background: rgba(120, 169, 255, 0.055);
    font-family: var(--mono);
    font-size: 12px;
    white-space: nowrap;
  }

  .arena-viz-byte:first-child { border-radius: 9px 0 0 9px; }
  .arena-viz-byte:last-child {
    border-right: 1px solid rgba(147, 185, 224, 0.23);
    border-radius: 0 9px 9px 0;
  }

  .arena-viz-byte.selected {
    border: 2px solid var(--cyan);
    color: var(--cyan);
    background: linear-gradient(90deg, rgba(101, 230, 224, 0.18), rgba(120, 169, 255, 0.16));
    font-weight: 700;
  }

  .arena-viz-slice {
    margin-top: 12px;
    border-radius: 8px;
    padding: 8px 10px;
    color: var(--muted);
    background: rgba(101, 230, 224, 0.065);
    font-size: 10px;
    text-align: center;
  }

  .arena-viz-slice code { color: var(--cyan); }

  .arena-viz-loc-head,
  .arena-viz-loc-row {
    display: grid;
    grid-template-columns: 0.7fr 1fr 0.7fr;
    gap: 5px;
    align-items: center;
  }

  .arena-viz-loc-head {
    margin-top: 12px;
    color: var(--faint);
    font-size: 8px;
    letter-spacing: 0.06em;
    text-transform: uppercase;
  }

  .arena-viz-loc-row {
    margin-top: 5px;
    border: 1px solid rgba(147, 185, 224, 0.14);
    border-radius: 7px;
    padding: 6px 8px;
    color: var(--muted);
    background: rgba(120, 169, 255, 0.04);
    font-family: var(--mono);
    font-size: 10px;
  }

  .arena-viz-loc-row.selected {
    border-color: rgba(101, 230, 224, 0.42);
    color: var(--cyan);
    background: rgba(101, 230, 224, 0.09);
  }

  .arena-viz-loc-foot {
    margin-top: 7px;
    color: var(--faint);
    font-size: 8.5px;
    text-align: right;
  }

  .arena-viz-bottom {
    display: grid;
    grid-template-columns: 1.55fr 0.45fr;
    gap: 12px;
    margin-top: 12px;
  }

  .arena-viz-fanout {
    display: grid;
    height: 96px;
    grid-template-columns: 145px 22px 1fr;
    grid-template-rows: auto 1fr;
    align-items: center;
    border: 1px solid var(--line);
    border-radius: 12px;
    padding: 8px 13px 10px;
    background: linear-gradient(155deg, var(--panel), var(--panel-2));
  }

  .arena-viz-fanout-title {
    grid-column: 1 / -1;
    color: var(--muted);
    font-size: 8.5px;
    font-weight: 700;
    letter-spacing: 0.08em;
    line-height: 1;
    text-transform: uppercase;
  }

  .arena-viz-fanout-title code {
    color: var(--cyan);
    font-size: 1em;
    letter-spacing: 0;
    text-transform: none;
  }

  .arena-viz-id {
    border: 1px solid rgba(101, 230, 224, 0.34);
    border-radius: 9px;
    padding: 8px 10px;
    color: var(--cyan);
    background: rgba(101, 230, 224, 0.075);
    font-family: var(--mono);
    font-size: 13px;
    font-weight: 700;
    text-align: center;
  }

  .arena-viz-id small {
    display: block;
    margin-top: 4px;
    color: var(--muted);
    font-family: Inter, ui-sans-serif, system-ui, sans-serif;
    font-size: 8px;
    font-weight: 400;
  }

  .arena-viz-consumers {
    display: grid;
    grid-template-columns: repeat(4, minmax(0, 1fr));
    gap: 7px;
  }

  .arena-viz-consumer {
    border: 1px solid rgba(120, 169, 255, 0.16);
    border-radius: 8px;
    padding: 9px 7px;
    color: #dce9f8;
    background: rgba(120, 169, 255, 0.055);
    font-size: 9px;
    text-align: center;
  }

  .arena-viz-consumer span {
    display: block;
    margin-top: 3px;
    color: var(--faint);
    font-size: 7.5px;
  }

  .arena-viz-rules {
    display: grid;
    gap: 8px;
  }

  .arena-viz-rule {
    display: flex;
    min-height: 44px;
    flex-direction: column;
    justify-content: center;
    border: 1px solid var(--line);
    border-left: 3px solid var(--cyan);
    border-radius: 9px;
    padding: 7px 9px;
    color: var(--muted);
    background: rgba(8, 22, 39, 0.86);
    font-size: 8.5px;
    line-height: 1.25;
  }

  .arena-viz-rule.tradeoff { border-left-color: var(--red); }
  .arena-viz-rule b {
    margin-bottom: 2px;
    color: var(--ink);
    font-size: 9px;
  }

  .labelset-viz-axis {
    display: grid;
    grid-template-columns: 2fr 3fr 2fr;
    margin-top: 13px;
    color: var(--faint);
    font-family: var(--mono);
    font-size: 8.5px;
  }

  .labelset-viz-axis span:nth-child(2) { color: var(--cyan); }

  .labelset-viz-pairs {
    display: grid;
    grid-template-columns: repeat(7, minmax(0, 1fr));
  }

  .labelset-viz-pair {
    display: flex;
    height: 61px;
    min-width: 0;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    border: 1px solid rgba(147, 185, 224, 0.23);
    border-right: 0;
    color: #dce9f8;
    background: rgba(120, 169, 255, 0.055);
    font-family: var(--mono);
    font-size: 10px;
    white-space: nowrap;
  }

  .labelset-viz-pair b {
    color: inherit;
    font-family: var(--mono);
    font-size: 10px;
  }

  .labelset-viz-pair span {
    margin-top: 3px;
    color: var(--muted);
    font-size: 7px;
  }

  .labelset-viz-pair small {
    margin-top: 1px;
    color: var(--faint);
    font-size: 6.5px;
  }

  .labelset-viz-pair:first-child { border-radius: 9px 0 0 9px; }
  .labelset-viz-pair:last-child {
    border-right: 1px solid rgba(147, 185, 224, 0.23);
    border-radius: 0 9px 9px 0;
  }

  .labelset-viz-pair.row-start {
    border-left: 2px solid rgba(185, 149, 255, 0.48);
  }

  .labelset-viz-pair.selected {
    border-top: 2px solid var(--cyan);
    border-bottom: 2px solid var(--cyan);
    color: var(--cyan);
    background: linear-gradient(90deg, rgba(101, 230, 224, 0.18), rgba(120, 169, 255, 0.16));
    font-weight: 700;
  }

  .labelset-viz-pair.selected span { color: #dce9f8; }
  .labelset-viz-pair.selected small { color: var(--cyan); }
  .labelset-viz-pair.selected-start { border-left: 2px solid var(--cyan); }
  .labelset-viz-pair.selected-end { border-right: 2px solid var(--cyan); }

  .storage-example {
    display: flex;
    min-height: 55px;
    align-items: center;
    justify-content: space-between;
    gap: 18px;
    margin-bottom: 12px;
    border: 1px solid rgba(101, 230, 224, 0.24);
    border-left: 4px solid var(--cyan);
    border-radius: 12px;
    padding: 9px 14px;
    background: rgba(101, 230, 224, 0.055);
  }

  .storage-example-copy {
    min-width: 0;
    color: var(--muted);
    font-size: 11px;
  }

  .storage-example-copy b {
    display: block;
    margin-bottom: 4px;
    color: var(--cyan);
    font-size: 9px;
    letter-spacing: 0.13em;
    text-transform: uppercase;
  }

  .storage-example-copy code {
    color: #edf7ff;
    font-size: 13px;
    white-space: nowrap;
  }

  .storage-example-ref {
    flex: 0 0 auto;
    border: 1px solid rgba(120, 169, 255, 0.28);
    border-radius: 9px;
    padding: 7px 11px;
    color: var(--blue);
    background: rgba(120, 169, 255, 0.075);
    font-family: var(--mono);
    font-size: 11px;
    font-weight: 700;
    text-align: center;
  }

  .storage-example-ref span {
    display: block;
    margin-top: 2px;
    color: var(--faint);
    font-family: Inter, ui-sans-serif, system-ui, sans-serif;
    font-size: 8px;
    font-weight: 500;
  }

  .storage-repr {
    display: grid;
    height: 340px;
    grid-template-columns: 252px minmax(0, 1fr);
    gap: 12px;
  }

  .storage-file {
    min-width: 0;
    border: 1px solid var(--line);
    border-radius: 13px;
    padding: 11px 13px;
    background: linear-gradient(155deg, rgba(14, 30, 52, 0.92), rgba(9, 23, 41, 0.82));
  }

  .storage-file-head {
    display: flex;
    align-items: baseline;
    justify-content: space-between;
    gap: 10px;
    margin-bottom: 7px;
  }

  .storage-filename {
    color: var(--cyan);
    font-family: var(--mono);
    font-size: 13px;
    font-weight: 800;
  }

  .storage-role {
    color: var(--faint);
    font-size: 8px;
    font-weight: 750;
    letter-spacing: 0.08em;
    text-transform: uppercase;
  }

  .storage-symbols {
    border-top: 3px solid var(--cyan);
  }

  .storage-symbols > p {
    margin: 0 0 9px;
    color: var(--muted);
    font-size: 10px;
    line-height: 1.3;
  }

  .storage-symbol-list {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 6px;
  }

  .storage-symbol {
    min-width: 0;
    border: 1px solid rgba(147, 185, 224, 0.14);
    border-radius: 8px;
    padding: 7px 8px;
    background: rgba(120, 169, 255, 0.045);
  }

  .storage-symbol b {
    display: block;
    color: var(--blue);
    font-family: var(--mono);
    font-size: 8px;
  }

  .storage-symbol span {
    display: block;
    overflow: hidden;
    margin-top: 3px;
    color: #dce9f8;
    font-family: var(--mono);
    font-size: 8px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .storage-symbol-note {
    margin-top: 9px;
    border-top: 1px solid rgba(147, 185, 224, 0.13);
    padding-top: 8px;
    color: var(--faint);
    font-size: 8.5px;
    line-height: 1.28;
  }

  .storage-main {
    display: grid;
    min-width: 0;
    grid-template-rows: 76px 106px minmax(0, 1fr);
    gap: 10px;
  }

  .storage-index {
    border-top: 3px solid var(--violet);
  }

  .storage-index-expression {
    display: flex;
    min-width: 0;
    align-items: center;
    justify-content: space-between;
    gap: 12px;
  }

  .storage-index-expression code {
    overflow: hidden;
    color: #dce9f8;
    font-size: 9.5px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .storage-index-result {
    flex: 0 0 auto;
    color: var(--violet);
    font-family: var(--mono);
    font-size: 11px;
    font-weight: 800;
  }

  .storage-index-result span {
    color: var(--faint);
    font-family: Inter, ui-sans-serif, system-ui, sans-serif;
    font-size: 8px;
    font-weight: 500;
  }

  .storage-series {
    border-top: 3px solid var(--blue);
  }

  .storage-series-row {
    display: grid;
    grid-template-columns: 0.72fr 1.35fr 1fr;
    gap: 8px;
  }

  .storage-series-cell {
    min-width: 0;
    border-left: 2px solid rgba(120, 169, 255, 0.34);
    padding-left: 9px;
  }

  .storage-series-cell b {
    display: block;
    margin-bottom: 4px;
    color: var(--ink);
    font-size: 9px;
  }

  .storage-series-cell span,
  .storage-series-cell code {
    display: block;
    overflow: hidden;
    color: var(--muted);
    font-size: 8.5px;
    line-height: 1.32;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .storage-series-cell code {
    border: 0;
    padding: 0;
    background: transparent;
    font-family: var(--mono);
  }

  .storage-payload-row {
    display: grid;
    min-width: 0;
    grid-template-columns: 0.78fr 1.22fr;
    gap: 10px;
  }

  .storage-routes {
    display: grid;
    min-width: 0;
    grid-template-rows: repeat(2, minmax(0, 1fr));
    gap: 7px;
  }

  .storage-route {
    display: grid;
    min-width: 0;
    grid-template-columns: 52px minmax(0, 1fr) 18px;
    align-items: center;
    gap: 7px;
    border: 1px solid var(--line);
    border-radius: 10px;
    padding: 7px 9px;
    background: rgba(9, 24, 43, 0.82);
  }

  .storage-route-tag {
    border-radius: 999px;
    padding: 4px 6px;
    color: #06221b;
    background: var(--green);
    font-size: 7px;
    font-weight: 850;
    letter-spacing: 0.05em;
    text-align: center;
    text-transform: uppercase;
  }

  .storage-route.overflow .storage-route-tag {
    color: #2a1c00;
    background: var(--amber);
  }

  .storage-route-copy {
    min-width: 0;
  }

  .storage-route-copy b {
    display: block;
    overflow: hidden;
    color: var(--ink);
    font-family: var(--mono);
    font-size: 9px;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .storage-route-copy span {
    display: block;
    overflow: hidden;
    margin-top: 2px;
    color: var(--faint);
    font-size: 7.5px;
    line-height: 1.2;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .storage-route-arrow {
    color: var(--cyan);
    font-size: 16px;
    font-weight: 850;
  }

  .storage-chunks {
    border-top: 3px solid var(--green);
  }

  .storage-lanes {
    display: flex;
    gap: 6px;
    margin-bottom: 8px;
  }

  .storage-lane {
    border: 1px solid rgba(120, 232, 167, 0.25);
    border-radius: 999px;
    padding: 4px 7px;
    color: var(--green);
    background: rgba(120, 232, 167, 0.055);
    font-size: 7.5px;
  }

  .storage-lane.ooo {
    border-color: rgba(255, 199, 106, 0.24);
    color: var(--amber);
    background: rgba(255, 199, 106, 0.055);
  }

  .storage-chunk-record {
    border-left: 2px solid rgba(120, 232, 167, 0.38);
    padding-left: 9px;
  }

  .storage-chunk-record code {
    display: block;
    overflow: hidden;
    border: 0;
    padding: 0;
    color: #dce9f8;
    background: transparent;
    font-size: 8.5px;
    line-height: 1.35;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .storage-chunk-record span {
    display: block;
    margin-top: 4px;
    color: var(--faint);
    font-size: 8px;
  }

  .storage-governance {
    display: grid;
    grid-template-columns: 0.82fr 1.18fr;
    gap: 10px;
    margin-top: 10px;
  }

  .storage-governance > div {
    display: flex;
    min-width: 0;
    min-height: 45px;
    align-items: center;
    gap: 10px;
    border: 1px solid rgba(147, 185, 224, 0.16);
    border-radius: 10px;
    padding: 8px 11px;
    background: rgba(9, 23, 41, 0.65);
  }

  .storage-governance b {
    flex: 0 0 auto;
    color: var(--amber);
    font-family: var(--mono);
    font-size: 9px;
  }

  .storage-governance span {
    overflow: hidden;
    color: var(--muted);
    font-size: 8.5px;
    line-height: 1.25;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .status-row {
    display: grid;
    grid-template-columns: 84px 1fr;
    gap: 11px;
    align-items: start;
    margin: 8px 0;
  }

  .status-label {
    border-radius: 999px;
    padding: 5px 8px;
    text-align: center;
    font-size: 11px;
    font-weight: 850;
    letter-spacing: 0.03em;
    text-transform: uppercase;
  }

  .status-label.solid {
    color: #092018;
    background: var(--green);
  }

  .status-label.partial {
    color: #2a1c00;
    background: var(--amber);
  }

  .status-label.gap {
    color: #2c0710;
    background: var(--red);
  }

  .status-copy {
    color: var(--muted);
    font-size: 14px;
    line-height: 1.32;
  }

  .spec-table {
    width: 100%;
    overflow: hidden;
    border-collapse: separate;
    border: 1px solid var(--line);
    border-radius: 13px;
    border-spacing: 0;
    background: rgba(8, 22, 39, 0.80);
  }

  .spec-table th {
    padding: 8px 11px;
    border-bottom: 1px solid rgba(101, 230, 224, 0.26);
    color: var(--cyan);
    background: rgba(101, 230, 224, 0.06);
    font-size: 12px;
    font-weight: 800;
    letter-spacing: 0.04em;
    text-align: left;
    text-transform: uppercase;
  }

  .spec-table td {
    padding: 8px 11px;
    border-bottom: 1px solid rgba(147, 185, 224, 0.11);
    color: var(--muted);
    font-size: 12px;
    line-height: 1.26;
    vertical-align: top;
  }

  .spec-table tr:last-child td { border-bottom: 0; }
  .spec-table td:first-child {
    color: #dce9f8;
    font-family: var(--mono);
    font-weight: 760;
  }

  section .spec-table,
  section .spec-table thead,
  section .spec-table tbody,
  section .spec-table tr,
  section .spec-table th,
  section .spec-table td {
    background-color: rgba(8, 22, 39, 0.96) !important;
  }

  section .spec-table th {
    background-color: rgba(16, 43, 61, 0.98) !important;
  }

  section .spec-table tbody tr:nth-child(even) td {
    background-color: rgba(11, 28, 48, 0.98) !important;
  }

  .state-table {
    display: grid;
    grid-template-columns: 145px 1fr 1fr;
    overflow: hidden;
    border: 1px solid var(--line);
    border-radius: 13px;
  }

  .state-table > div {
    min-height: 57px;
    padding: 10px 12px;
    border-right: 1px solid rgba(147, 185, 224, 0.11);
    border-bottom: 1px solid rgba(147, 185, 224, 0.11);
    color: var(--muted);
    background: rgba(8, 22, 39, 0.72);
    font-size: 12px;
    line-height: 1.3;
  }

  .state-table > div:nth-child(3n) { border-right: 0; }
  .state-table > div:nth-last-child(-n + 3) { border-bottom: 0; }
  .state-table .head-cell {
    color: var(--cyan);
    background: rgba(101, 230, 224, 0.06);
    font-weight: 800;
  }

  .state-table .row-head {
    color: #dce9f8;
    font-weight: 760;
  }

  .cover h1 {
    margin: 2px 0 5px;
    font-size: 78px;
    line-height: 0.98;
    letter-spacing: -0.055em;
  }

  .cover h2 {
    margin-top: 12px;
    color: #cfe0f4;
    font-size: 31px;
    font-weight: 560;
  }

  .cover-flow {
    position: absolute;
    right: 64px;
    bottom: 102px;
    left: 64px;
  }

  .cover-flow .node {
    min-height: 78px;
  }

  .cover-flow .node b { font-size: 14px; }
  .cover-flow .node span { font-size: 10px; }

  .cover .tagline {
    width: 720px;
    margin-top: 24px;
    color: var(--muted);
    font-size: 20px;
    line-height: 1.42;
  }

  .statement {
    display: flex;
    flex-direction: column;
    justify-content: center;
  }

  .statement h1 {
    max-width: 1080px;
    font-size: 58px;
    line-height: 1.12;
  }

  .statement .lede { font-size: 25px; }
</style>

<!-- _class: cover -->
<!-- _paginate: false -->

<div class="eyebrow">Architecture walkthrough</div>

# Chronoxide

## An OTLP-native metrics TSDB

<div class="tagline">
Preserve typed telemetry on disk. Expose Prometheus-compatible query semantics
without flattening away the source model.
</div>

<div class="cover-flow">
  <div class="pipeline">
    <div class="node"><b>OTLP</b><span>Kafka or replay capture</span></div>
    <div class="arrow">→</div>
    <div class="node"><b>Interner</b><span>strings → compact IDs</span></div>
    <div class="arrow">→</div>
    <div class="node"><b>Head</b><span>event-time windows</span></div>
    <div class="arrow">→</div>
    <div class="node"><b>Schema 8</b><span>immutable typed segments</span></div>
    <div class="arrow">→</div>
    <div class="node"><b>PromQL</b><span>select · project · evaluate</span></div>
  </div>
</div>

---

<!-- _class: statement -->

# Native on disk.<br><strong>Prometheus-shaped at the query boundary.</strong>

<p class="lede">
Chronoxide keeps Histogram, ExponentialHistogram, Summary, temporality, flags,
start time, and reset hints as correctness data—then projects compatible views
only when a query asks for them.
</p>

<div class="split-line"></div>

<div class="grid three">
  <div class="card cyan-top compact">
    <h3>OTLP-native</h3>
    <p>Typed values remain typed instead of becoming a spray of scalar series at ingest.</p>
  </div>
  <div class="card blue-top compact">
    <h3>SSD-oriented</h3>
    <p>Windowed writes, immutable segments, lazy positional metadata, selective chunk reads.</p>
  </div>
  <div class="card violet-top compact">
    <h3>PromQL-compatible</h3>
    <p>Selectors, functions, aggregations, virtual projections, and Prometheus-oracle tests.</p>
  </div>
</div>

---

# One datapoint, end to end

<div class="grid four">
  <div class="card cyan-top">
    <span class="pill cyan-pill">01 · ingest edge</span>
    <h3 style="margin-top:13px">Kafka or capture</h3>
    <p>Raw <code>ExportMetricsServiceRequest</code>, source metadata, and trusted <code>captured_at_ms</code>.</p>
  </div>
  <div class="card blue-top">
    <span class="pill blue-pill">02 · mutable path</span>
    <h3 style="margin-top:13px">Decode · validate · intern</h3>
    <p>Required event time, canonical labels, typed values, per-partition windowed head.</p>
  </div>
  <div class="card violet-top">
    <span class="pill">03 · near store</span>
    <h3 style="margin-top:13px">Seal Schema 8</h3>
    <p>Sorted segment dictionary, dense refs, native chunks, selector indexes, footer inventory.</p>
  </div>
  <div class="card amber-top">
    <span class="pill amber-pill">04 · read path</span>
    <h3 style="margin-top:13px">Plan · read · project</h3>
    <p>PromQL AST, postings/FST planning, lazy chunk I/O, merge/dedupe, evaluation.</p>
  </div>
</div>

<div style="height:20px"></div>

<div class="callout">
  <strong>Two rules hold across every arrow:</strong>
  event time decides where a sample lives; typed OTLP metadata survives until evaluation.
</div>

<div class="source">Sources: docs/superpowers/specs/storage.md · clock.md · crate-boundaries.md</div>

---

# The vocabulary: identity, time, storage—and trust

<div class="grid three vocab-grid">
  <div class="card cyan-top vocab-card">
    <div class="eyebrow">Data identity</div>
    <div class="vocab-list">
      <div>
        <b>OTLP datapoint → query sample(s)</b>
        <span>One typed input observation at <code>event_ms</code>. Storage keeps it typed; projection may expose one or several Prometheus-shaped samples.</span>
      </div>
      <div>
        <b>Logical series</b>
        <span>Normalized metric name plus its complete canonical label set.</span>
      </div>
      <div>
        <b><code>SeriesRef</code> / <code>series_ref</code></b>
        <span>A dense handle, not global identity: head-local while mutable, remapped segment-locally when sealed.</span>
      </div>
    </div>
  </div>

  <div class="card blue-top vocab-card">
    <div class="eyebrow">Mutable event time</div>
    <div class="vocab-list">
      <div>
        <b>Head</b>
        <span>Bounded mutable state owned per source partition before publication.</span>
      </div>
      <div>
        <b>Window</b>
        <span>One aligned event-time interval; 15 minutes in this walkthrough.</span>
      </div>
      <div>
        <b>OOO</b>
        <span>A per-series event-time regression in arrival order—not “outside the window.” Pre-seal co-seals; post-seal publishes as overlap.</span>
      </div>
    </div>
  </div>

  <div class="card amber-top vocab-card">
    <div class="eyebrow">Immutable storage</div>
    <div class="vocab-list">
      <div>
        <b>Segment</b>
        <span>An immutable, independently decodable database for an event-time range; overlaps are allowed.</span>
      </div>
      <div>
        <b>Segment-local series row</b>
        <span>One logical series as represented inside one segment; it can reappear in later segments.</span>
      </div>
      <div>
        <b>Chunk / frame</b>
        <span>A chunk is variable-sized encoded samples for one row, kind, and range. A frame is its physical wrapper.</span>
      </div>
    </div>
  </div>
</div>

<div style="height:15px"></div>

<div class="flow-row vocab-chain">
  <div class="flow-box"><b>one logical series</b><span>identity across time</span></div>
  <div class="flow-arrow">→</div>
  <div class="flow-box"><b>one row per segment</b><span>when that series is present</span></div>
  <div class="flow-arrow">→</div>
  <div class="flow-box"><b>one or more chunks</b><span>variable-length records</span></div>
  <div class="flow-arrow">→</div>
  <div class="flow-box"><b>one or more datapoints</b><span>timestamps + typed values</span></div>
</div>

<div style="height:11px"></div>

<div class="grid two vocab-notes">
  <div class="vocab-note">
    <b>Input versus output:</b> a <code>capture</code> is a replay input file;
    a <code>corpus</code> is the generated set of immutable segment directories
    used for testing or measurement.
  </div>
  <div class="vocab-note trust vocab-trust-note">
    <div class="vocab-trust-grid">
      <div class="vocab-trust-item">
        <b>Integrity</b>
        <span>Bytes are intact, bounded, and well-formed.</span>
      </div>
      <div class="vocab-trust-item">
        <b>Authority</b>
        <span>After required validation, data may decide the result. A hint may only guide work.</span>
      </div>
    </div>
  </div>
</div>

<div class="source">Sources: docs/superpowers/specs/storage.md §§2, 6, 9, 11, 15–16 · docs/superpowers/specs/clock.md</div>

---

# Five crates, one dependency direction

<div class="grid wide-right">
  <div class="card cyan-top">
    <div class="eyebrow">Process-neutral engine</div>
    <h2><code>chronoxide-core</code></h2>
    <p style="font-size:17px">
      Label normalization and interning, event-time policy, head storage,
      segment writer/reader, indexes, codecs, caches, and PromQL evaluation.
    </p>
    <div style="height:16px"></div>
    <span class="pill cyan-pill">hot paths stay together</span>
  </div>
  <div class="grid two">
    <div class="card compact blue-top">
      <h3><code>chronoxide-capture</code></h3>
      <p>Leaf codec for capture files, manifests, partitions, and compression.</p>
    </div>
    <div class="card compact violet-top">
      <h3><code>chronoxide-ingester</code></h3>
      <p>Kafka/replay sources, orchestration, config, telemetry, and reports.</p>
    </div>
    <div class="card compact amber-top">
      <h3><code>chronoxide-api</code></h3>
      <p>Prometheus-compatible instant and range HTTP endpoints over sealed storage.</p>
    </div>
    <div class="card compact green-top">
      <h3><code>chronoxide-query-cli</code></h3>
      <p>Smoke queries, benchmarks, storage verification, and an independent readback oracle.</p>
    </div>
  </div>
</div>

<div style="height:16px"></div>

<div class="flow-row">
  <div class="flow-box"><b>capture</b><span>no Chronoxide crate dependency</span></div>
  <div class="flow-arrow">←</div>
  <div class="flow-box"><b>ingester</b><span>depends on capture + core</span></div>
  <div class="flow-arrow">→</div>
  <div class="flow-box"><b>core</b><span>process-neutral engine</span></div>
  <div class="flow-arrow">←</div>
  <div class="flow-box"><b>API · query CLI</b><span>read-side shells depend on core</span></div>
</div>

<div class="source">Source: docs/superpowers/specs/crate-boundaries.md</div>

---

# Time is a two-clock contract

<div class="clock-stack">
  <div class="clock-lane">
    <div class="clock-name control">
      <span class="clock-kicker">CONTROL CLOCK</span>
      <strong>captured_at_ms</strong>
      <small>Live ingest time; the recorded capture time during deterministic replay.</small>
    </div>
    <div class="policy-window">
      <div class="policy-labels">
        <span>captured − max_age</span>
        <span>captured_at_ms</span>
        <span>captured + max_lead</span>
      </div>
      <div class="policy-track">
        <div class="policy-zone reject"><b>REJECT</b><span>too old</span></div>
        <div class="policy-zone accept"><b>ACCEPT</b><span>valid event-time window</span></div>
        <div class="policy-zone reject"><b>REJECT</b><span>too far ahead</span></div>
      </div>
    </div>
  </div>

  <div class="clock-compare">compare required <code>event_ms</code> with the policy window</div>

  <div class="clock-lane">
    <div class="clock-name data">
      <span class="clock-kicker">DATA CLOCK</span>
      <strong>event_ms</strong>
      <small>The required, non-zero OTLP datapoint timestamp: where the accepted sample belongs.</small>
    </div>
    <div class="event-effects">
      <div class="event-effect"><b>Head</b><span>window membership</span></div>
      <div class="event-effect"><b>Segment</b><span>min/max time range</span></div>
      <div class="event-effect"><b>Chunk</b><span>timestamp deltas</span></div>
      <div class="event-effect"><b>Query</b><span>PromQL sample time</span></div>
    </div>
  </div>
</div>

<div class="clock-guards">
  <div class="clock-guard">
    <b>HARD REJECT</b>
    <code>timestamp_unix_nano = 0</code>
    <span>Stop before interning, watermarks, reset tracking, or storage.</span>
  </div>
  <div class="clock-guard diagnostic">
    <b>DIAGNOSTIC</b>
    <code>Kafka / source timestamp</code>
    <span>Never substitutes for event time or trusted replay time.</span>
  </div>
</div>

<div class="callout clock-summary"><strong>Storage by event time.</strong> Control by capture / ingest time.</div>

<div class="source">Source: docs/superpowers/specs/clock.md</div>

---

# The ingester: from envelope to typed sample

<div class="ingest-scopebar">
  <div class="ingest-scope-node"><b>Envelope</b> source ordered</div>
  <div class="ingest-scope-arrow">→</div>
  <div class="ingest-scope-node"><b>Datapoint</b> validated + typed</div>
  <div class="ingest-scope-arrow">→</div>
  <div class="ingest-scope-node"><b>Partition</b> mutable head</div>
  <div class="ingest-scope-arrow">→</div>
  <div class="ingest-scope-node"><b>Process</b> single segment writer</div>
</div>

<div class="ingest-grid">
  <div class="ingest-stage">
    <div class="ingest-stage-head"><span class="ingest-step">01</span><h3>Read in stable order</h3></div>
    <p>Kafka live traffic or a capture file. The optional wrapper retains raw bytes and the trusted <code>captured_at_ms</code>.</p>
  </div>

  <div class="ingest-stage">
    <div class="ingest-stage-head"><span class="ingest-step">02</span><h3>Decode the envelope</h3></div>
    <p>Walk one OTLP <code>ExportMetricsServiceRequest</code>: resource → scope → metric → datapoint.</p>
  </div>

  <div class="ingest-stage">
    <div class="ingest-stage-head"><span class="ingest-step">03</span><h3>Gate before mutation</h3></div>
    <p>Require event time, then apply age/lead policy. A rejection changes no interner, watermark, reset, or head state.</p>
  </div>

  <div class="ingest-stage">
    <div class="ingest-stage-head"><span class="ingest-step">04</span><h3>Build canonical identity</h3></div>
    <p>Merge metric name with OTLP attributes; normalize, sort, deduplicate, then intern the labelset.</p>
  </div>

  <div class="ingest-stage">
    <div class="ingest-stage-head"><span class="ingest-step">05</span><h3>Preserve the typed value</h3></div>
    <p>Keep Gauge, Sum, Histogram, ExponentialHistogram, or Summary semantics; attach typed reset hints.</p>
  </div>

  <div class="ingest-stage">
    <div class="ingest-stage-head"><span class="ingest-step">06</span><h3>Append and rotate</h3></div>
    <p>Record by event time in the partition head. Completed windows drain to the shared single-writer sealer.</p>
  </div>
</div>

<div class="ingest-notes">
  <div class="ingest-note">
    <b>Capture-only bypass</b>
    <span>Persist the raw envelope; skip decoding and processor mutation.</span>
  </div>
  <div class="ingest-note">
    <b>Replay clock</b>
    <span>Reuse recorded <code>captured_at_ms</code>—never the replay machine’s wall clock.</span>
  </div>
  <div class="ingest-note reliability">
    <b>Reliability boundary</b>
    <span>WAL/checkpoint recovery remains explicit reliability work. Sealed-segment success alone does not prove production durability.</span>
  </div>
</div>

<div class="source">Sources: chronoxide-ingester/src/{source.rs,ingester.rs,processor/otlp/pipeline.rs} · AGENTS.md</div>

---

# Write-path state machine and mutation order

<div class="mutation-flow">
  <div class="mutation-phase">
    <div class="mutation-phase-head">
      <span class="mutation-phase-no">01</span>
      <h3>Policy gate</h3>
    </div>
    <div class="phase-code">
      <div class="code-line">decision = <span class="tok-fn">policy.evaluate</span>(</div>
      <div class="code-line indent">time_unix_nano,</div>
      <div class="code-line indent">captured_at_ms,</div>
      <div class="code-line">)</div>
      <div class="code-gap"></div>
      <div class="code-line"><span class="tok-kw">if</span> decision.reject:</div>
      <div class="code-line indent">counters += <span class="tok-num">1</span></div>
      <div class="code-line indent"><span class="tok-kw">continue</span></div>
      <div class="code-gap"></div>
      <div class="code-line">event_ms = decision.accepted_ms</div>
    </div>
    <div class="phase-effect"><b>state effect</b><span>On rejection: counters only.</span></div>
  </div>

  <div class="mutation-phase">
    <div class="mutation-phase-head">
      <span class="mutation-phase-no">02</span>
      <h3>Decode + identity</h3>
    </div>
    <div class="phase-code">
      <div class="code-line">value = <span class="tok-fn">decode_otlp_value</span>()</div>
      <div class="code-line"><span class="tok-fn">validate_typed_shape</span>(value)</div>
      <div class="code-gap"></div>
      <div class="code-line">series = <span class="tok-fn">canonicalize_and_intern</span>(</div>
      <div class="code-line indent">labels,</div>
      <div class="code-line">)</div>
      <div class="code-gap"></div>
      <div class="code-line"><span class="tok-kw">if</span> number_value_missing:</div>
      <div class="code-line indent">missing_number_values += <span class="tok-num">1</span></div>
      <div class="code-line indent"><span class="tok-kw">continue</span></div>
    </div>
    <div class="phase-effect"><b>state effect</b><span>Validated shape, then interner.</span></div>
  </div>

  <div class="mutation-phase">
    <div class="mutation-phase-head">
      <span class="mutation-phase-no">03</span>
      <h3>Metadata + append</h3>
    </div>
    <div class="phase-code">
      <div class="code-line"><span class="tok-fn">stamp_reset_hint_if_typed_counter</span>(</div>
      <div class="code-line indent">series, value,</div>
      <div class="code-line">)</div>
      <div class="code-gap"></div>
      <div class="code-line">rotated = <span class="tok-fn">partition_head.record</span>(</div>
      <div class="code-line indent">series, event_ms, value,</div>
      <div class="code-line">)</div>
      <div class="code-gap"></div>
      <div class="code-line"><span class="tok-kw">if</span> rotated:</div>
      <div class="code-line indent"><span class="tok-fn">seal</span>(rotated)</div>
    </div>
    <div class="phase-effect"><b>state effect</b><span>Reset tracker → head → writer.</span></div>
  </div>
</div>

<div class="mutation-notes">
  <div class="mutation-note invariant">
    <b>Observable invariant</b>
    <span>Rejected time cannot grow cardinality or reset state. Malformed typed values cannot partially reach storage; missing numbers never become zero.</span>
  </div>
  <div class="mutation-note">
    <b>State ownership</b>
    <span>Global: interner, reset tracker, counters, writer. Partition: active/OOO windows, last timestamps, selector cache.</span>
  </div>
  <div class="mutation-note shutdown">
    <b>Deterministic drain</b>
    <span>Sort partitions, then ranges; write OOO before in-order for equal ranges.</span>
  </div>
</div>

<div class="source">Source: chronoxide-ingester/src/processor/otlp/{label_interner.rs,pipeline.rs}</div>

---

# Interning changes the allocation topology

<div class="intern-scope-head">
  <span class="intern-scope-chip">Workload motivation</span>
  <span>Historical production-shaped trace—not the controlled benchmark below.</span>
</div>

<div class="trace-metrics">
  <div class="trace-metric"><strong>11.38M</strong><span>OTLP messages</span></div>
  <div class="trace-metric"><strong>413.6M</strong><span>datapoints observed</span></div>
  <div class="trace-metric"><strong>2.62M</strong><span>unique strings interned</span></div>
  <div class="trace-metric"><strong>23.4</strong><span>labels per series observation, mean</span></div>
</div>

<div class="intern-bench-head">
  <b>Controlled microbenchmarks · generated dataset with 100,513 unique symbols</b>
  <span>Allocation snapshot + Criterion batches · CPU rows normalized per operation</span>
</div>

<div class="intern-shapes">
  <div class="intern-shape">
    <h3>Baseline backing store: per-symbol <code>Arc&lt;str&gt;</code></h3>
    <code>HashMap&lt;Arc&lt;str&gt;, SymbolId&gt; + Vec&lt;Arc&lt;str&gt;&gt;</code>
    <p>Every new string gets a separate refcounted heap allocation. The API already returns dense IDs.</p>
  </div>
  <div class="intern-shape arena-shape">
    <h3>Default backing store: packed arena</h3>
    <code>Vec&lt;u8&gt; + Vec&lt;PackedSymbolLoc&gt; + hash → SymbolId</code>
    <p>Bytes and locations grow in batches; a unique string does not require its own allocation.</p>
  </div>
</div>

<table class="benchmark-table">
  <thead>
    <tr><th>Metric</th><th class="type-header">ArcSymbolTable</th><th class="type-header">ArenaSymbolTablePacked</th><th>Interpretation</th></tr>
  </thead>
  <tbody>
    <tr><td><code>alloc_calls</code></td><td class="number">100,530</td><td class="number">18</td><td class="improvement">5,585× fewer</td></tr>
    <tr><td>Requested live bytes</td><td class="number">9.31 MiB</td><td class="number">5.75 MiB</td><td class="improvement">38.2% lower</td></tr>
    <tr><td>Internal fragmentation</td><td class="number">372,296 B</td><td class="number">8,152 B</td><td class="improvement">97.8% lower · not process RSS</td></tr>
    <tr class="cpu-start"><td><code>intern/unique</code><span class="batch-count">100,513/batch</span></td><td class="number">88.4 ns/symbol</td><td class="number">38.3 ns/symbol</td><td class="improvement">56.6% lower latency</td></tr>
    <tr><td><code>lookup/hit</code><span class="batch-count">200,000/batch</span></td><td class="number">25.4 ns/lookup</td><td class="number">28.5 ns/lookup</td><td class="tradeoff">12.1% higher latency</td></tr>
    <tr><td><code>resolve+hash</code><span class="batch-count">100,513/batch</span></td><td class="number">15.7 ns/ID</td><td class="number">14.2 ns/ID</td><td class="improvement">9.3% lower latency in this run</td></tr>
  </tbody>
</table>

<div class="callout intern-clarifier"><strong>Normalization:</strong> arena <code>lookup/hit</code> is <code>5.7041 ms ÷ 200,000 = 28.5 ns/lookup</code>; Arc is <code>5.0876 ms ÷ 200,000 = 25.4 ns/lookup</code>. The arena hit path verifies byte equality. Raw resolve alone was not isolated.</div>

<div class="source">Published results: baarse.substack.com/i/184509086/results-speed-size · benchmark harness @ 2dc78e9 · normalized values are derived from published batch timings.</div>

---

# <strong>ArenaSymbolTable</strong>: bytes once, IDs everywhere

<div class="arena-viz-path">
  <div class="arena-viz-node"><b>"service.name"</b><span>borrowed input bytes</span></div>
  <div class="arena-viz-arrow">→</div>
  <div class="arena-viz-node"><b>64-bit hash</b><span><code>hash_to_id</code> finds a candidate</span></div>
  <div class="arena-viz-arrow">→</div>
  <div class="arena-viz-node"><b>SymbolId(1)</b><span>dense <code>u32</code> candidate</span></div>
  <div class="arena-viz-arrow">→</div>
  <div class="arena-viz-node"><b>id_to_loc[1]</b><span><code>offset=8 · len=12</code></span></div>
  <div class="arena-viz-arrow">→</div>
  <div class="arena-viz-node active"><b>arena[8..20]</b><span>full byte equality ✓</span></div>
</div>

<div class="arena-viz-layout">
  <div class="arena-viz-buffer">
    <div class="arena-viz-header"><b>arena: Vec&lt;u8&gt;</b><span>one grow-only contiguous byte buffer</span></div>
    <div class="arena-viz-axis"><span>0</span><span>8</span><span>20</span><span>24 →</span></div>
    <div class="arena-viz-bytes">
      <div class="arena-viz-byte">__name__</div>
      <div class="arena-viz-byte selected">service.name</div>
      <div class="arena-viz-byte">prod</div>
      <div class="arena-viz-byte">next…</div>
    </div>
    <div class="arena-viz-slice"><code>arena[8 .. 8 + 12]</code> → <code>"service.name"</code> → compare with borrowed input</div>
  </div>

  <div class="arena-viz-locs">
    <div class="arena-viz-header"><b>id_to_loc</b><span>Vec&lt;PackedSymbolLoc&gt;</span></div>
    <div class="arena-viz-loc-head"><span>ID</span><span>offset</span><span>len</span></div>
    <div class="arena-viz-loc-row"><span>0</span><span>0</span><span>8</span></div>
    <div class="arena-viz-loc-row selected"><span>1</span><span>8</span><span>12</span></div>
    <div class="arena-viz-loc-row"><span>2</span><span>20</span><span>4</span></div>
    <div class="arena-viz-loc-foot">6 bytes / symbol · <code>u32 + u16</code></div>
  </div>
</div>

<div class="arena-viz-bottom">
  <div class="arena-viz-fanout">
    <div class="arena-viz-fanout-title">After string interning: where <code>SymbolId</code> is used</div>
    <div class="arena-viz-id">SymbolId(1)<small>only the handle travels</small></div>
    <div class="arena-viz-arrow">→</div>
    <div class="arena-viz-consumers">
      <div class="arena-viz-consumer">Labelset pairs<span>key ID · value ID</span></div>
      <div class="arena-viz-consumer">Head rows<span>compact identity</span></div>
      <div class="arena-viz-consumer">Postings<span>selector operands</span></div>
      <div class="arena-viz-consumer">Segment sealing<span>deterministic remap</span></div>
    </div>
  </div>

  <div class="arena-viz-rules">
    <div class="arena-viz-rule"><b>Hash is a hint</b>Collision IDs live in a side table; resolved byte equality is authoritative.</div>
    <div class="arena-viz-rule tradeoff"><b>Monotonic lifetime</b>Individual symbols are not deleted; reclaim by rebuilding the table.</div>
  </div>
</div>

<div class="source">Source: chronoxide-core/src/labels/symbol_table.rs</div>

---

<div class="eyebrow">From OTLP attributes to one series</div>

# <strong>FlatInternedLabelSetStore</strong>: row → SeriesRef

<div class="arena-viz-path">
  <div class="arena-viz-node labelset-viz-input">
    <b>3 labels</b>
    <span><code>__name__</code> → http.server.duration<br><code>service.name</code> → checkout<br><code>status.code</code> → 200</span>
  </div>
  <div class="arena-viz-arrow">→</div>
  <div class="arena-viz-node"><b>3 ID pairs</b><span><code>(0,3)</code> · <code>(1,4)</code> · <code>(5,6)</code></span></div>
  <div class="arena-viz-arrow">→</div>
  <div class="arena-viz-node"><b>one flat row</b><span>adjacent pairs in <code>key_values</code></span></div>
  <div class="arena-viz-arrow">→</div>
  <div class="arena-viz-node"><b>hash + row equality</b><span>reuse a match or append once</span></div>
  <div class="arena-viz-arrow">→</div>
  <div class="arena-viz-node active"><b>SeriesRef(1)</b><span>dense head-local <code>u32</code></span></div>
</div>

<div class="arena-viz-layout">
  <div class="arena-viz-buffer">
    <div class="arena-viz-header"><b>flat pair buffer · key_values</b><span>Vec&lt;InternedKeyValue&gt; · 8 bytes / pair</span></div>
    <div class="labelset-viz-axis"><span>SeriesRef(0) · offset 0</span><span>SeriesRef(1) · offset 2 · len 3</span><span>SeriesRef(2) · offset 5</span></div>
    <div class="labelset-viz-pairs">
      <div class="labelset-viz-pair"><b>(0,7)</b><span>other row</span></div>
      <div class="labelset-viz-pair"><b>(1,8)</b><span>other row</span></div>
      <div class="labelset-viz-pair row-start selected selected-start"><b>(0,3)</b><span>__name__</span><small>http.server.duration</small></div>
      <div class="labelset-viz-pair selected"><b>(1,4)</b><span>service.name</span><small>checkout</small></div>
      <div class="labelset-viz-pair selected selected-end"><b>(5,6)</b><span>status.code</span><small>200</small></div>
      <div class="labelset-viz-pair row-start"><b>(0,9)</b><span>other row</span></div>
      <div class="labelset-viz-pair"><b>(1,10)</b><span>other row</span></div>
    </div>
    <div class="arena-viz-slice"><code>SeriesRef(1)</code> → <code>series[1] = { 2, 3 }</code> → <code>key_values[2..5]</code></div>
  </div>

  <div class="arena-viz-locs">
    <div class="arena-viz-header"><b>row directory · series</b><span>Vec&lt;SeriesLoc&gt;</span></div>
    <div class="arena-viz-loc-head"><span>ref</span><span>offset</span><span>len</span></div>
    <div class="arena-viz-loc-row"><span>0</span><span>0</span><span>2</span></div>
    <div class="arena-viz-loc-row selected"><span>1</span><span>2</span><span>3</span></div>
    <div class="arena-viz-loc-row"><span>2</span><span>5</span><span>2</span></div>
    <div class="arena-viz-loc-foot">8 bytes / series · <code>u32 + u32</code></div>
  </div>
</div>

<div class="arena-viz-bottom">
  <div class="arena-viz-fanout">
    <div class="arena-viz-fanout-title">After label-row interning: where <code>SeriesRef</code> is used</div>
    <div class="arena-viz-id">SeriesRef(1)<small>the dense handle travels</small></div>
    <div class="arena-viz-arrow">→</div>
    <div class="arena-viz-consumers">
      <div class="arena-viz-consumer">Head samples<span>series-owned blocks</span></div>
      <div class="arena-viz-consumer">OOO tracking<span>last timestamp</span></div>
      <div class="arena-viz-consumer">Segment writer<span>source row</span></div>
      <div class="arena-viz-consumer">Seal ordering<span>deterministic output</span></div>
    </div>
  </div>

  <div class="arena-viz-rules">
    <div class="arena-viz-rule"><b>Hit or miss</b>AHash narrows candidates; equality reuses a ref, otherwise row + locator append once.</div>
    <div class="arena-viz-rule"><b>No Vec per series</b>Rows share one pair buffer; each SeriesRef adds only one 8-byte locator.</div>
  </div>
</div>

<div class="source">Sources: chronoxide-core/src/otlp_labelset.rs · labels/interners/flat.rs · storage.md §4.2</div>

---

# Three identities — never mix their scopes

<div class="grid three">
  <div class="card cyan-top">
    <span class="pill cyan-pill">SymbolId · u32</span>
    <h3 style="margin-top:13px">A string in one dictionary</h3>
    <p>Head interning uses runtime IDs. Sealing builds a sorted segment-local dictionary and remaps every symbol reference.</p>
    <div style="height:12px"></div>
    <p class="small"><strong class="cyan">Never compare across segments.</strong></p>
  </div>
  <div class="card blue-top">
    <span class="pill blue-pill">series_ref · u32</span>
    <h3 style="margin-top:13px">A dense row in one segment</h3>
    <p>Addresses hot series metadata, postings, and chunk locators. Assigned deterministically during sealing.</p>
    <div style="height:12px"></div>
    <p class="small"><strong class="blue">Compact and local.</strong></p>
  </div>
  <div class="card violet-top">
    <span class="pill">series_id · u64</span>
    <h3 style="margin-top:13px">Stable identity across storage</h3>
    <p>Fingerprint of the canonical labelset. Readers materialize all labels and recompute it before trusting the ID.</p>
    <div style="height:12px"></div>
    <p class="small"><strong class="violet">Stable—but always verified.</strong></p>
  </div>
</div>

<div style="height:22px"></div>

<div class="flow-row">
  <div class="flow-box"><b>Head</b><span>runtime symbols + head-local SeriesRef</span></div>
  <div class="flow-arrow">→</div>
  <div class="flow-box"><b>Seal</b><span>sort strings · remap IDs · order series</span></div>
  <div class="flow-arrow">→</div>
  <div class="flow-box"><b>Segment</b><span>segment symbols + series_ref + verified series_id</span></div>
</div>

<div class="source">Source: docs/superpowers/specs/storage.md §1 and §4.1</div>

---

# The head: windowed mutable state

<div class="grid wide-right">
  <div>
    <div class="card cyan-top">
      <h3>One head per source partition</h3>
      <p>Each holds an active event-time window, retained completed windows, OOO windows, per-series max event times, and a lazily rebuilt selector index.</p>
    </div>
    <div style="height:14px"></div>
    <div class="card compact amber-top">
      <h3>Rotation rule</h3>
      <p><code>event_ms ≥ active.end_ms</code> advances to the sample’s aligned window. An accepted per-series regression—or an event before <code>active.start_ms</code>—routes to an in-memory OOO window without rewinding it.</p>
    </div>
  </div>
  <div class="card">
    <div class="eyebrow">Per-series encoded blocks</div>
    <div class="flow-row">
      <div class="flow-box"><b>timestamps</b><span>window-relative deltas + varints</span></div>
      <div class="flow-arrow">+</div>
      <div class="flow-box"><b>numbers</b><span>raw / Gorilla / other configured codecs</span></div>
      <div class="flow-arrow">+</div>
      <div class="flow-box"><b>typed values</b><span>schema-varlen native payloads</span></div>
    </div>
    <div style="height:14px"></div>
    <div class="head-time-viz">
      <div class="head-time-head">
        <b>OTLP event-time placement · <code>SeriesRef(1)</code></b>
        <span>writer-backed window = 15m · 1–4 = arrival order</span>
      </div>
      <div class="head-time-ticks"><span>11:30</span><span>11:45</span><span>12:00</span><span>12:15</span></div>
      <div class="head-time-track">
        <div class="head-time-window previous">
          <b>earlier aligned window</b>
          <span>[11:30, 11:45)</span>
        </div>
        <div class="head-time-window current">
          <b>active until event 4</b>
          <span>[11:45, 12:00)</span>
          <div class="head-time-event" style="left:15%"><i>1</i><small>11:47</small></div>
          <div class="head-time-event late" style="left:47%"><i>3</i><small>11:52 · OOO</small></div>
          <div class="head-time-event" style="left:80%"><i>2</i><small>11:57</small></div>
        </div>
        <div class="head-time-window next">
          <b>active after event 4</b>
          <span>[12:00, 12:15)</span>
          <div class="head-time-event at-start"><i>4</i><small>12:00</small></div>
        </div>
      </div>
      <div class="head-time-rotation"><code>4: 12:00 ≥ end_ms</code><span>→ co-seal [11:45, 12:00) into <code>chunks.bin</code> · activate [12:00, 12:15)</span></div>
      <div class="head-time-caption">
        <span><strong><code>event_ms</code> chooses the window.</strong> Event 3 arrived after event 2, so it enters the in-memory OOO lane. Because it arrived before publication, it co-seals with the active samples.</span>
        <code>pre-seal → chunks.bin · post-seal → ooo_chunks.bin</code>
      </div>
    </div>
  </div>
</div>

<div style="height:16px"></div>

<div class="callout small">
  Core query APIs can merge active-head and sealed results. The current HTTP
  shell opens the immutable sealed store.
</div>

<div class="source">Sources: chronoxide-core/src/storage/head/* · chronoxide-ingester/src/processor/otlp/pipeline.rs</div>

---

# Sealing deterministically canonicalizes head state

<div class="pipeline">
  <div class="node"><b>Drain + coalesce</b><span>decode · merge pre-seal OOO · dedupe</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Canonical row order</b><span>metric · kind · labels · series ID</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Record segment</b><span>writer-local refs · append chunks · intern labels</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Finalize series refs</b><span>verify order · rewrite chunks when needed</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Remap + write</b><span>sorted symbols · series · indexes · footer</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Publish</b><span>atomic rename · manifest seal record · CURRENT</span></div>
</div>

<div style="height:24px"></div>

<div class="grid three">
  <div class="card compact cyan-top">
    <h3>Self-contained</h3>
    <p>Each segment is independently decodable: local symbols, series metadata, chunk locators, indexes, and a checksummed footer. Cross-segment precedence remains in the manifest.</p>
  </div>
  <div class="card compact blue-top">
    <h3>Immutable</h3>
    <p>Published segments never reopen. Pre-seal OOO co-seals into <code>chunks.bin</code>; post-seal OOO becomes a newer overlapping segment, merged by manifest order at read time.</p>
  </div>
  <div class="card compact violet-top">
    <h3>Replayable</h3>
    <p>The same records and order, preserved <code>captured_at_ms</code>, identical policy + head/writer config, the same format/code, and a deterministic ID seed reproduce names and bytes in a fresh root.</p>
  </div>
</div>

<div style="height:18px"></div>

<div class="grid two">
  <div class="callout small">
    <b>Deterministic boundary</b><br>
    Head symbol IDs do not persist: sealing sorts a segment-local dictionary and remaps every persisted symbol reference.
  </div>
  <div class="callout warn small">
    <b>Durability caveat</b><br>
    Rename is atomic, but segment files and the temporary directory are not yet explicitly <code>fsync</code>ed before publication.
  </div>
</div>

<div class="source">Sources: storage.md §§4.1, 6.2, 6.4.1.1, 11 · processor/otlp/{pipeline,segment_output}.rs · segment/writer/{record,ordering,seal}.rs</div>

---

# One series across a Schema 8 segment

<div class="storage-example">
  <div class="storage-example-copy">
    <b>Running example · same logical series as the interning slides</b>
    <code>http.server.duration{service.name="checkout", status.code="200"}</code>
  </div>
  <div class="storage-example-ref">
    head SeriesRef(1) → segment SeriesRef(r)
    <span><code>r</code> is the row ordinal after deterministic sealing</span>
  </div>
</div>

<div class="storage-repr">
  <div class="storage-file storage-symbols">
    <div class="storage-file-head">
      <span class="storage-filename">symbols.bin · v3</span>
      <span class="storage-role">strings ↔ IDs</span>
    </div>
    <p>One sorted, segment-local dictionary shared by series rows and selector indexes.</p>
    <div class="storage-symbol-list">
      <div class="storage-symbol"><b>S_name</b><span>"__name__"</span></div>
      <div class="storage-symbol"><b>S_metric</b><span>"http.server.duration"</span></div>
      <div class="storage-symbol"><b>S_service</b><span>"service.name"</span></div>
      <div class="storage-symbol"><b>S_checkout</b><span>"checkout"</span></div>
      <div class="storage-symbol"><b>S_status</b><span>"status.code"</span></div>
      <div class="storage-symbol"><b>S_200</b><span>"200"</span></div>
    </div>
    <div class="storage-symbol-note">
      <code>S_*</code> denotes a segment-local <code>u32</code> ordinal. The
      head IDs shown earlier are remapped when the dictionary is sorted.
    </div>
  </div>
  <div class="storage-main">
    <div class="storage-file storage-index">
      <div class="storage-file-head">
        <span class="storage-filename">indexes.puffin · v9</span>
        <span class="storage-role">label predicates → rows</span>
      </div>
      <div class="storage-index-expression">
        <code>P(S_name,S_metric) ∩ P(S_service,S_checkout) ∩ P(S_status,S_200)</code>
        <span class="storage-index-result">→ { r } <span>candidate SeriesRef</span></span>
      </div>
    </div>
    <div class="storage-file storage-series">
      <div class="storage-file-head">
        <span class="storage-filename">series.bin · v3 · row r</span>
        <span class="storage-role">what the series is + where its samples are</span>
      </div>
      <div class="storage-series-row">
        <div class="storage-series-cell">
          <b>Identity + type</b>
          <code>series_id = stored fingerprint</code>
          <code>kind_mask = Histogram (example)</code>
        </div>
        <div class="storage-series-cell">
          <b>Labels without strings</b>
          <code>keyset = [S_name, S_service, S_status]</code>
          <code>values = [S_metric, S_checkout, S_200]</code>
        </div>
        <div class="storage-series-cell">
          <b>Chunk routing</b>
          <span>usual case: one inline locator</span>
          <span>otherwise: one overflow-blob pointer</span>
        </div>
      </div>
    </div>
    <div class="storage-payload-row">
      <div class="storage-routes">
        <div class="storage-route">
          <span class="storage-route-tag">inline</span>
          <div class="storage-route-copy">
            <b>locator lives in row r</b>
            <span>one chunk · one kind/lane · fields fit</span>
          </div>
          <span class="storage-route-arrow">→</span>
        </div>
        <div class="storage-route overflow">
          <span class="storage-route-tag">overflow</span>
          <div class="storage-route-copy">
            <b>chunk_index.bin · v2</b>
            <span>checked entries: time · kind · lane · offset · length</span>
          </div>
          <span class="storage-route-arrow">→</span>
        </div>
      </div>
      <div class="storage-file storage-chunks">
        <div class="storage-file-head">
          <span class="storage-filename">chunks.bin / ooo_chunks.bin</span>
          <span class="storage-role">one selected lane · sample bytes</span>
        </div>
        <div class="storage-lanes">
          <span class="storage-lane">chunks.bin · ordinary + pre-seal OOO coalesced</span>
          <span class="storage-lane ooo">ooo_chunks.bin · post-seal-only overlap</span>
        </div>
        <div class="storage-chunk-record">
          <code>FrameHeader → ChunkHeader { series_ref: r, kind/encoding, min/max, point_count, lengths, CRC }</code>
          <code>optional Count/Sum scalar lane → native { timestamps[] + typed values[] }</code>
          <span>No metric name or labels are repeated in the chunk.</span>
        </div>
      </div>
    </div>
  </div>
</div>

<div class="storage-governance">
  <div><b>meta.json</b><span>segment event-time bounds and counts support coarse pruning</span></div>
  <div><b>footer.bin</b><span>exact size + XXH64 inventory binds every non-footer segment artifact</span></div>
</div>

<div class="source">Source: docs/superpowers/specs/storage.md §§4.1, 6.3–6.4, 8, 11, 15–16</div>

---

# Physical layout of a Schema 8 segment

<table class="spec-table">
  <thead>
    <tr>
      <th>Artifact</th>
      <th>Root / record contract</th>
      <th>Paging / payload granularity</th>
      <th>Local integrity boundary</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>symbols.bin v3</td>
      <td>80 B header · 48 B descriptor</td>
      <td>greedy ≈32 KiB variable pages; complete first/last fences</td>
      <td>root CRC32C authenticates descriptors; one CRC per string page</td>
    </tr>
    <tr>
      <td>series.bin v3</td>
      <td>176 B header · 16 B hot/cold descriptor</td>
      <td>16 KiB hot pages · 409 × 40 B records/page</td>
      <td>root CRC, hot-page CRC, and CRC-described 16 KiB cold ranges</td>
    </tr>
    <tr>
      <td>SeriesHotV3</td>
      <td>40 B fixed record</td>
      <td>inline usual one-chunk locator; otherwise overflow</td>
      <td>indexed prefix CRC binds locator to chunk/scalar headers</td>
    </tr>
    <tr>
      <td>chunk_index.bin v2</td>
      <td>64 B root · 32 B overflow-blob header</td>
      <td>44 B per overflow chunk entry</td>
      <td>root CRC + complete blob CRC; canonical series_ref order</td>
    </tr>
    <tr>
      <td>chunk frame</td>
      <td>14 B frame · 40 B chunk · optional 16 B scalar header</td>
      <td>currently one individually addressable chunk per frame</td>
      <td>frame/header agreement, prefix CRC, scalar CRC, native payload CRC</td>
    </tr>
    <tr>
      <td>indexes.puffin v9</td>
      <td>16 B header · 256 B trailer · 48 B directory records</td>
      <td>16 KiB exact-directory pages; variable postings/FST/range payloads</td>
      <td>root/directory CRC chain + expected count + payload CRC</td>
    </tr>
    <tr>
      <td>footer.bin schema 8</td>
      <td>164 B · seven 20 B file entries</td>
      <td>whole segment artifact inventory</td>
      <td>exact size + XXH64 for every non-footer file</td>
    </tr>
  </tbody>
</table>

<div style="height:13px"></div>

<div class="callout small">
  These sizes are versioned bytes, not implementation hints. Changing them
  requires a new format boundary and deterministic replay of old corpora.
</div>

<div class="source">Sources: storage.md · schema7-inline-series-design.md · schema8-adaptive-postings-design.md</div>

---

# The common case is one checked hop

<div class="grid wide-left">
  <div class="card">
    <div class="eyebrow">Single-chunk series</div>
    <div class="small muted" style="margin:-2px 0 14px">
      <b class="amber">Why inline it?</b> In the measured corpus, all 47.8M
      segment-local series rows had one chunk, averaging 10.48 datapoints.
      A logical series can reappear in every segment. Inlining this dominant
      locator avoids a <code>chunk_index.bin</code> lookup; multi-chunk rows use overflow.
    </div>
    <div class="flow-row">
      <div class="flow-box"><b><code>series_ref</code></b><span>dense row number</span></div>
      <div class="flow-arrow">→</div>
      <div class="flow-box"><b>40-byte hot record</b><span>label location + inline locator</span></div>
      <div class="flow-arrow">→</div>
      <div class="flow-box"><b>chunk bytes</b><span>header + scalar lane? + payload</span></div>
    </div>
    <div style="height:18px"></div>
    <div class="codebox">SeriesHotV3
  series_id        8 B
  keyset_id,row    8 B
  control          4 B   // kind mask + tag + lane len
  inline payload  20 B   // time deltas, offset, len, CRC
                  ────
                  40 B</div>
  </div>
  <div>
    <div class="card cyan-top">
      <h3>Inline is canonical when</h3>
      <ul>
        <li>exactly one chunk</li>
        <li>one kind and one lane</li>
        <li>time/offset/length fields fit</li>
        <li>indexed prefix CRC agrees</li>
      </ul>
    </div>
    <div style="height:14px"></div>
    <div class="card amber-top">
      <h3>Overflow when needed</h3>
      <p>Multiple chunks, mixed kinds/lanes, or width exceptions point to one complete checksummed blob in <code>chunk_index.bin</code>.</p>
    </div>
  </div>
</div>

<div style="height:16px"></div>

<div class="callout small">
  Lazy does not mean permissive: any touched checksum, bounds, count, ordering,
  or header disagreement is corruption—not “no result.”
</div>

<div class="source">Source: docs/superpowers/specs/archive/storage/2026-07-13-storage-schema7-inline-series-design.md</div>

---

# Read only what the query needs. Trust only validated bytes.

<div class="pipeline">
  <div class="node"><b>Footer inventory</b><span>schema + exact file sizes/hashes</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Validated root</b><span>bounds + descriptors + CRC chain</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Touched page/blob</b><span>admit bounds · CRC · parse</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Generation-bound object</b><span>decoded values + provenance</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Query decision</b><span>membership · prune · materialize</span></div>
</div>

<div style="height:22px"></div>

<div class="state-table">
  <div class="head-cell">Question</div>
  <div class="head-cell">Normal timed query</div>
  <div class="head-cell">Explicit full validation</div>

  <div class="row-head">What gets read?</div>
  <div>Immutable roots plus only pages, postings, label rows, and chunks selected by this plan.</div>
  <div>Every footer-tracked file and every root/page/blob, outside timed query measurements.</div>

  <div class="row-head">What is authoritative?</div>
  <div>Exact postings and validated rows. Routing/metric summaries prune only while a matching same-generation capability is held.</div>
  <div>Complete semantic validation can mint the capability after proving summary agreement.</div>

  <div class="row-head">What enters cache?</div>
  <div>Only completely validated immutable objects, charged to aggregate metadata budgets.</div>
  <div>Validation may run with zero retention; success is proof, not cache warmth.</div>
</div>

<div style="height:17px"></div>

<div class="grid two">
  <div class="callout redline small">
    Structural corruption is sticky for that page/blob and generation.
  </div>
  <div class="callout small">
    Reads are positional and immutable—no shared seek cursor between query sessions.
  </div>
</div>

<div class="source">Sources: storage.md §9, §15–16 · schema7-inline-series-design.md §reader/governor</div>

---

# Native typed values stay native on disk

<div class="chunk-anatomy">
  <div class="chunk-anatomy-node frame">
    <span class="chunk-anatomy-kicker">physical wrapper</span>
    <b>FrameHeader · 14 B</b>
    <span>Currently one chunk per frame; the locator addresses past this wrapper.</span>
  </div>
  <div class="chunk-anatomy-arrow">→</div>
  <div class="chunk-anatomy-node header">
    <span class="chunk-anatomy-kicker">individually addressed record</span>
    <b>ChunkHeader · 40 B</b>
    <div class="chunk-header-groups">
      <span>kind · encoding · flags</span>
      <span>series_ref</span>
      <span>min/max time · point count</span>
      <span>lengths · native payload CRC</span>
    </div>
  </div>
  <div class="chunk-anatomy-arrow">→</div>
  <div class="chunk-anatomy-node scalar">
    <span class="chunk-anatomy-kicker">optional fast path</span>
    <b>TypedScalarLane?</b>
    <span>HIST / EXPHIST / SUMMARY only: count and optional sum without native decode.</span>
  </div>
  <div class="chunk-anatomy-arrow">→</div>
  <div class="chunk-anatomy-node native">
    <span class="chunk-anatomy-kicker">authoritative values</span>
    <b>Native payload</b>
    <span>Timestamp stream, reusable schemas, and type-specific sample bodies.</span>
  </div>
</div>

<div class="chunk-payload-grid">
  <div class="chunk-payload-card number">
    <span class="chunk-payload-kind">FLOAT / INT64</span>
    <h3>Number values</h3>
    <div class="chunk-payload-shape">
      <div><b>Timestamps</b><span><code>t0</code> plus ordered deltas.</span></div>
      <div><b>Values</b><span>Raw/Gorilla <code>f64</code> or raw/delta-ZigZag <code>i64</code>.</span></div>
    </div>
    <div class="chunk-number-gap">
      <b>Known gap:</b> Gauge versus Sum, number start time/flags, Sum
      temporality, and monotonicity are not persisted yet. Missing values are
      rejected—not written as zero.
    </div>
  </div>

  <div class="chunk-payload-card hist">
    <span class="chunk-payload-kind">HIST</span>
    <h3>Histogram</h3>
    <div class="chunk-payload-shape">
      <div><b>Reusable schema</b><span>Finite, ordered explicit bounds.</span></div>
      <div><b>Each sample</b><span>Typed metadata · count · optional sum/min/max · bucket counts.</span></div>
    </div>
  </div>

  <div class="chunk-payload-card exphist">
    <span class="chunk-payload-kind">EXPHIST</span>
    <h3>ExponentialHistogram</h3>
    <div class="chunk-payload-shape">
      <div><b>Reusable schema</b><span>Scale plus zero threshold.</span></div>
      <div><b>Each sample</b><span>Typed metadata · count · optional sum/min/max · zero and positive/negative bucket spans.</span></div>
    </div>
  </div>

  <div class="chunk-payload-card summary">
    <span class="chunk-payload-kind">SUMMARY</span>
    <h3>Summary</h3>
    <div class="chunk-payload-shape">
      <div><b>Reusable schema</b><span>Strictly ordered quantile positions.</span></div>
      <div><b>Each sample</b><span>Typed metadata · count · sum · aligned quantile values.</span></div>
    </div>
  </div>
</div>

<div class="chunk-semantics-bottom">
  <div>
    <b>TypedSampleMetadata</b>
    <span>Histogram, ExponentialHistogram, and Summary retain OTLP flags,
      temporality, reset hint, and optional <code>start_time_ms</code> in every
      native sample—and in the scalar lane when present.</span>
  </div>
  <div class="stale">
    <b>Exact staleness</b>
    <span>Typed <code>NO_RECORDED_VALUE</code> projects to the stale-NaN
      sentinel. For numbers, only that exact bit pattern is stale; ordinary
      <code>NaN</code> and <code>±Inf</code> remain values.</span>
  </div>
</div>

<div class="source">Source: docs/superpowers/specs/storage.md §§11.1–11.4, 12</div>

---

# Indexes turn matchers into set operations

<div class="codebox">{ namespace="prod", pod=~"api-.*", zone!="us-east-1a" }</div>

<div style="height:17px"></div>

<div class="pipeline">
  <div class="node"><b>Normalize</b><span>metric + label names</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Prune segments</b><span>time + authorized routing facts</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Inverted index</b><span>postings for <code>namespace=prod</code></span></div>
  <div class="arrow">∩</div>
  <div class="node"><b>FST + union</b><span>values matching <code>api-.*</code></span></div>
  <div class="arrow">−</div>
  <div class="node"><b>Deferred predicate</b><span>negation + absent-label rules</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>series_ref set</b><span>candidate rows only</span></div>
</div>

<div style="height:22px"></div>

<div class="grid three">
  <div class="card cyan-top compact">
    <h3>Inverted index (postings)</h3>
    <p>Each exact <code>(label, value)</code> maps to an ordered <code>SeriesRef</code> postings list. Schema 8 encodes it as RAW32 or delta-ULEB128; raw wins ties.</p>
  </div>
  <div class="card blue-top compact">
    <h3>Regex values</h3>
    <p>Safe-prefix FST traversal visits candidate values and checks the regex; broad expansions hit an explicit examined-value budget.</p>
  </div>
  <div class="card amber-top compact">
    <h3>Absence is semantic</h3>
    <p>Every matcher also evaluates the missing label as <code>""</code>; postings alone are not always complete.</p>
  </div>
</div>

<div class="source">Sources: storage.md §15 · schema8-adaptive-postings-design.md</div>

---

# Postings reverse the lookup: label term → candidate series

<div class="postings-deep-grid">
  <div class="postings-deep-panel">
    <div class="postings-deep-head">
      <b>Forward view · series rows</b>
      <span><code>SeriesRef</code> → labels</span>
    </div>
    <div class="postings-deep-row"><code>r1</code><span>{ namespace=prod, pod=api-1 }</span></div>
    <div class="postings-deep-row"><code>r4</code><span>{ namespace=prod, pod=api-canary }</span></div>
    <div class="postings-deep-row"><code>r7</code><span>{ namespace=prod, pod=api-1 }</span></div>
    <div class="postings-deep-row"><code>r10</code><span>{ namespace=prod, pod=api-worker-7 }</span></div>
  </div>

  <div class="postings-invert">
    <span>index at<br>seal</span>
    <b>→</b>
  </div>

  <div class="postings-deep-panel inverted">
    <div class="postings-deep-head">
      <b>Inverted index · postings</b>
      <span>exact term → list</span>
    </div>
    <div class="postings-deep-row"><code>(namespace, "prod")</code><code>A = [1, 4, 7, 10]</code></div>
    <div class="postings-deep-row"><code>(pod, "api-1")</code><code>B = [1, 7]</code></div>
    <div class="postings-deep-row"><code>(pod, "api-canary")</code><code>[4]</code></div>
    <div class="postings-deep-row"><code>(pod, "api-worker-7")</code><code>[10]</code></div>
  </div>
</div>

<div class="postings-query-strip">
  <div class="postings-query-node">
    <b>Exact selector</b>
    <code>{namespace="prod", pod="api-1"}</code>
  </div>
  <div class="postings-query-arrow">→</div>
  <div class="postings-query-node">
    <b>Read two lists · intersect</b>
    <code>A ∩ B = { 1, 7 }</code>
    <span>No full-series label scan.</span>
  </div>
  <div class="postings-query-arrow">→</div>
  <div class="postings-query-node result">
    <b>Read candidate data only</b>
    <code>series.bin → locators → chunks</code>
    <span>Labels, time, and samples live elsewhere.</span>
  </div>
</div>

<div class="postings-deep-details">
  <div class="postings-deep-detail">
    <b><code>SeriesRef</code> is the operand</b>
    <p>A dense segment-local <code>u32</code> assigned at seal—not a global
      series ID, timestamp, or sample. Each segment performs its own set
      algebra.</p>
  </div>
  <div class="postings-deep-detail sorted">
    <b>Sorted lists merge linearly</b>
    <p>Strictly increasing references let two cursors compute intersection or
      union in <code>O(|A| + |B|)</code>, without building a hash table.</p>
  </div>
  <div class="postings-deep-detail encoded">
    <b>Schema 8 chooses per list</b>
    <div class="postings-codec-example">
      <span>refs</span><code>[1, 4, 7, 10]</code>
      <span>RAW32</span><code>4 B header + 16 B body</code>
      <span>delta</span><code class="delta">[1, 3, 3, 3] → 4 B + 4 B</code>
    </div>
    <p>First ref is absolute; later values are positive ULEB128 gaps.
      Delta wins only when strictly smaller; RAW32 wins ties.</p>
  </div>
</div>

<div class="source">Sources: storage.md §§6.4.1, 15.1.3, 15.2–15.3 · schema8-adaptive-postings-design.md</div>

---

# FST finds label values. The inverted index finds series.

<div class="matcher-example-bar">
  <span class="matcher-example-label">same selector</span>
  <span class="matcher-selector-brace">{</span>
  <span class="matcher-token">namespace="prod"<span>exact equality</span></span>
  <span class="matcher-token regex">pod=~"api-.*"<span>positive regex</span></span>
  <span class="matcher-token negative">zone!="us-east-1a"<span>negative + absence</span></span>
  <span class="matcher-selector-brace">}</span>
</div>

<div class="matcher-explain-grid">
  <div class="matcher-explain-card">
    <div class="matcher-explain-head">
      <b>Exact equality: one inverted-index lookup</b>
      <span>value is already known</span>
    </div>
    <div class="matcher-exact-path">
      <div class="matcher-query-node">
        <code>namespace="prod"</code>
        <span>No value discovery is needed.</span>
      </div>
      <div class="matcher-path-arrow">→</div>
      <div class="matcher-posting-node">
        <b>postings(namespace, prod)</b>
        <code>A = { 1, 4, 7, 10 }</code>
        <span>Sorted segment-local <code>series_ref</code>s.</span>
      </div>
    </div>
    <div class="matcher-posting-definition">
      <b>Inverted index:</b> each exact
      <code>(label name, label value)</code> term maps to one sorted postings
      list of segment-local <code>series_ref</code>s. Conjunction becomes set
      intersection.
    </div>
  </div>

  <div class="matcher-explain-card regex">
    <div class="matcher-explain-head">
      <b>Regex: discover values, then union postings</b>
      <span>value is a set</span>
    </div>
    <div class="matcher-fst-flow">
      <div class="matcher-query-node">
        <code>pod=~"api-.*"</code>
        <span>The matcher names no single exact value.</span>
      </div>
      <div class="matcher-path-arrow">→</div>
      <div class="matcher-fst">
        <div class="matcher-fst-head">
          <b>pod value FST</b>
          <span>compact prefix-sharing dictionary</span>
        </div>
        <div class="matcher-fst-prefix">
          <b>api-</b>
          <div class="matcher-fst-branches">
            <span>1</span><span>canary</span><span>worker-7</span>
          </div>
        </div>
        <div class="matcher-fst-misses">pruned: database-1 · kube-proxy</div>
      </div>
    </div>
    <div class="matcher-regex-postings">
      <div><b>api-1</b><span>→ { 1, 7 }</span></div>
      <div><b>api-canary</b><span>→ { 4, 8 }</span></div>
      <div><b>api-worker-7</b><span>→ { 10 }</span></div>
    </div>
    <div class="matcher-union"><b>resolve via symbols.bin · union postings</b> → B = { 1, 4, 7, 8, 10 }</div>
  </div>
</div>

<div class="matcher-set-flow">
  <div class="matcher-set-box"><b>Exact result · A</b><code>{ 1, 4, 7, 10 }</code></div>
  <div class="matcher-set-op">∩</div>
  <div class="matcher-set-box"><b>Regex result · B</b><code>{ 1, 4, 7, 8, 10 }</code></div>
  <div class="matcher-set-op">=</div>
  <div class="matcher-set-box"><b>Positive base</b><code>{ 1, 4, 7, 10 }</code></div>
  <div class="matcher-set-op">→</div>
  <div class="matcher-set-box negative">
    <b>Evaluate <code>zone!="us-east-1a"</code></b>
    <span><code>r4</code> is east → drop · <code>r10</code> is absent → compare <code>""</code> → keep</span>
  </div>
  <div class="matcher-set-op">→</div>
  <div class="matcher-set-box final"><b>Final rows</b><code>{ 1, 7, 10 }</code></div>
</div>

<div class="matcher-regex-guardrail">
  <b>Broad-regex guardrail</b>
  <span>The current reader returns <code>QuotaExceeded</code> after 100,000
    examined values by default. The specified series-driven fallback over an
    already-selective base is not implemented yet.</span>
</div>

<div class="source">Sources: storage.md §§15.2–15.3 · segment/query_reader/facade.rs · query_types/limits.rs</div>

---

# Query engine: plan → read → merge → evaluate

<div class="pipeline">
  <div class="node"><b>PromQL text</b><span>instant or range request</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Parse + lower</b><span>supported internal AST</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Rewrite selectors</b><span>native + virtual projections</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Plan metadata</b><span>segments · symbols · postings</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Read chunks</b><span>batched pread / io_uring</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Merge + eval</b><span>dedupe · functions · operators</span></div>
</div>

<div style="height:22px"></div>

<div class="grid two">
  <div class="card cyan-top">
    <h3>Demand-driven reads</h3>
    <p>Immutable positional readers load only required symbol/index pages and chunk spans. Shared caches retain only validated data under explicit budgets.</p>
  </div>
  <div class="card red-top">
    <h3>Guardrails are part of the plan</h3>
    <p>Matched series, projected series, chunk requests, logical bytes, decoded samples, and regex-expanded values are charged deterministically.</p>
  </div>
</div>

<div style="height:16px"></div>

<div class="callout warn small">
  Production range queries evaluate the instant expression independently at
  each step. The one-pass scalar path is an explicit diagnostic comparator,
  not the production default.
</div>

<div class="source">Sources: storage.md §16 · chronoxide-core/src/storage/segment/query_* · promql/*</div>

---

# Projection: compatibility without lossy ingest

<div class="projection-map">
  <div class="projection-native-card">
    <div class="projection-card-kicker">Stored once · illustrative native Histogram sample</div>
    <div class="projection-metric-line">
      <code>request.duration</code>
      <span>normalizes to</span>
      <code class="normalized">request_duration</code>
    </div>
    <div class="projection-metadata">
      <div><span>event time</span><b>12:00:00</b></div>
      <div><span>start time</span><b>11:55:00</b></div>
      <div><span>temporality</span><b>CUMULATIVE</b></div>
      <div><span>flags · reset meta</span><b>0 · retained</b></div>
    </div>
    <div class="projection-aggregates">
      <div class="projection-aggregate"><span>count</span><b>10</b></div>
      <div class="projection-aggregate"><span>optional sum <small>present</small></span><b>4.4 s</b></div>
    </div>
    <div class="projection-native-buckets">
      <div class="projection-native-bucket-row"><span>OTLP bucket range</span><b>per-range count</b></div>
      <div class="projection-native-bucket-row"><span>(−Inf, 0.1]</span><b>3</b></div>
      <div class="projection-native-bucket-row"><span>(0.1, 0.5]</span><b>4</b></div>
      <div class="projection-native-bucket-row"><span>(0.5, 1]</span><b>2</b></div>
      <div class="projection-native-bucket-row"><span>(1, +Inf)</span><b>1</b></div>
    </div>
    <div class="projection-native-checks">
      <div class="projection-native-check"><b>shape check</b><code>3 bounds → 4 buckets</code></div>
      <div class="projection-native-check"><b>count check</b><code>3 + 4 + 2 + 1 = 10</code></div>
      <div class="projection-native-check"><b>reset metadata</b><span>derived / retained by Chronoxide</span></div>
    </div>
  </div>
  <div class="projection-fanout">
    <span>project at</span>
    <b>query time</b>
    <div class="projection-fanout-arrows"><i>↗</i><i>↘</i></div>
    <small>no duplicate<br>ingest</small>
  </div>
  <div class="projection-output-stack">
    <div class="projection-output-card native">
      <div class="projection-output-head">
        <b>Native typed path</b>
        <span>still one histogram element</span>
      </div>
      <div class="projection-native-functions">
        <div class="projection-native-function"><code>histogram_count</code><b>10</b></div>
        <div class="projection-native-function"><code>histogram_sum</code><b>4.4 s</b></div>
        <div class="projection-native-function"><code>histogram_avg</code><b>0.44 s</b></div>
      </div>
      <div class="projection-native-shape">
        <code>histogram_quantile</code> and <code>histogram_fraction</code>
        consume the same typed bucket shape.
      </div>
    </div>
    <div class="projection-output-card classic">
      <div class="projection-output-head">
        <b>Classic virtual series</b>
        <span>emitted on demand · not persisted copies</span>
      </div>
      <div class="projection-classic-scalars">
        <div class="projection-classic-scalar"><code>request_duration_count</code><b>10</b></div>
        <div class="projection-classic-scalar"><code>request_duration_sum</code><b>4.4 s</b></div>
      </div>
      <div class="projection-prefix-line">
        <span>per-range counts [3, 4, 2, 1]</span>
        <b>prefix sum →</b>
        <span><code>request_duration_bucket</code></span>
      </div>
      <div class="projection-classic-buckets">
        <div class="projection-classic-bucket"><code>le="0.1"</code><b>3</b></div>
        <div class="projection-classic-bucket"><code>le="0.5"</code><b>7</b></div>
        <div class="projection-classic-bucket"><code>le="1"</code><b>9</b></div>
        <div class="projection-classic-bucket inf"><code>le="+Inf"</code><b>10 = _count</b></div>
      </div>
      <div class="projection-classic-note">Absent optional sum ⇒ no virtual <code>_sum</code> series.</div>
    </div>
  </div>
</div>

<div class="projection-contract">
  <div>
    <b>Stored once. Projected only when the query asks.</b>
    <span>The native typed value remains the source of truth.</span>
  </div>
  <div class="projection-contract-arrow">→</div>
  <div>
    <b>Temporality still governs the result.</b>
    <span>CUMULATIVE remains cumulative. Validated DELTA intervals are
      accumulated into cumulative-shaped PromQL counters; raw deltas are never
      exposed.</span>
  </div>
</div>

<div class="source">Sources: storage.md §11.5 · docs/promql-coverage.md</div>

---

# Delta histogram evaluation crosses three domains

<div class="delta-gate">
  <div class="delta-gate-side"><b>Selected record gate</b><span>Select by interval intersection; every non-stale datapoint must be valid.</span></div>
  <div class="delta-gate-condition"><code>start_time_ms &lt; timestamp_ms</code></div>
  <div class="delta-gate-side stale"><b>Stale gap</b><span><code>NO_RECORDED_VALUE</code> has no interval and is exempt.</span></div>
</div>

<div class="delta-domain-map">
  <div class="delta-domain-axis">
    <b>logical time →</b><span>pre-range</span><span>t₁</span><span>t<sub>gap</sub></span><span>t₂</span><span>t₃</span>
  </div>
  <div class="delta-domain-row stored">
    <div class="delta-domain-label"><span>1 · stored domain</span><b>Delta interval records</b><small>validated typed input</small></div>
    <div class="delta-domain-track">
      <div class="delta-domain-cell empty"><b>seed source</b><span>[s₀, t₀) · outside range</span></div>
      <div class="delta-domain-cell"><b>Δ₁</b><span>[s₁, t₁)</span></div>
      <div class="delta-domain-cell stale"><b>stale</b><span>gap · no interval</span></div>
      <div class="delta-domain-cell"><b>Δ₂</b><span>[s₂, t₂)</span></div>
      <div class="delta-domain-cell"><b>Δ₃</b><span>[s₃, t₃)</span></div>
    </div>
  </div>
  <div class="delta-domain-row virtual">
    <div class="delta-domain-label"><span>2 · projection domain</span><b>Virtual cumulative surface</b><small>illustrative compatible fragment</small></div>
    <div class="delta-domain-track">
      <div class="delta-domain-cell seed"><b>C₀</b><span>aligned seed only</span></div>
      <div class="delta-domain-cell"><b>C₁</b><span>C₀ + Δ₁</span></div>
      <div class="delta-domain-cell stale-marker"><b>stale-NaN</b><span>preserved marker</span></div>
      <div class="delta-domain-cell"><b>C₂</b><span>Δ₂ · hint Unknown</span></div>
      <div class="delta-domain-cell"><b>C₃</b><span>Δ₂ + Δ₃</span></div>
    </div>
  </div>
  <div class="delta-domain-row promql">
    <div class="delta-domain-label"><span>3 · evaluation domain</span><b>Original logical range</b><small>reset-aware rate / increase</small></div>
    <div class="delta-domain-track">
      <div class="delta-range-seed"><b>C₀</b>subtraction seed<br>not selected</div>
      <div class="delta-range-band"><b>Evaluate C₁, C₂, C₃ · omit only the exact stale marker · keep the original duration</b></div>
    </div>
  </div>
</div>

<div class="delta-domain-contracts">
  <div class="delta-domain-contract">
    <b>Count and buckets</b>
    <p>Become cumulative-shaped before counter math. Native multi-sample
      extrapolation and virtual interval aggregation are separate tested
      algorithms. Continuous intervals stitch; gaps or overlaps make logical
      boundaries.</p>
  </div>
  <div class="delta-domain-contract reset">
    <b>Stale and reset semantics</b>
    <p>Stale restarts only the projection accumulator; the marker itself is not
      a reset. Its first synthetic post-gap hint is <code>Unknown</code>;
      stored cumulative/unknown-temporality hints remain authoritative.</p>
  </div>
  <div class="delta-domain-contract sum">
    <b>Optional signed sum</b>
    <p>Native and virtual paths both add valid signed IEEE intervals.
      Negative, <code>NaN</code>, or <code>±Inf</code> sums do not invalidate
      count/bucket results.</p>
  </div>
</div>

<div class="source">Sources: storage.md §§11.5, 13.3 · docs/promql-coverage.md OTLP temporality</div>

---

# Correctness lives in the ugly edges

<div class="grid two">
  <div class="card cyan-top">
    <span class="pill cyan-pill">staleness</span>
    <h3 style="margin-top:11px">Omit exactly one sentinel</h3>
    <p><code>rate()</code>/<code>increase()</code> omit the exact stale marker without shortening the logical range or inventing a reset.</p>
  </div>
  <div class="card violet-top">
    <span class="pill">typed counters</span>
    <h3 style="margin-top:11px">Stored hints stay authoritative</h3>
    <p>Temporality, start time, flags, and reset hints cross storage boundaries and survive virtual projection.</p>
  </div>
  <div class="card blue-top">
    <span class="pill blue-pill">duplicates</span>
    <h3 style="margin-top:11px">One deterministic winner</h3>
    <p>Head &gt; sealed; newer segment &gt; older; OOO lane &gt; in-order; later chunk entry wins within a lane.</p>
  </div>
  <div class="card red-top">
    <span class="pill red-pill">corruption</span>
    <h3 style="margin-top:11px">Never degrade into absence</h3>
    <p>A touched parse, checksum, bounds, ordering, count, or identity failure is an error—not pruning, cache miss, or empty output.</p>
  </div>
</div>

<div style="height:18px"></div>

<div class="callout">
  The fastest wrong answer is still wrong. Chronoxide treats semantic metadata
  and integrity metadata as part of the query result.
</div>

<div class="source">Sources: AGENTS.md core semantics · storage.md §8, §14, §16.5</div>

---

# PromQL surface today

<div class="grid three">
  <div class="card green-top">
    <div class="status-row">
      <div class="status-label solid">solid</div>
      <div class="status-copy">Instant query API; scalar and vector selectors with <code>=</code>, <code>!=</code>, <code>=~</code>, <code>!~</code>.</div>
    </div>
    <div class="status-row">
      <div class="status-label solid">solid</div>
      <div class="status-copy">Arithmetic, comparisons, set operators, common aggregations, label/math/calendar helpers.</div>
    </div>
    <div class="status-row">
      <div class="status-label solid">solid</div>
      <div class="status-copy">A broad family of range functions, including <code>rate</code>, <code>increase</code>, <code>changes</code>, and <code>resets</code>.</div>
    </div>
  </div>
  <div class="card amber-top">
    <div class="status-row">
      <div class="status-label partial">partial</div>
      <div class="status-copy">Range query parity: production repeated-step execution is established; deeper compositions still need expansion.</div>
    </div>
    <div class="status-row">
      <div class="status-label partial">partial</div>
      <div class="status-copy">Native histogram operators, vector-matching edges, lookback/staleness composition, and histogram-aware aggregation.</div>
    </div>
    <div class="status-row">
      <div class="status-label partial">partial</div>
      <div class="status-copy">Classic histogram, native Histogram/ExponentialHistogram, and Summary projections are useful but not complete Prometheus parity.</div>
    </div>
  </div>
  <div class="card red-top">
    <div class="status-row">
      <div class="status-label gap">gap</div>
      <div class="status-copy"><code>@</code> timestamp modifier is not lowered.</div>
    </div>
    <div class="status-row">
      <div class="status-label gap">gap</div>
      <div class="status-copy">PromQL subqueries <code>[range:resolution]</code> are unsupported.</div>
    </div>
    <div class="status-row">
      <div class="status-label gap">gap</div>
      <div class="status-copy">Complete WAL/checkpoint/recovery behavior remains explicit reliability work outside the query matrix.</div>
    </div>
  </div>
</div>

<div style="height:18px"></div>

<div class="callout small">
  “Partial” is a tracked compatibility status, not a euphemism for silent best effort.
</div>

<div class="source">Source: docs/promql-coverage.md (current compatibility matrix)</div>

---

# Deliberate simplifications—and the next design arguments

<div class="grid four">
  <div class="card cyan-top compact">
    <span class="pill cyan-pill">write path</span>
    <h3 style="margin-top:11px">Single writer</h3>
    <p>Per ingestion worker/shard to avoid coordination in the seal hot path.</p>
  </div>
  <div class="card blue-top compact">
    <span class="pill blue-pill">frames</span>
    <h3 style="margin-top:11px">One chunk today</h3>
    <p>Frames are ready for packing, but the current writer emits one addressable chunk per frame.</p>
  </div>
  <div class="card violet-top compact">
    <span class="pill">postings</span>
    <h3 style="margin-top:11px">Decode before sets</h3>
    <p>Schema 8 compresses lists on disk; query set operations still consume governed decoded <code>u32</code> refs.</p>
  </div>
  <div class="card amber-top compact">
    <span class="pill amber-pill">range queries</span>
    <h3 style="margin-top:11px">Repeat instant eval</h3>
    <p>Production semantics are simple and proven; one-pass scalar execution remains diagnostic.</p>
  </div>
</div>

<div style="height:15px"></div>

<div class="grid four">
  <div class="card compact red-top">
    <h3>No mixed formats</h3>
    <p>Strict homogeneous schema policy; migrations replay experimental corpora.</p>
  </div>
  <div class="card compact red-top">
    <h3>Near store first</h3>
    <p>Object-store blocks, distributed routing, and global compaction are outside this local format.</p>
  </div>
  <div class="card compact red-top">
    <h3>Recovery is not assumed</h3>
    <p>WAL/checkpoint/truncation and interrupted shutdown still require end-to-end proof.</p>
  </div>
  <div class="card compact red-top">
    <h3>API reads sealed data</h3>
    <p>Core supports head-aware queries; the current server shell opens immutable segments.</p>
  </div>
</div>

<div style="height:15px"></div>

<div class="callout small">
  The useful review questions are therefore concrete: encoded-domain postings,
  packed frames, queryable-head serving, one-pass admission, recovery, and long-term layout.
</div>

<div class="source">Sources: storage.md current implementation notes · crate-boundaries.md · one-pass-range-execution-design.md</div>

---

# A proof ladder—not a single green test

<div class="pipeline">
  <div class="node"><b>Focused tests</b><span>byte layouts · codecs · semantics</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Corruption tests</b><span>touched failures propagate</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Readback oracle</b><span>independent decoded expectations</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Prometheus oracle</b><span>real promtool golden suite</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Replay A/B</b><span>IDs · bytes · fingerprints</span></div>
  <div class="arrow">→</div>
  <div class="node"><b>Real corpus</b><span>cold/warm I/O and memory evidence</span></div>
</div>

<div style="height:24px"></div>

<div class="grid three">
  <div class="card cyan-top">
    <h3>Semantic fingerprints</h3>
    <p>Series/sample counts are insufficient. Results and intended statistics must match across A/B runs.</p>
  </div>
  <div class="card blue-top">
    <h3>Independent expectations</h3>
    <p>The readback verifier does not call production evaluator helpers merely to force agreement.</p>
  </div>
  <div class="card amber-top">
    <h3>Validation is separate</h3>
    <p>Complete footer/file validation runs outside timed query benchmarks; lazy queries validate what they touch.</p>
  </div>
</div>

<div style="height:18px"></div>

<div class="callout warn small">
  A skipped oracle case is a coverage gap—not a pass.
</div>

<div class="source">Sources: AGENTS.md verification policy · docs/promql-coverage.md · storage.md</div>

---

<!-- _class: statement -->

# Chronoxide keeps the <strong>meaning</strong><br>close to the <strong>bytes</strong>.

<p class="lede">
Capture time protects the ingest decision. Event time places the sample.
Interned IDs make the mutable path compact. Immutable typed segments make reads
selective. PromQL projections add compatibility without discarding OTLP semantics.
</p>

<div class="split-line"></div>

<div class="pipeline">
  <div class="node"><b>Trust the right clock</b><span>captured_at_ms for policy</span></div>
  <div class="arrow">·</div>
  <div class="node"><b>Store the right shape</b><span>native typed values</span></div>
  <div class="arrow">·</div>
  <div class="node"><b>Read the minimum</b><span>lazy governed metadata + chunks</span></div>
  <div class="arrow">·</div>
  <div class="node"><b>Prove the semantics</b><span>oracles, replay, corruption tests</span></div>
</div>
