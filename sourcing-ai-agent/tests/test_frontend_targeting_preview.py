"""FT2 frontend targeting + preview contract tests (frontend VM harness).

Pins the FT0 v2 handoff (.coord/handoffs/tml-ft0-targeting-decision-v2.md)
§10.2 six-row matrix, reworked per the FT2 fixed-forward r2 review so every
row exercises the PRODUCTION paths (real payload builders, real rendered
components, real projection functions, real interaction wiring) instead of
reimplemented mirror logic or source-string proxies:

1. Options remain server-derived; picker-off omits the object; picker-on
   defaults roles [] + both statuses with the server-default location
   DISPLAY seed (absent on the wire, so the server stays the default owner).
2. Shard preview math: [] + [current,former] -> 2 shards; 3 roles x 2
   statuses -> 6 shards with exact per-shard labels; 1 role x 1 status -> 1;
   stale/removed selected ids are surfaced as unavailable, never dropped.
3. Full-recall warning renders when roles empty and both statuses selected;
   shard count/list + bounded-budget note render before confirmation, and
   confirmation is DISABLED without a validated preview (options loading /
   failure / stale registry / missing or mismatched plan registry pin).
4. Location editing is REQUEST-OWNED end-to-end: SearchPage holds the
   presence-aware state, passes it through SearchComposer ->
   SourcingBackendClient -> the real initial submit and revision payloads;
   cohort option edits never silently reset locations; recovered revisions
   rehydrate the recovered request's locations (never a silent default-US);
   absent -> server default (omitted), explicit [] -> opt-out (serialized),
   present null / invalid bounds fail closed.
5. Facet consumption: the atomic server pair
   {function_bucket_ids, function_bucket_source} is consumed byte-exactly
   from the canonical top-level served layer; the metadata mirror is a
   comparison-only layer; materialized/profile layers never override the
   build-point pair; and the function facet OPTION source is only the
   canonical backend facet summary (no rebuild from candidate rows).
6. Dual-status candidate (server membership truth) appears under BOTH
   employment filters; membership bytes are exact (no trim/lowercase/dedupe
   repair, no present-null-as-absence), and Cohort provenance without a
   valid membership fails closed instead of falling back to display status.

The interaction matrix (review finding 7) renders the REAL SearchPage /
SearchFlow / SearchComposer / PlanCard / CohortSelectionPicker /
SourcingBackendClient / api transport chain inside a minimal DOM with a
stubbed backend (fake fetch): no provider/model/network calls, served=0
unchanged. Source-string assertions remain only as deletion/wiring pins.
"""

from __future__ import annotations

import json
import shutil
import subprocess
import textwrap
import unittest
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

_MINIDOM_PREAMBLE = r"""
// ---------------------------------------------------------------------------
// Minimal DOM sufficient for react-dom 18 client mounts + real event
// dispatch (no jsdom in the dependency set; this shim implements exactly the
// surface react-dom uses: node tree ops, attributes, event capture/bubble,
// text inputs/checkboxes, selection/focus stubs).
// ---------------------------------------------------------------------------
const path = require("path");

const XHTML_NS = "http://www.w3.org/1999/xhtml";

class MiniEvent {
  constructor(type, options = {}) {
    this.type = type;
    this.bubbles = Boolean(options.bubbles);
    this.cancelable = Boolean(options.cancelable);
    this.composed = Boolean(options.composed);
    this.defaultPrevented = false;
    this.propagationStopped = false;
    this.immediatePropagationStopped = false;
    this.target = null;
    this.currentTarget = null;
    this.eventPhase = 0;
    this.isTrusted = false;
    this.timeStamp = Date.now();
    const { bubbles, cancelable, composed, ...rest } = options;
    Object.assign(this, rest);
  }
  preventDefault() { if (this.cancelable) this.defaultPrevented = true; }
  stopPropagation() { this.propagationStopped = true; }
  stopImmediatePropagation() { this.immediatePropagationStopped = true; this.propagationStopped = true; }
  composedPath() { return this._path || []; }
}

class MiniNode {
  constructor(nodeType, nodeName, ownerDocument) {
    this.nodeType = nodeType;
    this.nodeName = nodeName;
    this.ownerDocument = ownerDocument || null;
    this.parentNode = null;
    this.childNodes = [];
    this._listeners = {};
  }
  get firstChild() { return this.childNodes[0] || null; }
  get lastChild() { return this.childNodes[this.childNodes.length - 1] || null; }
  get nextSibling() {
    if (!this.parentNode) return null;
    const siblings = this.parentNode.childNodes;
    const index = siblings.indexOf(this);
    return index >= 0 && index + 1 < siblings.length ? siblings[index + 1] : null;
  }
  get previousSibling() {
    if (!this.parentNode) return null;
    const siblings = this.parentNode.childNodes;
    const index = siblings.indexOf(this);
    return index > 0 ? siblings[index - 1] : null;
  }
  get parentElement() {
    return this.parentNode && this.parentNode.nodeType === 1 ? this.parentNode : null;
  }
  appendChild(node) {
    if (node.parentNode) node.parentNode.removeChild(node);
    node.parentNode = this;
    this.childNodes.push(node);
    return node;
  }
  insertBefore(node, before) {
    if (before == null) return this.appendChild(node);
    if (node.parentNode) node.parentNode.removeChild(node);
    const index = this.childNodes.indexOf(before);
    if (index < 0) throw new Error("insertBefore: reference node not found");
    node.parentNode = this;
    this.childNodes.splice(index, 0, node);
    return node;
  }
  removeChild(node) {
    const index = this.childNodes.indexOf(node);
    if (index < 0) throw new Error("removeChild: node not found");
    this.childNodes.splice(index, 1);
    node.parentNode = null;
    return node;
  }
  replaceChild(next, prev) {
    this.insertBefore(next, prev);
    this.removeChild(prev);
    return prev;
  }
  contains(node) {
    let current = node;
    while (current) {
      if (current === this) return true;
      current = current.parentNode;
    }
    return false;
  }
  addEventListener(type, listener, capture) {
    if (!this._listeners[type]) this._listeners[type] = [];
    this._listeners[type].push({ listener, capture: Boolean(capture) });
  }
  removeEventListener(type, listener, capture) {
    const list = this._listeners[type] || [];
    this._listeners[type] = list.filter(
      (entry) => entry.listener !== listener || entry.capture !== Boolean(capture),
    );
  }
  dispatchEvent(event) {
    if (!event.target) {
      Object.defineProperty(event, "target", { value: this, configurable: true });
    }
    const path = [];
    let current = this;
    while (current) { path.push(current); current = current.parentNode; }
    event._path = path.slice();
    event.eventPhase = 1;
    for (let i = path.length - 1; i >= 1 && !event.propagationStopped; i -= 1) {
      const node = path[i];
      event.currentTarget = node;
      for (const entry of (node._listeners[event.type] || []).slice()) {
        if (!entry.capture) continue;
        entry.listener.call(node, event);
        if (event.immediatePropagationStopped) break;
      }
    }
    for (let i = 0; i < path.length && !event.propagationStopped; i += 1) {
      const node = path[i];
      event.currentTarget = node;
      event.eventPhase = i === 0 ? 2 : 3;
      for (const entry of (node._listeners[event.type] || []).slice()) {
        if (entry.capture && i !== 0) continue;
        entry.listener.call(node, event);
        if (event.immediatePropagationStopped) break;
      }
    }
    event.currentTarget = null;
    return !event.defaultPrevented;
  }
  get textContent() {
    if (this.nodeType === 3 || this.nodeType === 8) return this.nodeValue || "";
    return this.childNodes.map((child) => child.textContent).join("");
  }
  set textContent(value) {
    if (this.nodeType === 3 || this.nodeType === 8) { this.nodeValue = String(value); return; }
    this.childNodes = [];
    const text = String(value ?? "");
    if (text) {
      const node = this.ownerDocument.createTextNode(text);
      node.parentNode = this;
      this.childNodes.push(node);
    }
  }
}

class MiniTextNode extends MiniNode {
  constructor(text, ownerDocument) {
    super(3, "#text", ownerDocument);
    this.nodeValue = String(text);
  }
  get data() { return this.nodeValue; }
  set data(value) { this.nodeValue = String(value); }
  get length() { return this.nodeValue.length; }
  splitText(offset) {
    const rest = this.nodeValue.slice(offset);
    this.nodeValue = this.nodeValue.slice(0, offset);
    const next = this.ownerDocument.createTextNode(rest);
    if (this.parentNode) this.parentNode.insertBefore(next, this.nextSibling);
    return next;
  }
}

class MiniCommentNode extends MiniNode {
  constructor(text, ownerDocument) {
    super(8, "#comment", ownerDocument);
    this.nodeValue = String(text);
  }
}

class MiniElement extends MiniNode {
  constructor(tagName, ownerDocument) {
    super(1, tagName.toUpperCase(), ownerDocument);
    this.tagName = this.nodeName;
    this.localName = tagName.toLowerCase();
    this.namespaceURI = XHTML_NS;
    this.attributes = {};
    this.style = {};
    this.dataset = {};
  }
  setAttribute(name, value) {
    const text = String(value);
    this.attributes[name] = text;
    if (name === "class") this.className = text;
    else if (!name.startsWith("data-") && !name.startsWith("aria-")) this[name] = text;
    if (name.startsWith("data-")) {
      const key = name.slice(5).replace(/-([a-z])/g, (_, c) => c.toUpperCase());
      this.dataset[key] = text;
    }
  }
  getAttribute(name) {
    return Object.prototype.hasOwnProperty.call(this.attributes, name) ? this.attributes[name] : null;
  }
  hasAttribute(name) { return Object.prototype.hasOwnProperty.call(this.attributes, name); }
  removeAttribute(name) { delete this.attributes[name]; }
  setAttributeNS(ns, name, value) { this.setAttribute(name, value); }
  getAttributeNS(ns, name) { return this.getAttribute(name); }
  removeAttributeNS(ns, name) { this.removeAttribute(name); }
  get children() { return this.childNodes.filter((node) => node.nodeType === 1); }
  get classList() {
    const self = this;
    const parse = () => String(self.attributes.class || "").split(/\s+/).filter(Boolean);
    return {
      add: (...names) => { const set = new Set([...parse(), ...names]); self.setAttribute("class", [...set].join(" ")); },
      remove: (...names) => { const drop = new Set(names); self.setAttribute("class", parse().filter((n) => !drop.has(n)).join(" ")); },
      contains: (name) => parse().includes(name),
      toggle: (name) => { parse().includes(name) ? self.classList.remove(name) : self.classList.add(name); },
    };
  }
  focus() { this.ownerDocument.activeElement = this; }
  blur() { if (this.ownerDocument.activeElement === this) this.ownerDocument.activeElement = null; }
  setSelectionRange(start, end) { this.selectionStart = start; this.selectionEnd = end; }
  getElementsByTagName(tag) {
    const wanted = tag.toUpperCase();
    const out = [];
    const walk = (node) => {
      for (const child of node.childNodes) {
        if (child.nodeType === 1 && (wanted === "*" || child.nodeName === wanted)) out.push(child);
        walk(child);
      }
    };
    walk(this);
    return out;
  }
  get innerHTML() {
    return this.childNodes.map((child) => {
      if (child.nodeType === 3) return child.nodeValue;
      if (child.nodeType === 8) return `<!--${child.nodeValue}-->`;
      const attrs = Object.entries(child.attributes).map(([k, v]) => ` ${k}="${v}"`).join("");
      return `<${child.localName}${attrs}>${child.innerHTML}</${child.localName}>`;
    }).join("");
  }
  set innerHTML(value) {
    this.childNodes = [];
    if (value) throw new Error("innerHTML setter only supports clearing in MiniDom");
  }
}

class MiniDocument extends MiniNode {
  constructor() {
    super(9, "#document", null);
    this.ownerDocument = this;
    this.documentElement = new MiniElement("html", this);
    this.body = new MiniElement("body", this);
    this.head = new MiniElement("head", this);
    this.documentElement.parentNode = this;
    this.childNodes.push(this.documentElement);
    this.documentElement.childNodes.push(this.head, this.body);
    this.head.parentNode = this.documentElement;
    this.body.parentNode = this.documentElement;
    this.activeElement = this.body;
    this.defaultView = null;
  }
  createElement(tagName) { return new MiniElement(tagName, this); }
  createElementNS(ns, tagName) { const el = new MiniElement(tagName, this); el.namespaceURI = ns; return el; }
  createTextNode(text) { return new MiniTextNode(text, this); }
  createComment(text) { return new MiniCommentNode(text, this); }
  createDocumentFragment() { return new MiniNode(11, "#document-fragment", this); }
  getSelection() {
    return { rangeCount: 0, getRangeAt: () => null, removeAllRanges: () => {}, addRange: () => {}, extend: () => {} };
  }
}

// React feature detection (`isEventSupported`) checks `'on<input>' in
// document` at react-dom LOAD time; the handler properties must exist on the
// prototype before react-dom is required.
for (const handlerName of [
  "oninput", "onchange", "onclick", "onkeydown", "onkeyup", "onkeypress",
  "onblur", "onfocus", "onfocusin", "onfocusout", "onscroll", "onselect",
  "onsubmit", "onmouseover", "onmouseout", "onmousedown", "onmouseup",
  "ondblclick", "oncontextmenu", "onpointerdown", "onpointerup", "ontouchstart",
  "ontouchend", "ondragstart", "ondrop", "onanimationend", "ontransitionend",
]) {
  if (!(handlerName in MiniNode.prototype)) {
    Object.defineProperty(MiniNode.prototype, handlerName, {
      value: undefined,
      writable: true,
      configurable: true,
    });
  }
}

function createMiniWindow() {
  const document = new MiniDocument();
  const window = {
    document,
    Event: MiniEvent,
    KeyboardEvent: MiniEvent,
    MouseEvent: MiniEvent,
    CustomEvent: MiniEvent,
    InputEvent: MiniEvent,
    FocusEvent: MiniEvent,
    Node: MiniNode,
    Element: MiniElement,
    HTMLElement: MiniElement,
    HTMLIFrameElement: class HTMLIFrameElement extends MiniElement {},
    ShadowRoot: class ShadowRoot extends MiniNode {},
    DocumentFragment: MiniNode,
    navigator: { userAgent: "mini-dom" },
    location: { protocol: "http:", hostname: "localhost", search: "", href: "http://localhost/" },
    setTimeout, clearTimeout, setInterval, clearInterval, queueMicrotask,
    MessageChannel: global.MessageChannel,
    requestAnimationFrame: (cb) => setTimeout(cb, 0),
    cancelAnimationFrame: (id) => clearTimeout(id),
    getSelection: () => document.getSelection(),
    addEventListener: () => {},
    removeEventListener: () => {},
    dispatchEvent: () => true,
    localStorage: (() => {
      const store = new Map();
      return {
        getItem: (k) => (store.has(k) ? store.get(k) : null),
        setItem: (k, v) => store.set(k, String(v)),
        removeItem: (k) => store.delete(k),
        clear: () => store.clear(),
        key: (i) => [...store.keys()][i] ?? null,
        get length() { return store.size; },
      };
    })(),
  };
  window.self = window;
  window.window = window;
  window.top = window;
  window.parent = window;
  document.defaultView = window;
  return window;
}

// Globals must exist BEFORE react-dom is required (its input-event feature
// detection reads the global document at module load).
const miniWindow = createMiniWindow();
globalThis.window = miniWindow;
globalThis.document = miniWindow.document;
globalThis.navigator = miniWindow.navigator;
globalThis.IS_REACT_ACT_ENVIRONMENT = true;

const fs = require("fs");
const vm = require("vm");
const ts = require("./frontend-demo/node_modules/typescript");
const React = require(path.join(process.cwd(), "frontend-demo/node_modules/react"));
const ReactDOMServer = require(path.join(process.cwd(), "frontend-demo/node_modules/react-dom/server"));
const ReactDOMClient = require(path.join(process.cwd(), "frontend-demo/node_modules/react-dom/client"));
const ReactJsxRuntime = require(path.join(process.cwd(), "frontend-demo/node_modules/react/jsx-runtime"));
const { act } = React;
const el = React.createElement;
const render = (element) => ReactDOMServer.renderToStaticMarkup(element);
"""

_HARNESS_PREAMBLE = r"""
// ---------------------------------------------------------------------------
// TypeScript module harness: one shared vm context (shared globals), real
// modules compiled on demand, per-path stubs for non-exercised leaves, and a
// fake fetch routing the real api.ts transport to a stubbed backend.
// ---------------------------------------------------------------------------
const fetchCalls = [];
const fetchRoutes = [];
const addRoute = (method, pathPrefix, handler) => {
  fetchRoutes.push({ method, pathPrefix, handler });
};
const fakeFetch = async (url, options = {}) => {
  const method = String(options.method || "GET").toUpperCase();
  const urlText = String(url);
  const pathname = urlText.startsWith("http") ? new URL(urlText).pathname : urlText;
  const body = typeof options.body === "string" ? JSON.parse(options.body) : null;
  fetchCalls.push({ method, path: pathname, body, url: urlText });
  const route = fetchRoutes.find(
    (entry) => entry.method === method && pathname.startsWith(entry.pathPrefix),
  );
  // Awaited so a route may defer its response behind a test-controlled gate
  // (deferred-promise interleavings for the revision/confirmation overlap
  // matrix); sync handlers are unaffected (await on a non-promise is the
  // identity). The full URL (with query string) is passed as the third arg
  // so handlers can echo request pagination/filter parameters.
  const payload = route ? await route.handler(body, pathname, urlText) : null;
  const ok = Boolean(route) && payload && payload.__status !== 404;
  const status = ok ? 200 : 404;
  const responseBody = ok ? payload : { error: `no stubbed route for ${method} ${pathname}` };
  return {
    ok,
    status,
    headers: {
      get: (name) => (String(name).toLowerCase() === "content-type" ? "application/json" : null),
    },
    text: async () => JSON.stringify(responseBody),
    json: async () => responseBody,
  };
};
const callsTo = (pathPrefix) =>
  fetchCalls.filter((call) => call.path.startsWith(pathPrefix));

const sessionState = { queryText: "", activeHistoryId: "" };
const searchHistoryStore = new Map();
const routeParams = { history: "", job: "" };
// react-router-dom returns a STABLE setSearchParams; SearchPage's route
// effect depends on it, so an unstable reference would re-run the effect
// (and its session reset) on every render.
const stableSetSearchParams = (next) => {
  routeParams.history = next.history || "";
  routeParams.job = next.job || "";
};
const reactRouterDomStub = {
  useSearchParams: () => {
    const params = { get: (key) => routeParams[key] || null };
    return [params, stableSetSearchParams];
  },
  useNavigate: () => () => {},
  useLocation: () => ({ pathname: "/", search: "", hash: "", state: null }),
  useParams: () => ({}),
  Link: ({ children }) => children || null,
  NavLink: ({ children }) => children || null,
};

const moduleStubs = {
  "frontend-demo/src/data/mockData.ts": {
    mockCandidateDetails: {},
    mockDashboard: {},
    mockManualReviewItems: [],
    mockPlan: {},
    mockRunStatus: {},
  },
  "frontend-demo/src/lib/demoSession.ts": {
    readDemoSession: () => ({
      queryText: sessionState.queryText || "",
      plan: null,
      reviewApproved: false,
      lastVisitedStage: "search",
      activeHistoryId: sessionState.activeHistoryId || "",
      phase: "idle",
      revisionText: "",
      timelineSteps: [],
      selectedCandidateId: "",
    }),
    writeDemoSession: (patch) => Object.assign(sessionState, patch || {}),
  },
  "frontend-demo/src/lib/searchHistory.ts": {
    readSearchHistoryItem: (id) => searchHistoryStore.get(id) || null,
    upsertSearchHistoryItem: (item) => searchHistoryStore.set(item.id, item),
    replaceSearchHistoryItem: (previousId, item) => {
      searchHistoryStore.delete(previousId);
      searchHistoryStore.set(item.id, item);
    },
    startNewSearchEventName: () => "frontend-demo-start-new-search",
  },
  "frontend-demo/src/hooks/useDashboardCandidateHydration.ts": {
    useDashboardCandidateHydration: () => ({
      isHydratingCandidates: false,
      candidateHydrationError: "",
    }),
  },
  "frontend-demo/src/hooks/useCandidateReviewState.ts": {
    useCandidateReviewState: () => ({
      backendItems: [],
      localRecords: {},
      effectiveReviewCount: 0,
      reviewStatusMap: {},
      refresh: async () => {},
    }),
  },
  "frontend-demo/src/components/ExcelWorkflowIntakePanel.tsx": {
    ExcelWorkflowIntakePanel: () => null,
  },
  "frontend-demo/src/components/ExecutionTimeline.tsx": {
    ExecutionTimeline: () => null,
  },
  "frontend-demo/src/components/ManualReviewQueuePanel.tsx": {
    ManualReviewQueuePanel: () => null,
  },
  "frontend-demo/src/components/TargetCandidatesPanel.tsx": {
    TargetCandidatesPanel: () => null,
  },
  "frontend-demo/src/components/Avatar.tsx": { Avatar: () => null },
  "frontend-demo/src/lib/reviewRegistry.ts": {
    addCandidateToReviewRegistry: () => {},
  },
  "frontend-demo/src/lib/targetCandidatesStore.ts": {
    addTargetCandidate: () => {},
    addTargetCandidates: () => {},
    readTargetCandidates: async () => [],
    targetCandidatesUpdatedEventName: () => "target-candidates-updated",
  },
  "frontend-demo/src/lib/workflowContext.ts": {
    buildWorkflowRoute: () => "/workflow",
  },
};

const sandboxGlobal = {
  console,
  TextEncoder,
  URL,
  URLSearchParams,
  Headers,
  FormData,
  Blob,
  AbortController,
  setTimeout,
  clearTimeout,
  setInterval,
  clearInterval,
  queueMicrotask,
  fetch: fakeFetch,
  window: miniWindow,
  document: miniWindow.document,
  navigator: miniWindow.navigator,
  localStorage: miniWindow.localStorage,
  location: miniWindow.location,
  crypto: (() => {
    let counter = 0;
    return { randomUUID: () => `uuid-${(counter += 1)}` };
  })(),
};

const resolveModulePath = (fromPath, specifier) => {
  const joined = path.normalize(path.join(path.dirname(fromPath), specifier));
  for (const candidate of [joined, `${joined}.ts`, `${joined}.tsx`, `${joined}/index.ts`]) {
    if (fs.existsSync(path.join(process.cwd(), candidate))) {
      return candidate;
    }
  }
  throw new Error(`Cannot resolve ${specifier} from ${fromPath}`);
};

const moduleCache = new Map();
function loadTs(relPath, overrides = {}) {
  if (Object.prototype.hasOwnProperty.call(overrides, relPath)) {
    return overrides[relPath];
  }
  if (Object.prototype.hasOwnProperty.call(moduleStubs, relPath)) {
    return moduleStubs[relPath];
  }
  if (moduleCache.has(relPath)) {
    return moduleCache.get(relPath);
  }
  const source = fs
    .readFileSync(path.join(process.cwd(), relPath), "utf8")
    .replaceAll("import.meta.env", "({})");
  const compiled = ts.transpileModule(source, {
    compilerOptions: {
      module: ts.ModuleKind.CommonJS,
      target: ts.ScriptTarget.ES2020,
      jsx: ts.JsxEmit.ReactJSX,
    },
  }).outputText;
  const module = { exports: {} };
  moduleCache.set(relPath, module.exports);
  const localRequire = (specifier) => {
    if (specifier === "react") return React;
    if (specifier === "react/jsx-runtime") return ReactJsxRuntime;
    if (specifier === "react-dom/client") return ReactDOMClient;
    if (specifier === "react-dom/server") return ReactDOMServer;
    if (specifier === "react-router-dom") return reactRouterDomStub;
    return loadTs(resolveModulePath(relPath, specifier));
  };
  // One context per module (the shared globals are passed by reference), so
  // each module's `require`/`module` bindings stay its own.
  vm.runInNewContext(
    compiled,
    {
      ...sandboxGlobal,
      module,
      exports: module.exports,
      require: localRequire,
    },
    { filename: path.basename(relPath) },
  );
  moduleCache.set(relPath, module.exports);
  return module.exports;
}

const cohortSelection = loadTs("frontend-demo/src/lib/cohortSelection.ts");
const api = loadTs("frontend-demo/src/lib/api.ts");
const sourcingBackend = loadTs("frontend-demo/src/lib/sourcingBackend.ts");
const historyRecovery = loadTs("frontend-demo/src/lib/historyRecovery.ts");
const candidateFilters = loadTs("frontend-demo/src/lib/candidateFilters.ts");
const picker = loadTs("frontend-demo/src/components/CohortSelectionPicker.tsx");
const planCard = loadTs("frontend-demo/src/components/PlanCard.tsx");
const searchFlow = loadTs("frontend-demo/src/components/SearchFlow.tsx");
const searchPage = loadTs("frontend-demo/src/pages/SearchPage.tsx");

// -- Mini-DOM query + event helpers -----------------------------------------
const findByTestId = (node, id) => {
  if (node.nodeType === 1 && node.getAttribute && node.getAttribute("data-testid") === id) {
    return node;
  }
  for (const child of node.childNodes || []) {
    const found = findByTestId(child, id);
    if (found) return found;
  }
  return null;
};
const findAllByTestId = (node, id, out = []) => {
  if (node.nodeType === 1 && node.getAttribute && node.getAttribute("data-testid") === id) {
    out.push(node);
  }
  for (const child of node.childNodes || []) {
    findAllByTestId(child, id, out);
  }
  return out;
};
const findInputByValue = (node, value) => {
  if (node.nodeType === 1 && node.nodeName === "INPUT" && node.value === value) {
    return node;
  }
  for (const child of node.childNodes || []) {
    const found = findInputByValue(child, value);
    if (found) return found;
  }
  return null;
};
const clickEl = (node) =>
  node.dispatchEvent(new MiniEvent("click", { bubbles: true, cancelable: true }));
const setInputValue = (node, value) => {
  node.value = value;
  node.dispatchEvent(new MiniEvent("input", { bubbles: true, cancelable: true }));
};
const setCheckbox = (node, checked) => {
  node.checked = checked;
  node.dispatchEvent(new MiniEvent("click", { bubbles: true, cancelable: true }));
};
const pressEnter = (node) =>
  node.dispatchEvent(new MiniEvent("keydown", { bubbles: true, cancelable: true, key: "Enter" }));
const settle = async (rounds = 8) => {
  for (let index = 0; index < rounds; index += 1) {
    await act(async () => {
      await Promise.resolve();
    });
  }
};
const mountApp = async () => {
  const container = miniWindow.document.createElement("div");
  miniWindow.document.body.appendChild(container);
  const root = ReactDOMClient.createRoot(container);
  await act(async () => {
    root.render(el(searchPage.SearchPage));
  });
  await settle();
  return { container, root };
};
const captureError = (callback) => {
  try {
    callback();
    return "";
  } catch (error) {
    return String(error?.message || error || "");
  }
};
"""

_FIXTURES_PREAMBLE = r"""
// ---------------------------------------------------------------------------
// Shared fixtures (server-derived options, cohorts, plans, backend envelopes)
// ---------------------------------------------------------------------------
const REGISTRY_VERSION = "cohort_selection.registry.v1";
const REGISTRY_DIGEST = "9f2c1ab4d5e6478091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708";
const optionsPayload = {
  schema_version: "cohort_selection.v1",
  registry_version: REGISTRY_VERSION,
  registry_digest: REGISTRY_DIGEST,
  role_buckets: [
    { id: "engineering", label: "Engineer", order: 20 },
    { id: "research", label: "Researcher", order: 10 },
    { id: "product_management", label: "Product Manager", order: 30 },
    { id: "infra_systems", label: "Infrastructure & Systems", order: 40 },
    { id: "founding", label: "Founder", order: 50 },
  ],
  employment_statuses: [
    { id: "former", label: "Former employees", order: 20 },
    { id: "current", label: "Current employees", order: 10 },
  ],
  role_match_options: [
    { id: "all", label: "Match all selected roles", order: 20 },
    { id: "any", label: "Match any selected role", order: 10 },
  ],
  defaults: { role_match: "any" },
};
const parsedOptions = cohortSelection.parseCohortSelectionOptionsPayload(optionsPayload);
const explicitCohort = {
  schema_version: "cohort_selection.v1",
  role_bucket_ids: ["research", "engineering"],
  employment_statuses: ["current", "former"],
  role_match: "any",
  source: "user_explicit",
};
const selectionFor = (roleIds, statusIds, roleMatch = "any") => ({
  schema_version: "cohort_selection.v1",
  role_bucket_ids: roleIds,
  employment_statuses: statusIds,
  role_match: roleMatch,
  source: "user_explicit",
});
const manifestPin = (overrides = {}) => ({
  registry_version: REGISTRY_VERSION,
  registry_digest: REGISTRY_DIGEST,
  manifest_digest: "manifest-digest-1",
  ...overrides,
});
// Production-shaped cohort plan record: the backend CohortProviderCompiler
// always embeds the provider execution manifest into the plan's
// acquisition_strategy for explicit-Cohort plans.
const cohortPlanRecord = (manifest) => ({
  acquisition_strategy: {
    provider_execution_manifest: manifest === undefined ? manifestPin() : manifest,
  },
});

const makePlan = (overrides = {}) => ({
  planId: "plan-1",
  rawUserRequest: "find people",
  targetCompany: "ACME",
  targetPopulation: "researchers",
  projectScope: "",
  keywords: [],
  acquisitionStrategy: "strategy",
  searchStrategy: [],
  estimatedCostLevel: "low",
  reviewRequired: true,
  status: "pending_review",
  cohortRegistryPin: {
    registryVersion: REGISTRY_VERSION,
    registryDigest: REGISTRY_DIGEST,
  },
  reviewGate: {
    status: "pending",
    requiredBeforeExecution: true,
    riskLevel: "low",
    reasons: [],
    confirmationItems: [],
    editableFields: [],
    suggestedActions: [],
    scopeHints: [],
    executionModeHints: [],
  },
  ...overrides,
});
const makeDecision = (overrides = {}) => ({
  confirmedCompanyScope: [],
  extraSourceFamilies: [],
  ...overrides,
});
const renderPlanCard = (props = {}) =>
  render(
    el(planCard.PlanCard, {
      plan: makePlan(),
      revisionText: "",
      reviewDecision: makeDecision(),
      cohortOptions: parsedOptions,
      isLoadingCohortOptions: false,
      cohortOptionsError: "",
      reviewChecklistConfirmed: true,
      isApplyingRevision: false,
      isConfirming: false,
      onRevisionChange: () => {},
      onReviewDecisionChange: () => {},
      onRetryCohortOptions: () => {},
      onReviewChecklistChange: () => {},
      onApplyRevision: () => {},
      onConfirm: () => {},
      ...props,
    }),
  );
const buttonTag = (markup, testid) => {
  const match = markup.match(new RegExp(`<button[^>]*data-testid="${testid}"[^>]*>`));
  return match ? match[0] : "";
};
const baseCandidate = {
  name: "",
  headline: "",
  summary: "",
  currentCompany: "",
  notesSnippet: "",
  team: "Unknown",
  focusAreas: [],
  matchReasons: [],
  education: [],
  experience: [],
  matchedKeywords: [],
  sourceMatches: [],
  outreachLayer: null,
};
const filterSelection = (patch) => ({
  layers: [],
  recallBuckets: [],
  employmentStatuses: [],
  locations: [],
  functionBuckets: [],
  searchKeyword: "",
  ...patch,
});

// -- Stubbed backend payloads -------------------------------------------------
const planResponseForRequest = (requestBody, overrides = {}) => {
  const cohort = requestBody.cohort_selection || explicitCohort;
  const requestMirror = {
    raw_user_request: requestBody.raw_user_request || "find people",
    cohort_selection: cohort,
    ...(Object.prototype.hasOwnProperty.call(requestBody, "target_locations")
      ? { target_locations: requestBody.target_locations }
      : {}),
    ...(Object.prototype.hasOwnProperty.call(requestBody, "exclude_target_locations")
      ? { exclude_target_locations: requestBody.exclude_target_locations }
      : {}),
  };
  // PRODUCTION SHAPE (rerun3 review findings 1/8): the real backend
  // `build_request_preview_payload()` never projects either location field —
  // the preview carries only the documented projection keys (incl.
  // cohort_selection). The canonical `request` above remains the sole
  // complete location mirror.
  const requestPreview = {
    request_view: "normalized_request",
    raw_user_request: requestMirror.raw_user_request,
    query: requestMirror.raw_user_request,
    target_company: "ACME",
    cohort_selection: cohort,
  };
  return {
    status: "pending",
    history_id: overrides.historyId || "hist-server-1",
    request: requestMirror,
    request_preview: requestPreview,
    plan: {
      target_company: "ACME",
      acquisition_strategy: {
        provider_execution_manifest: overrides.manifest === undefined ? manifestPin() : overrides.manifest,
      },
    },
    plan_review_gate: {
      status: "pending",
      required_before_execution: true,
      risk_level: "low",
      reasons: [],
      confirmation_items: [],
      editable_fields: overrides.editableFields || [],
      suggested_actions: [],
      scope_hints: [],
      execution_mode_hints: [],
    },
    plan_review_session: { review_id: overrides.reviewId || "review-1", status: "pending" },
    metadata: overrides.metadata === undefined
      ? { provider_execution_manifest: manifestPin() }
      : overrides.metadata,
  };
};
const recoveryEnvelopeFor = (historyId, planResponse, extra = {}) => ({
  recovery: {
    history_id: historyId,
    query_text: planResponse.request.raw_user_request,
    phase: "plan",
    review_id: planResponse.plan_review_session.review_id,
    job_id: "",
    request: planResponse.request,
    request_preview: planResponse.request_preview,
    plan: planResponse.plan,
    plan_review_gate: planResponse.plan_review_gate,
    plan_review_session: planResponse.plan_review_session,
    metadata: planResponse.metadata,
    created_at: "2026-07-19T00:00:00Z",
    updated_at: "2026-07-19T00:00:00Z",
    ...extra,
  },
});
const registerPlanBackend = (overrides = {}) => {
  addRoute("GET", "/api/cohort-selection/options", () => optionsPayload);
  addRoute("POST", "/api/plan/submit", (body) => planResponseForRequest(body, overrides));
  addRoute("GET", "/api/frontend-history/", () => {
    const lastSubmit = callsTo("/api/plan/submit").slice(-1)[0];
    const response = planResponseForRequest(lastSubmit ? lastSubmit.body : {}, overrides);
    return recoveryEnvelopeFor(response.history_id, response);
  });
};
const seedPlanHistoryItem = (historyId, plan, reviewId) => ({
  id: historyId,
  createdAt: "2026-07-19T00:00:00Z",
  updatedAt: "2026-07-19T00:00:00Z",
  queryText: "find people",
  summary: "find people",
  phase: "plan",
  errorMessage: "",
  plan,
  reviewId,
  jobId: "",
  revisionText: "",
  reviewDecision: {
    confirmedCompanyScope: [],
    targetCompanyLinkedinUrl: "",
    extraSourceFamilies: [],
  },
  reviewChecklistConfirmed: true,
  requiresReview: true,
  timelineSteps: [],
  selectedCandidateId: "",
  historyMetadata: {},
});
"""


def _run_node(script_body: str) -> dict:
    completed = subprocess.run(
        ["node", "-e", script_body],
        cwd=REPO_ROOT,
        text=True,
        capture_output=True,
        check=True,
    )
    return json.loads(completed.stdout)


class FrontendTargetingPreviewTest(unittest.TestCase):
    def setUp(self) -> None:
        if shutil.which("node") is None:
            self.skipTest("node is required for frontend TypeScript helper checks")

    def test_options_server_derived_and_picker_defaults(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            const pickerOffSubmit = api.__testBuildPlanSubmitPayload("find people");
            const defaultCohort = cohortSelection.createDefaultCohortSelection(parsedOptions);
            const defaultLocations = cohortSelection.createDefaultCohortLocationSelection();
            // Real render of the production picker with the request-owned
            // (controlled) absent location seed: the server-default location
            // display renders; the opt-out affordance stays unchecked.
            const enabledMarkup = render(el(picker.CohortSelectionPicker, {
              idPrefix: "search",
              value: defaultCohort,
              options: parsedOptions,
              locationValue: defaultLocations,
              onChange: () => {},
              onLocationChange: () => {},
            }));
            const disabledMarkup = render(el(picker.CohortSelectionPicker, {
              idPrefix: "search",
              value: null,
              options: parsedOptions,
              locationValue: defaultLocations,
              onChange: () => {},
              onLocationChange: () => {},
            }));
            console.log(JSON.stringify({
              parsedRoleIds: parsedOptions.roleBuckets.map((option) => option.id),
              parsedStatusIds: parsedOptions.employmentStatuses.map((option) => option.id),
              defaultCohort,
              defaultLocations,
              pickerOffOmitsCohort: !("cohort_selection" in pickerOffSubmit),
              pickerOffOmitsTargetLocations: !("target_locations" in pickerOffSubmit),
              pickerOffOmitsExcludeLocations: !("exclude_target_locations" in pickerOffSubmit),
              enabledShowsServerDefault:
                enabledMarkup.includes('data-testid="search-target-locations-default"')
                && enabledMarkup.includes("United States"),
              enabledOptOutUnchecked: (() => {
                const match = enabledMarkup.match(
                  /<input[^>]*data-testid="search-target-locations-optout"[^>]*>/,
                );
                return Boolean(match) && !match[0].includes("checked");
              })(),
              enabledRendersOptionLabels:
                enabledMarkup.includes("Researcher")
                && enabledMarkup.includes("Engineer")
                && enabledMarkup.includes("Infrastructure &amp; Systems")
                && enabledMarkup.includes("Founder")
                && enabledMarkup.includes("Current employees")
                && enabledMarkup.includes("Former employees"),
              disabledHidesLocationFields:
                !disabledMarkup.includes("search-target-locations-default")
                && !disabledMarkup.includes("search-target-locations-optout"),
            }));
            """
        )
        payload = _run_node(script)
        self.assertEqual(
            payload["parsedRoleIds"],
            ["research", "engineering", "product_management", "infra_systems", "founding"],
        )
        self.assertEqual(payload["parsedStatusIds"], ["current", "former"])
        self.assertEqual(
            payload["defaultCohort"],
            {
                "schema_version": "cohort_selection.v1",
                "role_bucket_ids": [],
                "employment_statuses": ["current", "former"],
                "role_match": "any",
                "source": "user_explicit",
            },
        )
        # Absent location state: the server default applies on the wire; the
        # picker renders it as a display seed (FT0 §7.2 tri-state).
        self.assertEqual(payload["defaultLocations"], {})
        self.assertTrue(payload["pickerOffOmitsCohort"])
        self.assertTrue(payload["pickerOffOmitsTargetLocations"])
        self.assertTrue(payload["pickerOffOmitsExcludeLocations"])
        self.assertTrue(payload["enabledShowsServerDefault"])
        self.assertTrue(payload["enabledOptOutUnchecked"])
        self.assertTrue(payload["enabledRendersOptionLabels"])
        self.assertTrue(payload["disabledHidesLocationFields"])

        # Deletion pins (review finding 1): the module-global draft registry
        # may not return; location state is request-owned end-to-end.
        cohort_source = (REPO_ROOT / "frontend-demo/src/lib/cohortSelection.ts").read_text(encoding="utf-8")
        self.assertNotIn("publishCohortLocationDraft", cohort_source)
        self.assertNotIn("readCohortLocationDraft", cohort_source)
        self.assertNotIn("canonicalCohortSelectionKey", cohort_source)
        self.assertNotIn("pendingCohortLocationDraft", cohort_source)
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        self.assertNotIn("readCohortLocationDraft", api_source)
        self.assertNotIn("pendingCohortLocationDraft", api_source)

    def test_shard_preview_math_and_stale_selection_surfacing(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            const allRolesBothStatuses = cohortSelection.buildCohortShardPreview(
              selectionFor([], ["current", "former"]),
              parsedOptions,
            );
            const threeRolesTwoStatuses = cohortSelection.buildCohortShardPreview(
              selectionFor(["research", "engineering", "product_management"], ["current", "former"]),
              parsedOptions,
            );
            const oneRoleOneStatus = cohortSelection.buildCohortShardPreview(
              selectionFor(["research"], ["current"]),
              parsedOptions,
            );
            // Registry drift: a selected role no longer exists in the current
            // options. It must surface as unavailable, never silently drop.
            const staleRole = cohortSelection.buildCohortShardPreview(
              selectionFor(["research", "removed_role"], ["current", "former"]),
              parsedOptions,
            );
            // Every selected role is unavailable: no fabricated All-roles.
            const allRolesStale = cohortSelection.buildCohortShardPreview(
              selectionFor(["removed_role"], ["current"]),
              parsedOptions,
            );
            // Unknown status ids must not produce a false full-recall call.
            const staleStatus = cohortSelection.buildCohortShardPreview(
              selectionFor([], ["current", "former", "ghost_status"]),
              parsedOptions,
            );
            const staleRoleMatch = cohortSelection.buildCohortShardPreview(
              selectionFor(["research"], ["current"], "bogus_match"),
              parsedOptions,
            );
            // Rerun3 finding 7: toggling one AVAILABLE option must never
            // silently drop unavailable selected ids. They stay preserved
            // (original relative order, after the available ones) until the
            // user explicitly removes them.
            const toggleAddsAvailable = cohortSelection.toggleOrderedOption(
              ["research", "removed_role"],
              "engineering",
              true,
              parsedOptions.roleBuckets,
            );
            const toggleRemovesAvailable = cohortSelection.toggleOrderedOption(
              ["research", "removed_role"],
              "research",
              false,
              parsedOptions.roleBuckets,
            );
            // Explicit removal of the unavailable id DOES drop it (a
            // deliberate user action, not a side effect of another edit).
            const explicitRemoval = cohortSelection.toggleOrderedOption(
              ["research", "removed_role"],
              "removed_role",
              false,
              parsedOptions.roleBuckets,
            );
            const toggleStatusPreservesUnavailable = cohortSelection.toggleOrderedOption(
              ["current", "ghost_status"],
              "former",
              true,
              parsedOptions.employmentStatuses,
            );
            // Rendered: unavailable selections render as VISIBLE removable
            // chips (never invisible state), one per unavailable id.
            const chipMarkup = render(el(picker.CohortSelectionPicker, {
              idPrefix: "chip",
              value: selectionFor(["research", "removed_role"], ["current", "ghost_status"]),
              options: parsedOptions,
              locationValue: cohortSelection.createDefaultCohortLocationSelection(),
              onChange: () => {},
              onLocationChange: () => {},
            }));
            console.log(JSON.stringify({
              allRolesBothStatuses,
              threeRolesTwoStatuses,
              oneRoleOneStatus,
              staleRole,
              allRolesStale,
              staleStatus,
              staleRoleMatch,
              toggleAddsAvailable,
              toggleRemovesAvailable,
              explicitRemoval,
              toggleStatusPreservesUnavailable,
              chipRoleChips: (chipMarkup.match(/chip-unavailable-role-chip"/g) || []).length,
              chipStatusChips: (chipMarkup.match(/chip-unavailable-status-chip"/g) || []).length,
              chipRoleRemoveButton: chipMarkup.includes("chip-unavailable-role-remove-removed_role"),
              chipStatusRemoveButton: chipMarkup.includes("chip-unavailable-status-remove-ghost_status"),
            }));
            """
        )
        payload = _run_node(script)

        # [] roles + [current, former] -> S * max(1, 0) = 2 status-only shards.
        self.assertEqual(payload["allRolesBothStatuses"]["shardCount"], 2)
        self.assertEqual(
            [
                (shard["shardId"], shard["statusLabel"], shard["roleId"], shard["roleLabel"])
                for shard in payload["allRolesBothStatuses"]["shards"]
            ],
            [
                ("current:all_roles", "Current employees", None, "All roles"),
                ("former:all_roles", "Former employees", None, "All roles"),
            ],
        )
        self.assertTrue(payload["allRolesBothStatuses"]["isFullRecall"])
        self.assertFalse(payload["allRolesBothStatuses"]["hasUnavailableSelections"])
        self.assertEqual(
            payload["allRolesBothStatuses"]["registryDigest"],
            "9f2c1ab4d5e6478091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708",
        )

        # 3 roles x 2 statuses -> 6 shards with exact per-shard labels.
        self.assertEqual(payload["threeRolesTwoStatuses"]["shardCount"], 6)
        self.assertEqual(
            [
                (shard["shardId"], shard["statusLabel"], shard["roleLabel"])
                for shard in payload["threeRolesTwoStatuses"]["shards"]
            ],
            [
                ("current:research", "Current employees", "Researcher"),
                ("current:engineering", "Current employees", "Engineer"),
                ("current:product_management", "Current employees", "Product Manager"),
                ("former:research", "Former employees", "Researcher"),
                ("former:engineering", "Former employees", "Engineer"),
                ("former:product_management", "Former employees", "Product Manager"),
            ],
        )

        # 1 role x 1 status -> 1 shard.
        self.assertEqual(payload["oneRoleOneStatus"]["shardCount"], 1)
        self.assertEqual(
            [
                (shard["shardId"], shard["statusLabel"], shard["roleLabel"])
                for shard in payload["oneRoleOneStatus"]["shards"]
            ],
            [("current:research", "Current employees", "Researcher")],
        )

        # Stale role: surfaced as unavailable; the available part still
        # previews honestly (research x 2 statuses), never 2x2=4 and never
        # "All roles" for a nonempty selection.
        stale_role = payload["staleRole"]
        self.assertEqual(stale_role["unavailableRoleIds"], ["removed_role"])
        self.assertEqual(stale_role["unavailableStatusIds"], [])
        self.assertTrue(stale_role["hasUnavailableSelections"])
        self.assertEqual(
            [shard["shardId"] for shard in stale_role["shards"]],
            ["current:research", "former:research"],
        )
        self.assertFalse(stale_role["isFullRecall"])

        # All roles stale: zero shards, no fabricated all-roles expansion.
        all_roles_stale = payload["allRolesStale"]
        self.assertEqual(all_roles_stale["shardCount"], 0)
        self.assertEqual(all_roles_stale["shards"], [])
        self.assertEqual(all_roles_stale["unavailableRoleIds"], ["removed_role"])
        self.assertTrue(all_roles_stale["hasUnavailableSelections"])

        # Unknown status id: no false full-recall classification.
        stale_status = payload["staleStatus"]
        self.assertEqual(stale_status["unavailableStatusIds"], ["ghost_status"])
        self.assertTrue(stale_status["hasUnavailableSelections"])
        self.assertFalse(stale_status["isFullRecall"])

        # Unknown role_match: flagged unavailable as well.
        stale_role_match = payload["staleRoleMatch"]
        self.assertTrue(stale_role_match["roleMatchUnavailable"])
        self.assertTrue(stale_role_match["hasUnavailableSelections"])

        # Rerun3 finding 7: edits to available options preserve unavailable
        # selected ids inertly; only an explicit removal drops them.
        self.assertEqual(
            payload["toggleAddsAvailable"],
            ["research", "engineering", "removed_role"],
        )
        self.assertEqual(payload["toggleRemovesAvailable"], ["removed_role"])
        self.assertEqual(payload["explicitRemoval"], ["research"])
        self.assertEqual(
            payload["toggleStatusPreservesUnavailable"],
            ["current", "former", "ghost_status"],
        )

        # Unavailable selections render as visible removable chips.
        self.assertEqual(payload["chipRoleChips"], 1)
        self.assertEqual(payload["chipStatusChips"], 1)
        self.assertTrue(payload["chipRoleRemoveButton"])
        self.assertTrue(payload["chipStatusRemoveButton"])

    def test_full_recall_warning_budget_and_confirmation_gate(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            const fullRecallCohort = selectionFor([], ["current", "former"]);
            const scopedCohort = selectionFor(["research"], ["current"]);
            const staleCohort = selectionFor(["removed_role"], ["current"]);

            // Validated preview: shard count/list + bounded-budget note render
            // BEFORE the confirmation button; the button is enabled.
            const validMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: fullRecallCohort }),
              reviewDecision: makeDecision({ cohortSelection: fullRecallCohort }),
            });
            const validPreviewIndex = validMarkup.indexOf('data-testid="plan-cohort-shard-preview"');
            const validBudgetIndex = validMarkup.indexOf('data-testid="plan-shard-budget-note"');
            const validConfirmIndex = validMarkup.indexOf('data-testid="plan-confirm-button"');

            // Options loading: no validated preview -> confirmation disabled.
            const loadingMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: scopedCohort }),
              reviewDecision: makeDecision({ cohortSelection: scopedCohort }),
              cohortOptions: null,
              isLoadingCohortOptions: true,
            });

            // Options failure: retryable blocking state -> confirmation disabled.
            const failureMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: scopedCohort }),
              reviewDecision: makeDecision({ cohortSelection: scopedCohort }),
              cohortOptions: null,
              cohortOptionsError: "options fetch failed",
            });

            // Registry drift (stale selected role): blocked as well.
            const staleMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: staleCohort }),
              reviewDecision: makeDecision({ cohortSelection: staleCohort }),
            });

            // Legacy plan without a cohort: no gate, confirmation enabled.
            const legacyMarkup = renderPlanCard({});

            // Scoped (non full-recall) cohort: preview without the warning.
            const scopedMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: scopedCohort }),
              reviewDecision: makeDecision({ cohortSelection: scopedCohort }),
            });

            // Review finding 3: confirmation binds to the plan's server-owned
            // registry pin. A plan pin MISSING from the mapped payload blocks.
            const missingPinMarkup = renderPlanCard({
              plan: makePlan({ cohortSelection: scopedCohort, cohortRegistryPin: undefined }),
              reviewDecision: makeDecision({ cohortSelection: scopedCohort }),
            });
            // A plan pin that does not EXACTLY equal the options pin blocks.
            const mismatchPinMarkup = renderPlanCard({
              plan: makePlan({
                cohortSelection: scopedCohort,
                cohortRegistryPin: {
                  registryVersion: "cohort_selection.registry.v1",
                  registryDigest: "other-digest",
                },
              }),
              reviewDecision: makeDecision({ cohortSelection: scopedCohort }),
            });

            console.log(JSON.stringify({
              valid: {
                hasPreview: validPreviewIndex >= 0,
                hasFullRecallWarning: validMarkup.includes('data-testid="plan-full-recall-warning"'),
                hasBudgetNote: validBudgetIndex >= 0,
                previewBeforeConfirm:
                  validPreviewIndex >= 0 && validConfirmIndex >= 0 && validPreviewIndex < validConfirmIndex,
                budgetBeforeConfirm:
                  validBudgetIndex >= 0 && validConfirmIndex >= 0 && validBudgetIndex < validConfirmIndex,
                confirmDisabled: buttonTag(validMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: validMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
              },
              loading: {
                confirmDisabled: buttonTag(loadingMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: loadingMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
                hasPreview: loadingMarkup.includes('data-testid="plan-cohort-shard-preview"'),
              },
              failure: {
                confirmDisabled: buttonTag(failureMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: failureMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
                blockerMentionsError: failureMarkup.includes("options fetch failed"),
                hasRetry:
                  failureMarkup.indexOf('data-testid="plan-cohort-preview-blocked"') >= 0
                  && failureMarkup.indexOf(
                    "重试",
                    failureMarkup.indexOf('data-testid="plan-cohort-preview-blocked"'),
                  ) >= 0,
                hasPreview: failureMarkup.includes('data-testid="plan-cohort-shard-preview"'),
              },
              stale: {
                confirmDisabled: buttonTag(staleMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: staleMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
                blockerMentionsRole: staleMarkup.includes("removed_role"),
                hasPreview: staleMarkup.includes('data-testid="plan-cohort-shard-preview"'),
              },
              legacy: {
                confirmDisabled: buttonTag(legacyMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: legacyMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
                hasPreview: legacyMarkup.includes('data-testid="plan-cohort-shard-preview"'),
              },
              scoped: {
                hasPreview: scopedMarkup.includes('data-testid="plan-cohort-shard-preview"'),
                hasFullRecallWarning: scopedMarkup.includes('data-testid="plan-full-recall-warning"'),
                confirmDisabled: buttonTag(scopedMarkup, "plan-confirm-button").includes("disabled"),
              },
              missingPin: {
                confirmDisabled: buttonTag(missingPinMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: missingPinMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
                blockerMentionsPin: missingPinMarkup.includes("注册表 pin"),
                hasPreview: missingPinMarkup.includes('data-testid="plan-cohort-shard-preview"'),
              },
              mismatchPin: {
                confirmDisabled: buttonTag(mismatchPinMarkup, "plan-confirm-button").includes("disabled"),
                hasBlocker: mismatchPinMarkup.includes('data-testid="plan-cohort-preview-blocked"'),
                blockerMentionsMismatch: mismatchPinMarkup.includes("不一致"),
                hasPreview: mismatchPinMarkup.includes('data-testid="plan-cohort-shard-preview"'),
              },
            }));
            """
        )
        payload = _run_node(script)

        valid = payload["valid"]
        self.assertTrue(valid["hasPreview"])
        self.assertTrue(valid["hasFullRecallWarning"])
        self.assertTrue(valid["hasBudgetNote"])
        self.assertTrue(valid["previewBeforeConfirm"])
        self.assertTrue(valid["budgetBeforeConfirm"])
        self.assertFalse(valid["confirmDisabled"])
        self.assertFalse(valid["hasBlocker"])

        loading = payload["loading"]
        self.assertTrue(loading["confirmDisabled"])
        self.assertTrue(loading["hasBlocker"])
        self.assertFalse(loading["hasPreview"])

        failure = payload["failure"]
        self.assertTrue(failure["confirmDisabled"])
        self.assertTrue(failure["hasBlocker"])
        self.assertTrue(failure["blockerMentionsError"])
        self.assertTrue(failure["hasRetry"])
        self.assertFalse(failure["hasPreview"])

        stale = payload["stale"]
        self.assertTrue(stale["confirmDisabled"])
        self.assertTrue(stale["hasBlocker"])
        self.assertTrue(stale["blockerMentionsRole"])
        self.assertFalse(stale["hasPreview"])

        legacy = payload["legacy"]
        self.assertFalse(legacy["confirmDisabled"])
        self.assertFalse(legacy["hasBlocker"])
        self.assertFalse(legacy["hasPreview"])

        scoped = payload["scoped"]
        self.assertTrue(scoped["hasPreview"])
        self.assertFalse(scoped["hasFullRecallWarning"])
        self.assertFalse(scoped["confirmDisabled"])

        # Review finding 3: pin evidence unavailable -> blocked; pin mismatch
        # vs the options response -> blocked; the validated preview is hidden
        # in both cases.
        missing_pin = payload["missingPin"]
        self.assertTrue(missing_pin["confirmDisabled"])
        self.assertTrue(missing_pin["hasBlocker"])
        self.assertTrue(missing_pin["blockerMentionsPin"])
        self.assertFalse(missing_pin["hasPreview"])

        mismatch_pin = payload["mismatchPin"]
        self.assertTrue(mismatch_pin["confirmDisabled"])
        self.assertTrue(mismatch_pin["hasBlocker"])
        self.assertTrue(mismatch_pin["blockerMentionsMismatch"])
        self.assertFalse(mismatch_pin["hasPreview"])

        # The fail-closed handler is bound on the production button, and the
        # disabled state is the same cohortPreviewBlocker that gates it.
        # Review finding 4 (r3): the button is ALSO cross-disabled while any
        # plan action (revision/confirmation) is in flight, so overlapping
        # actions against different plan identities cannot both succeed.
        plan_source = (REPO_ROOT / "frontend-demo/src/components/PlanCard.tsx").read_text(encoding="utf-8")
        self.assertIn("onClick={handleConfirm}", plan_source)
        self.assertIn("disabled={planActionBusy || Boolean(cohortPreviewBlocker)}", plan_source)
        self.assertIn("if (cohortPreviewBlocker) {", plan_source)

    def test_location_fields_round_trip_through_real_request_paths(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            const defaultCohort = cohortSelection.createDefaultCohortSelection(parsedOptions);

            // --- Initial submit + revision through the REAL payload builder
            // with EXPLICIT request-owned location arguments (no ambient
            // registry exists anymore). ---
            const seedSubmit = api.__testBuildPlanSubmitPayload("find people", "", defaultCohort);
            const editedSubmit = api.__testBuildPlanSubmitPayload(
              "find people",
              "",
              defaultCohort,
              ["Canada"],
              ["Europe"],
            );
            const editedRevision = api.__testBuildPlanSubmitPayload(
              "find people with revision",
              "history-server-owned-1",
              defaultCohort,
              ["Canada"],
              ["Europe"],
            );
            const optOutSubmit = api.__testBuildPlanSubmitPayload(
              "find people",
              "",
              defaultCohort,
              [],
              undefined,
            );
            // Non-Cohort requests never gain locations, even when the caller
            // forgets to gate the arguments.
            const nonCohortSubmit = api.__testBuildPlanSubmitPayload("find people");
            // Presence tri-state: absent on both axes omits the keys.
            const absentCohortSubmit = api.__testBuildPlanSubmitPayload(
              "find people",
              "",
              defaultCohort,
              undefined,
              undefined,
            );

            // --- Review path: locations only through the authorized channel,
            // and the two fields are independent contracts. ---
            const unauthorizedReview = api.planReviewDecisionToApiPayload(
              makeDecision({
                cohortSelection: explicitCohort,
                targetLocations: ["Canada"],
                excludeTargetLocations: ["Europe"],
              }),
              [],
            );
            const targetOnlyReview = api.planReviewDecisionToApiPayload(
              makeDecision({
                cohortSelection: explicitCohort,
                targetLocations: ["Canada"],
                excludeTargetLocations: ["Europe"],
              }),
              ["target_locations"],
            );
            const excludeOnlyReview = api.planReviewDecisionToApiPayload(
              makeDecision({
                cohortSelection: explicitCohort,
                targetLocations: ["Canada"],
                excludeTargetLocations: [],
              }),
              ["exclude_target_locations"],
            );
            const authorizedReview = api.planReviewDecisionToApiPayload(
              makeDecision({
                cohortSelection: explicitCohort,
                targetLocations: ["Canada"],
                excludeTargetLocations: [],
              }),
              ["target_locations", "exclude_target_locations"],
            );
            const nullReviewError = captureError(() =>
              api.planReviewDecisionToApiPayload(
                makeDecision({ cohortSelection: explicitCohort, targetLocations: null }),
                ["target_locations"],
              ),
            );
            // Tagged clear wire contract (rerun3 review finding 6): an
            // initialized, gate-authorized axis restored to the ABSENT state
            // serializes as the explicit tagged clear operation — never an
            // omitted key (which would read as "not part of this decision").
            const clearedReview = api.planReviewDecisionToApiPayload(
              makeDecision({
                cohortSelection: explicitCohort,
                targetLocationsInitialized: true,
                excludeTargetLocationsInitialized: true,
              }),
              ["target_locations", "exclude_target_locations"],
            );
            // An axis the decision never initialized stays omitted (no
            // silent write through the authorized channel).
            const untouchedReview = api.planReviewDecisionToApiPayload(
              makeDecision({ cohortSelection: explicitCohort }),
              ["target_locations"],
            );

            // --- cloneReviewDecision must not silently drop the fields. ---
            const clonedDecision = historyRecovery.cloneReviewDecision(
              makePlan({
                reviewDecisionDefaults: makeDecision({
                  cohortSelection: explicitCohort,
                  targetLocations: ["Canada"],
                  excludeTargetLocations: [],
                }),
              }),
            );

            // --- Recovery mirrors: tri-state + stale values + fail closed. ---
            const recoveredPlan = api.__testMapPlanPayloadToDemoPlan(
              {
                request: {
                  raw_user_request: "find people",
                  cohort_selection: explicitCohort,
                  target_locations: [" 旧金山  Bay Area "],
                  exclude_target_locations: ["Europe"],
                },
                request_preview: {
                  cohort_selection: explicitCohort,
                  target_locations: [" 旧金山  Bay Area "],
                  exclude_target_locations: ["Europe"],
                },
                plan: cohortPlanRecord(),
              },
              "find people",
            );
            const recoveredOptOut = api.__testMapPlanPayloadToDemoPlan(
              {
                request: { cohort_selection: explicitCohort, target_locations: [] },
                request_preview: { cohort_selection: explicitCohort, target_locations: [] },
                plan: cohortPlanRecord(),
              },
              "find people",
            );
            const legacyPlan = api.__testMapPlanPayloadToDemoPlan(
              {
                request: { raw_user_request: "find people", cohort_selection: explicitCohort },
                request_preview: { cohort_selection: explicitCohort },
                plan: cohortPlanRecord(),
              },
              "find people",
            );
            const conflictError = captureError(() =>
              api.__testMapPlanPayloadToDemoPlan(
                {
                  request: { cohort_selection: explicitCohort, target_locations: ["Canada"] },
                  request_preview: { cohort_selection: explicitCohort, target_locations: ["United States"] },
                  plan: cohortPlanRecord(),
                },
                "find people",
              ),
            );
            const nullMirrorError = captureError(() =>
              api.__testMapPlanPayloadToDemoPlan(
                {
                  request: { cohort_selection: explicitCohort, target_locations: null },
                  plan: cohortPlanRecord(),
                },
                "find people",
              ),
            );
            const tooManyError = captureError(() =>
              cohortSelection.buildCohortLocationApiPayload(Array(17).fill("x"), undefined),
            );
            const tooLongError = captureError(() =>
              cohortSelection.buildCohortLocationApiPayload(["x".repeat(241)], undefined),
            );
            const nullItemError = captureError(() =>
              cohortSelection.buildCohortLocationApiPayload(["United States", null], undefined),
            );
            const nullFieldError = captureError(() =>
              cohortSelection.buildCohortLocationApiPayload(null, undefined),
            );
            const locationInsideCohortError = captureError(() =>
              cohortSelection.parseCohortSelectionPayload({
                ...explicitCohort,
                target_locations: ["United States"],
              }),
            );
            const defaultSummary = cohortSelection.summarizeCohortLocations(undefined, undefined);
            const optOutSummary = cohortSelection.summarizeCohortLocations([], []);
            const staleSummary = cohortSelection.summarizeCohortLocations(["旧金山  Bay Area"], []);

            console.log(JSON.stringify({
              seedSubmit,
              editedSubmit,
              editedRevision,
              optOutSubmit,
              nonCohortSubmit,
              absentCohortSubmit,
              unauthorizedReview,
              targetOnlyReview,
              excludeOnlyReview,
              authorizedReview,
              nullReviewError,
              clearedReview,
              untouchedReview,
              clonedDecision: {
                targetLocations: clonedDecision.targetLocations ?? null,
                excludeTargetLocations: clonedDecision.excludeTargetLocations ?? null,
                hasTargetKey: Object.prototype.hasOwnProperty.call(clonedDecision, "targetLocations"),
                hasExcludeKey: Object.prototype.hasOwnProperty.call(clonedDecision, "excludeTargetLocations"),
              },
              recovered: {
                targetLocations: recoveredPlan.targetLocations ?? null,
                excludeTargetLocations: recoveredPlan.excludeTargetLocations ?? null,
                reviewDefaultTargetLocations: recoveredPlan.reviewDecisionDefaults.targetLocations ?? null,
                cohortSelection: recoveredPlan.cohortSelection,
              },
              recoveredOptOut: {
                targetLocations: recoveredOptOut.targetLocations ?? null,
                hasTargetKey: Object.prototype.hasOwnProperty.call(recoveredOptOut, "targetLocations"),
                optOutValue: recoveredOptOut.targetLocations,
              },
              legacyTargetLocations: legacyPlan.targetLocations ?? null,
              conflictError,
              nullMirrorError,
              tooManyError,
              tooLongError,
              nullItemError,
              nullFieldError,
              locationInsideCohortError,
              defaultSummary,
              optOutSummary,
              staleSummary,
            }));
            """
        )
        payload = _run_node(script)

        explicit_cohort = {
            "schema_version": "cohort_selection.v1",
            "role_bucket_ids": ["research", "engineering"],
            "employment_statuses": ["current", "former"],
            "role_match": "any",
            "source": "user_explicit",
        }

        # Seeded (absent) state: omitted -> server default applies.
        self.assertNotIn("target_locations", payload["seedSubmit"])
        self.assertNotIn("exclude_target_locations", payload["seedSubmit"])
        self.assertNotIn("target_locations", payload["absentCohortSubmit"])

        # Edited locations enter BOTH the real initial and revision payloads,
        # alongside (never inside) the closed 5-field cohort object.
        edited = payload["editedSubmit"]
        self.assertEqual(edited["target_locations"], ["Canada"])
        self.assertEqual(edited["exclude_target_locations"], ["Europe"])
        self.assertEqual(
            sorted(edited["cohort_selection"].keys()),
            ["employment_statuses", "role_bucket_ids", "role_match", "schema_version", "source"],
        )
        revision = payload["editedRevision"]
        self.assertEqual(revision["history_id"], "history-server-owned-1")
        self.assertEqual(revision["target_locations"], ["Canada"])
        self.assertEqual(revision["exclude_target_locations"], ["Europe"])

        # Explicit [] = opt-out: serialized, not omitted.
        self.assertEqual(payload["optOutSubmit"]["target_locations"], [])

        # Non-cohort submits never gain locations.
        self.assertNotIn("target_locations", payload["nonCohortSubmit"])
        self.assertNotIn("cohort_selection", payload["nonCohortSubmit"])

        # Review: unauthorized decisions carry NO location fields; the two
        # fields serialize INDEPENDENTLY through their own authorized
        # channels (independent contracts, review finding 2).
        self.assertNotIn("target_locations", payload["unauthorizedReview"])
        self.assertNotIn("exclude_target_locations", payload["unauthorizedReview"])
        self.assertEqual(payload["unauthorizedReview"]["cohort_selection"], explicit_cohort)
        self.assertEqual(payload["targetOnlyReview"]["target_locations"], ["Canada"])
        self.assertNotIn("exclude_target_locations", payload["targetOnlyReview"])
        self.assertNotIn("target_locations", payload["excludeOnlyReview"])
        self.assertEqual(payload["excludeOnlyReview"]["exclude_target_locations"], [])
        self.assertEqual(payload["authorizedReview"]["target_locations"], ["Canada"])
        self.assertEqual(payload["authorizedReview"]["exclude_target_locations"], [])
        self.assertIn("target_locations must be a list of names", payload["nullReviewError"])

        # Tagged clear (rerun3 finding 6): initialized + authorized + restored
        # absence serializes as the explicit tagged operation; an
        # uninitialized axis stays omitted.
        self.assertEqual(payload["clearedReview"]["target_locations"], {"op": "clear"})
        self.assertEqual(payload["clearedReview"]["exclude_target_locations"], {"op": "clear"})
        self.assertNotIn("target_locations", payload["untouchedReview"])

        # cloneReviewDecision preserves the fields presence-intact.
        cloned = payload["clonedDecision"]
        self.assertEqual(cloned["targetLocations"], ["Canada"])
        self.assertEqual(cloned["excludeTargetLocations"], [])
        self.assertTrue(cloned["hasTargetKey"])
        self.assertTrue(cloned["hasExcludeKey"])

        # Recovery: stale historical free-text preserved (trimmed only, never
        # silently deleted); explicit opt-out and legacy absence round-trip
        # distinctly.
        recovered = payload["recovered"]
        self.assertEqual(recovered["targetLocations"], ["旧金山  Bay Area"])
        self.assertEqual(recovered["excludeTargetLocations"], ["Europe"])
        self.assertEqual(recovered["reviewDefaultTargetLocations"], ["旧金山  Bay Area"])
        self.assertEqual(recovered["cohortSelection"], explicit_cohort)
        self.assertEqual(payload["recoveredOptOut"]["optOutValue"], [])
        self.assertTrue(payload["recoveredOptOut"]["hasTargetKey"])
        self.assertIsNone(payload["legacyTargetLocations"])

        # Fail closed: conflicting mirrors, present null, bounds violations,
        # null items, and smuggling locations inside the closed object.
        self.assertIn("conflicting target_locations mirrors", payload["conflictError"])
        self.assertIn("target_locations must be a list of names", payload["nullMirrorError"])
        self.assertIn("at most 16 items", payload["tooManyError"])
        self.assertIn("1-240 characters", payload["tooLongError"])
        self.assertIn("non-empty names", payload["nullItemError"])
        self.assertIn("target_locations must be a list of names", payload["nullFieldError"])
        self.assertIn("do not match the v1 contract", payload["locationInsideCohortError"])

        # Display tri-state: server default vs explicit opt-out vs stale values.
        self.assertEqual(payload["defaultSummary"], "目标地区: United States（服务端默认）")
        self.assertEqual(payload["optOutSummary"], "目标地区: 不限地区（已显式退出地区筛选）")
        self.assertEqual(payload["staleSummary"], "目标地区: 旧金山  Bay Area")

    def test_facet_consumption_atomic_pair_and_fail_closed(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            // --- Real projection path: the atomic pair from the canonical
            // top-level served layer, byte-exact (review finding 6). ---
            const topLevel = api.__testDeriveCandidate({
              candidate_id: "served-top",
              function_bucket_ids: ["research", "engineering"],
              function_bucket_source: "lane_membership",
            });
            const matchingMirrors = api.__testDeriveCandidate({
              candidate_id: "served-agree",
              function_bucket_ids: ["research"],
              function_bucket_source: "registry_evidence",
              metadata: {
                function_bucket_ids: ["research"],
                function_bucket_source: "registry_evidence",
              },
            });
            const verbatimCase = api.__testDeriveCandidate({
              candidate_id: "served-verbatim",
              function_bucket_ids: ["Research"],
              function_bucket_source: "registry_evidence",
            });
            // The metadata mirror is comparison-only: a mirror without the
            // canonical top-level pair fails closed.
            const mirrorOnlyError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-mirror-only",
                metadata: {
                  function_bucket_ids: ["founding"],
                  function_bucket_source: "registry_evidence",
                },
              }),
            );
            const conflictError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-conflict",
                function_bucket_ids: ["research"],
                function_bucket_source: "registry_evidence",
                metadata: {
                  function_bucket_ids: ["engineering"],
                  function_bucket_source: "registry_evidence",
                },
              }),
            );
            const sourceConflictError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-source-conflict",
                function_bucket_ids: ["research"],
                function_bucket_source: "lane_membership",
                metadata: {
                  function_bucket_ids: ["research"],
                  function_bucket_source: "legacy_inference",
                },
              }),
            );
            const missingPartnerError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-missing-partner",
                function_bucket_ids: ["research"],
              }),
            );
            const sourceOnlyError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-source-only",
                function_bucket_source: "registry_evidence",
              }),
            );
            const wrongShapeError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-shape",
                function_bucket_ids: "research",
                function_bucket_source: "registry_evidence",
              }),
            );
            const emptyListError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-empty",
                function_bucket_ids: [],
                function_bucket_source: "registry_evidence",
              }),
            );
            const badSourceError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-source",
                function_bucket_ids: ["research"],
                function_bucket_source: "client_guess",
              }),
            );
            // Strict bytes (review finding 6): whitespace variants, duplicate
            // ids, present nulls, and padded provenance all fail closed —
            // nothing is trimmed/deduped/repaired into validity.
            const whitespaceIdError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-whitespace-id",
                function_bucket_ids: [" research "],
                function_bucket_source: "registry_evidence",
              }),
            );
            const duplicateIdsError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-duplicate-ids",
                function_bucket_ids: ["research", "research"],
                function_bucket_source: "registry_evidence",
              }),
            );
            const nullIdsError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-null-ids",
                function_bucket_ids: null,
                function_bucket_source: "registry_evidence",
              }),
            );
            const nullSourceError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-null-source",
                function_bucket_ids: ["research"],
                function_bucket_source: null,
              }),
            );
            const paddedSourceError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-padded-source",
                function_bucket_ids: ["research"],
                function_bucket_source: " registry_evidence ",
              }),
            );
            // Legacy record without the pair (functionIds ["24"] would have
            // been projected as research by the backend): no facet membership
            // is synthesized client-side.
            const legacyNoPair = api.__testDeriveCandidate({
              candidate_id: "legacy-no-pair",
              function_ids: ["24"],
            });
            // Materialized/profile layers never override the build-point
            // pair: a conflicting enrichment pair fails closed, a
            // materialized-only pair fails closed, an identical pair keeps
            // the base bytes, and an absent enrichment pair keeps the base.
            const overlayConflictError = captureError(() =>
              api.__testDeriveCandidateFromNormalizedRecord(
                {
                  candidate_id: "overlay-conflict",
                  function_bucket_ids: ["research"],
                  function_bucket_source: "registry_evidence",
                },
                {
                  function_bucket_ids: ["engineering"],
                  function_bucket_source: "lane_membership",
                },
              ),
            );
            const overlayMaterializedOnlyError = captureError(() =>
              api.__testDeriveCandidateFromNormalizedRecord(
                { candidate_id: "overlay-materialized-only" },
                {
                  function_bucket_ids: ["engineering"],
                  function_bucket_source: "lane_membership",
                },
              ),
            );
            const overlayIdentical = api.__testDeriveCandidateFromNormalizedRecord(
              {
                candidate_id: "overlay-identical",
                function_bucket_ids: ["research"],
                function_bucket_source: "registry_evidence",
              },
              {
                function_bucket_ids: ["research"],
                function_bucket_source: "registry_evidence",
              },
            );
            const overlayKeepsBase = api.__testDeriveCandidateFromNormalizedRecord(
              {
                candidate_id: "overlay-keep",
                function_bucket_ids: ["research"],
                function_bucket_source: "registry_evidence",
              },
              {},
            );

            // --- Real filter matching over the derived candidates (the
            // per-candidate MATCH path; there is no local OPTION source). ---
            const candidates = [
              {
                ...baseCandidate,
                id: "dual-bucket-member",
                functionBucketIds: ["engineering", "infra_systems"],
                functionBucketSource: "lane_membership",
              },
              {
                ...baseCandidate,
                id: "founding-member",
                functionBucketIds: ["founding"],
                functionBucketSource: "registry_evidence",
              },
              {
                ...baseCandidate,
                id: "server-unknown",
                functionBucketIds: ["unknown"],
                functionBucketSource: "legacy_inference",
              },
              {
                ...baseCandidate,
                id: "facet-unavailable",
                functionIds: ["24"],
              },
            ];
            const hits = (functionBuckets) =>
              candidateFilters
                .filterCandidatesByFacets(candidates, filterSelection({ functionBuckets }), [])
                .map((candidate) => candidate.id);
            console.log(JSON.stringify({
              topLevel: {
                functionBucketIds: topLevel.functionBucketIds,
                functionBucketSource: topLevel.functionBucketSource,
              },
              matchingMirrors: {
                functionBucketIds: matchingMirrors.functionBucketIds,
                functionBucketSource: matchingMirrors.functionBucketSource,
              },
              verbatimIds: verbatimCase.functionBucketIds,
              mirrorOnlyError,
              conflictError,
              sourceConflictError,
              missingPartnerError,
              sourceOnlyError,
              wrongShapeError,
              emptyListError,
              badSourceError,
              whitespaceIdError,
              duplicateIdsError,
              nullIdsError,
              nullSourceError,
              paddedSourceError,
              legacyNoPair: {
                functionBucketIds: legacyNoPair.functionBucketIds ?? null,
                functionBucketSource: legacyNoPair.functionBucketSource ?? null,
              },
              overlayConflictError,
              overlayMaterializedOnlyError,
              overlayIdentical: {
                functionBucketIds: overlayIdentical.functionBucketIds,
                functionBucketSource: overlayIdentical.functionBucketSource,
              },
              overlayKeepsBase: {
                functionBucketIds: overlayKeepsBase.functionBucketIds,
                functionBucketSource: overlayKeepsBase.functionBucketSource,
              },
              localOptionSourceExported:
                typeof candidateFilters.buildFunctionOptions !== "undefined"
                || typeof candidateFilters.defaultFunctionSelection !== "undefined",
              engineeringHits: hits(["engineering"]),
              infraHits: hits(["infra_systems"]),
              foundingHits: hits(["founding"]),
              unknownHits: hits(["unknown"]),
              unfilteredHits: hits([]),
            }));
            """
        )
        payload = _run_node(script)

        # Atomic pair, verbatim from the authoritative top-level layer.
        self.assertEqual(payload["topLevel"]["functionBucketIds"], ["research", "engineering"])
        self.assertEqual(payload["topLevel"]["functionBucketSource"], "lane_membership")
        self.assertEqual(payload["matchingMirrors"]["functionBucketIds"], ["research"])
        # Ids are consumed verbatim, never case-normalized.
        self.assertEqual(payload["verbatimIds"], ["Research"])

        # Fail closed on every malformed/conflicting shape, including the
        # comparison-only mirror without a top-level pair.
        self.assertIn("without the canonical top-level pair", payload["mirrorOnlyError"])
        self.assertIn("conflicting function_bucket mirrors", payload["conflictError"])
        self.assertIn("conflicting function_bucket mirrors", payload["sourceConflictError"])
        self.assertIn("one field missing", payload["missingPartnerError"])
        self.assertIn("one field missing", payload["sourceOnlyError"])
        self.assertIn("incomplete function_bucket pair", payload["wrongShapeError"])
        self.assertIn("incomplete function_bucket pair", payload["emptyListError"])
        self.assertIn("invalid function_bucket_source", payload["badSourceError"])

        # Strict bytes: no trim/dedupe/null repair anywhere.
        self.assertIn("malformed function_bucket_ids", payload["whitespaceIdError"])
        self.assertIn("duplicate function_bucket_ids", payload["duplicateIdsError"])
        self.assertIn("present null", payload["nullIdsError"])
        self.assertIn("present null", payload["nullSourceError"])
        self.assertIn("invalid function_bucket_source", payload["paddedSourceError"])

        # Missing pair: facet unavailable, never repaired into membership.
        self.assertIsNone(payload["legacyNoPair"]["functionBucketIds"])
        self.assertIsNone(payload["legacyNoPair"]["functionBucketSource"])

        # Materialized/profile layers never override the build-point pair.
        self.assertIn("conflicts with the canonical function_bucket pair", payload["overlayConflictError"])
        self.assertIn("without the canonical build-point pair", payload["overlayMaterializedOnlyError"])
        self.assertEqual(payload["overlayIdentical"]["functionBucketIds"], ["research"])
        self.assertEqual(payload["overlayIdentical"]["functionBucketSource"], "registry_evidence")
        self.assertEqual(payload["overlayKeepsBase"]["functionBucketIds"], ["research"])
        self.assertEqual(payload["overlayKeepsBase"]["functionBucketSource"], "registry_evidence")

        # Review finding 5: no local function-facet OPTION source is exported.
        self.assertFalse(payload["localOptionSourceExported"])

        # Matching: server ids only, including infra_systems/founding; the
        # unavailable-facet row claims no membership in any bucket.
        self.assertEqual(payload["engineeringHits"], ["dual-bucket-member"])
        self.assertEqual(payload["infraHits"], ["dual-bucket-member"])
        self.assertEqual(payload["foundingHits"], ["founding-member"])
        self.assertEqual(payload["unknownHits"], ["server-unknown"])
        self.assertEqual(
            sorted(payload["unfilteredHits"]),
            ["dual-bucket-member", "facet-unavailable", "founding-member", "server-unknown"],
        )

        # The deleted local taxonomy and option source may not return.
        filters_source = (REPO_ROOT / "frontend-demo/src/lib/candidateFilters.ts").read_text(encoding="utf-8")
        self.assertNotIn("const ROLE_BUCKET_TO_FUNCTION_BUCKET", filters_source)
        self.assertNotIn("const FUNCTION_BUCKET_KEYWORDS", filters_source)
        self.assertNotIn("inferredFunctionBucketFromProfile(", filters_source)
        self.assertNotIn("countKeywordOccurrences(", filters_source)
        self.assertNotIn('normalizedIds.includes("24")', filters_source)
        self.assertNotIn('normalizedIds.includes("8")', filters_source)
        self.assertNotIn('normalizedIds.includes("19")', filters_source)
        self.assertNotIn("export function buildFunctionOptions", filters_source)
        self.assertNotIn("export function defaultFunctionSelection", filters_source)
        self.assertNotIn("buildFunctionOptions(", filters_source)
        self.assertNotIn("defaultFunctionSelection(", filters_source)
        self.assertIn("candidate.functionBucketIds", filters_source)
        api_source = (REPO_ROOT / "frontend-demo/src/lib/api.ts").read_text(encoding="utf-8")
        self.assertNotIn("pickNonEmptyStringList", api_source)
        self.assertNotIn("pickFunctionBucketSource", api_source)

    def test_employment_membership_dual_status_and_single_display(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            // PRODUCTION SHAPE (rerun3 findings 3/8): the backend's canonical
            // served field is TOP-LEVEL `employment_statuses`
            // (`candidate_artifacts.py`); `metadata.cohort_employment_statuses`
            // is a comparison-only mirror. Display status stays single (FT0 §6).
            const dual = api.__testDeriveCandidate({
              candidate_id: "dual-member",
              employment_status: "current",
              employment_statuses: ["current", "former"],
              metadata: { cohort_employment_statuses: ["current", "former"] },
            });
            // The rerun3 probe: a top-level-only row owns membership (the old
            // frontend owner ignored the canonical layer entirely).
            const topLevelOnly = api.__testDeriveCandidate({
              candidate_id: "top-level-only",
              employment_status: "former",
              employment_statuses: ["current", "former"],
            });
            const legacy = api.__testDeriveCandidate({
              candidate_id: "legacy-lead",
              employment_status: "lead",
            });
            // A metadata mirror WITHOUT the canonical owner is invalid
            // evidence — membership is never borrowed from the mirror.
            const mirrorWithoutOwnerError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "mirror-without-owner",
                metadata: { cohort_employment_statuses: ["current", "former"] },
              }),
            );
            // A mirror that disagrees with the canonical layer fails closed.
            const conflictingMirrorError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "conflicting-mirror",
                employment_statuses: ["current", "former"],
                metadata: { cohort_employment_statuses: ["current"] },
              }),
            );
            const malformedError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "bad-membership",
                employment_statuses: "current",
              }),
            );
            const emptyMembershipError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "empty-membership",
                employment_statuses: [],
              }),
            );
            const invalidMembershipError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "invalid-membership",
                employment_statuses: ["contractor"],
              }),
            );
            // Strict bytes (review finding 4): present null is invalid (not
            // absence), whitespace/case variants are invalid (no
            // trim/lowercase repair), duplicates are invalid (no dedupe).
            const nullMembershipError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "null-membership",
                employment_statuses: null,
              }),
            );
            const paddedMembershipError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "padded-membership",
                employment_statuses: [" Current "],
              }),
            );
            const caseMembershipError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "case-membership",
                employment_statuses: ["CURRENT"],
              }),
            );
            const duplicateMembershipError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "duplicate-membership",
                employment_statuses: ["current", "current"],
              }),
            );
            // Cohort provenance without membership: fail closed — the lossy
            // display status may not become membership truth.
            const provenanceWithoutMembershipError = captureError(() =>
              api.__testDeriveCandidate({
                candidate_id: "provenance-without-membership",
                employment_status: "current",
                metadata: { cohort_lane_membership: [{ employment_status: "current" }] },
              }),
            );

            // Enrichment overlay (rerun3 finding 3): the materialized record
            // is a comparison-only mirror. A stale materialized membership
            // must NEVER replace the canonical base membership.
            const baseDual = {
              candidate_id: "enrich-dual",
              employment_status: "current",
              employment_statuses: ["current", "former"],
            };
            const enrichedAgreeing = api.__testDeriveCandidateFromNormalizedRecord(
              baseDual,
              { employment_statuses: ["current", "former"], headline: "Enriched headline" },
            );
            const enrichmentConflictError = captureError(() =>
              api.__testDeriveCandidateFromNormalizedRecord(
                baseDual,
                { employment_statuses: ["current"] },
              ),
            );
            const enrichmentMetadataMirrorError = captureError(() =>
              api.__testDeriveCandidateFromNormalizedRecord(
                baseDual,
                { metadata: { cohort_employment_statuses: ["former"] } },
              ),
            );
            // Membership evidence in the enrichment layer WITHOUT a canonical
            // build-point membership fails closed (no second owner).
            const enrichmentWithoutBaseError = captureError(() =>
              api.__testDeriveCandidateFromNormalizedRecord(
                { candidate_id: "enrich-legacy", employment_status: "lead" },
                { employment_statuses: ["current"] },
              ),
            );
            // Rerun4 finding 6: the materialized record's OWN two mirrors
            // (top-level + metadata) must agree byte-exactly — a dual-present
            // conflict fails closed instead of the top level silently winning.
            const enrichmentDualMirrorConflictError = captureError(() =>
              api.__testDeriveCandidateFromNormalizedRecord(
                baseDual,
                {
                  employment_statuses: ["current", "former"],
                  metadata: { cohort_employment_statuses: ["former"] },
                },
              ),
            );
            const enrichmentDualMirrorAgreeing = api.__testDeriveCandidateFromNormalizedRecord(
              baseDual,
              {
                employment_statuses: ["current", "former"],
                metadata: { cohort_employment_statuses: ["current", "former"] },
              },
            );

            const candidates = [
              {
                ...baseCandidate,
                id: "dual-display-current",
                employmentStatus: "current",
                cohortEmploymentStatuses: ["current", "former"],
              },
              {
                ...baseCandidate,
                id: "single-former",
                employmentStatus: "former",
                cohortEmploymentStatuses: ["former"],
              },
              {
                ...baseCandidate,
                id: "legacy-display-current",
                employmentStatus: "current",
              },
              {
                ...baseCandidate,
                id: "legacy-lead",
                employmentStatus: "lead",
              },
            ];
            const employmentOptions = candidateFilters.buildEmploymentOptions(candidates);
            const hits = (employmentStatuses) =>
              candidateFilters
                .filterCandidatesByFacets(candidates, filterSelection({ employmentStatuses }), [])
                .map((candidate) => candidate.id);
            console.log(JSON.stringify({
              dual: {
                employmentStatus: dual.employmentStatus,
                cohortEmploymentStatuses: dual.cohortEmploymentStatuses,
              },
              topLevelOnly: {
                employmentStatus: topLevelOnly.employmentStatus,
                cohortEmploymentStatuses: topLevelOnly.cohortEmploymentStatuses,
              },
              legacyMembership: legacy.cohortEmploymentStatuses ?? null,
              mirrorWithoutOwnerError,
              conflictingMirrorError,
              malformedError,
              emptyMembershipError,
              invalidMembershipError,
              nullMembershipError,
              paddedMembershipError,
              caseMembershipError,
              duplicateMembershipError,
              provenanceWithoutMembershipError,
              enrichedAgreeing: {
                cohortEmploymentStatuses: enrichedAgreeing.cohortEmploymentStatuses,
                headline: enrichedAgreeing.headline,
              },
              enrichmentConflictError,
              enrichmentMetadataMirrorError,
              enrichmentWithoutBaseError,
              enrichmentDualMirrorConflictError,
              enrichmentDualMirrorAgreeing: enrichmentDualMirrorAgreeing.cohortEmploymentStatuses,
              employmentOptions,
              currentOnly: hits(["current"]),
              formerOnly: hits(["former"]),
              bothStatuses: hits(["current", "former"]),
              displayStatuses: candidates.map((candidate) => candidate.employmentStatus),
            }));
            """
        )
        payload = _run_node(script)

        # Membership truth comes from the canonical top-level layer (verbatim);
        # the metadata mirror may only confirm it. Display stays single-valued.
        self.assertEqual(payload["dual"]["employmentStatus"], "current")
        self.assertEqual(payload["dual"]["cohortEmploymentStatuses"], ["current", "former"])
        self.assertEqual(payload["topLevelOnly"]["employmentStatus"], "former")
        self.assertEqual(payload["topLevelOnly"]["cohortEmploymentStatuses"], ["current", "former"])
        self.assertIsNone(payload["legacyMembership"])

        # No borrowed/conflicting membership: mirror-without-owner and
        # disagreeing mirrors fail closed.
        self.assertIn("mirror without the canonical", payload["mirrorWithoutOwnerError"])
        self.assertIn("conflicting employment_statuses mirrors", payload["conflictingMirrorError"])
        self.assertIn("malformed employment_statuses", payload["malformedError"])
        self.assertIn("empty employment_statuses", payload["emptyMembershipError"])
        self.assertIn("invalid employment_statuses", payload["invalidMembershipError"])

        # Strict bytes: present-null / padded / case / duplicate all invalid.
        self.assertIn("malformed employment_statuses", payload["nullMembershipError"])
        self.assertIn("invalid employment_statuses", payload["paddedMembershipError"])
        self.assertIn("invalid employment_statuses", payload["caseMembershipError"])
        self.assertIn("duplicate employment_statuses", payload["duplicateMembershipError"])

        # Cohort provenance without membership fails closed (no display-status
        # fallback when Cohort provenance is expected).
        self.assertIn(
            "Cohort provenance without employment_statuses",
            payload["provenanceWithoutMembershipError"],
        )

        # Enrichment: an agreeing materialized mirror keeps the canonical base
        # membership (and the enrichment still lands); a stale materialized
        # membership — top-level or metadata-shaped — and membership evidence
        # without a build-point owner all fail closed.
        self.assertEqual(
            payload["enrichedAgreeing"]["cohortEmploymentStatuses"],
            ["current", "former"],
        )
        self.assertEqual(payload["enrichedAgreeing"]["headline"], "Enriched headline")
        self.assertIn(
            "conflicts with the canonical employment_statuses membership",
            payload["enrichmentConflictError"],
        )
        self.assertIn(
            "conflicts with the canonical employment_statuses membership",
            payload["enrichmentMetadataMirrorError"],
        )
        self.assertIn(
            "without the canonical build-point membership",
            payload["enrichmentWithoutBaseError"],
        )
        # Rerun4 finding 6: a dual-present conflict INSIDE the materialized
        # record fails closed; dual-present agreement keeps the base.
        self.assertIn(
            "conflicting employment membership mirrors",
            payload["enrichmentDualMirrorConflictError"],
        )
        self.assertEqual(
            payload["enrichmentDualMirrorAgreeing"],
            ["current", "former"],
        )

        # The dual-status candidate counts in BOTH buckets.
        self.assertEqual(
            payload["employmentOptions"],
            [
                {"id": "current", "label": "在职", "count": 2},
                {"id": "former", "label": "已离职", "count": 2},
            ],
        )

        # ... and matches BOTH filters; legacy display-status compat and the
        # lead semantic are unchanged.
        self.assertEqual(
            sorted(payload["currentOnly"]),
            ["dual-display-current", "legacy-display-current"],
        )
        self.assertEqual(sorted(payload["formerOnly"]), ["dual-display-current", "single-former"])
        self.assertEqual(
            sorted(payload["bothStatuses"]),
            ["dual-display-current", "legacy-display-current", "legacy-lead", "single-former"],
        )

        # One card, one display status (FT0 §6.2 display-only projection).
        self.assertEqual(
            payload["displayStatuses"],
            ["current", "former", "current", "lead"],
        )

    # ------------------------------------------------------------------
    # Review finding 7: rendered production-integration matrix over the REAL
    # SearchPage -> SearchFlow -> SearchComposer/PlanCard -> picker ->
    # SourcingBackendClient -> api transport chain with a stubbed backend.
    # ------------------------------------------------------------------
    def test_request_owned_location_survives_cohort_edits_and_submits(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            (async () => {
              registerPlanBackend();
              const { container } = await mountApp();

              // Type the query, enable the cohort, add a location.
              setInputValue(findByTestId(container, "search-composer-input"), "find people");
              await settle(2);
              setCheckbox(findByTestId(container, "search-cohort-enabled"), true);
              await settle(2);
              const targetInput = findByTestId(container, "search-target-locations-input-input");
              setInputValue(targetInput, "Canada");
              pressEnter(targetInput);
              await settle(2);
              const tagsAfterLocationEdit = findAllByTestId(
                container,
                "search-target-locations-input-tag",
              ).map((node) => node.textContent.replace(/×/g, "").trim());

              // Now EDIT the cohort options (toggle the research role on).
              // Request-owned location state must NOT be silently reset.
              const researchCheckbox = findInputByValue(
                findByTestId(container, "search-cohort-roles"),
                "research",
              );
              setCheckbox(researchCheckbox, true);
              await settle(2);
              const tagsAfterCohortEdit = findAllByTestId(
                container,
                "search-target-locations-input-tag",
              ).map((node) => node.textContent.replace(/×/g, "").trim());

              // Submit through the REAL SearchPage -> SourcingBackendClient
              // -> buildPlanSubmitPayload -> fake fetch path.
              clickEl(findByTestId(container, "search-composer-submit"));
              await settle();
              const submits = callsTo("/api/plan/submit");
              const submitBody = submits.length ? submits[submits.length - 1].body : null;
              const planLocations = findByTestId(container, "plan-cohort-locations");

              console.log(JSON.stringify({
                tagsAfterLocationEdit,
                tagsAfterCohortEdit,
                submitCallCount: submits.length,
                submitBody,
                planLocationsText: planLocations ? planLocations.textContent : "",
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # The location edit renders, and the cohort option edit does NOT
        # silently reset it (round-1 defect: the draft key changed and the
        # location vanished from the request).
        self.assertEqual(payload["tagsAfterLocationEdit"], ["Canada"])
        self.assertEqual(payload["tagsAfterCohortEdit"], ["Canada"])

        # The real submit payload carries BOTH the edited cohort and the
        # request-owned locations.
        self.assertEqual(payload["submitCallCount"], 1)
        submit = payload["submitBody"]
        self.assertEqual(submit["cohort_selection"]["role_bucket_ids"], ["research"])
        self.assertEqual(
            submit["cohort_selection"]["employment_statuses"],
            ["current", "former"],
        )
        self.assertEqual(submit["target_locations"], ["Canada"])
        self.assertNotIn("exclude_target_locations", submit)
        self.assertNotIn("target_locations", submit["cohort_selection"])

        # The plan mirror displays the submitted locations.
        self.assertIn("Canada", payload["planLocationsText"])

    def test_recovered_revision_rehydrates_locations(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            const findTextareaById = (node, id) => {
              if (node.nodeType === 1 && node.nodeName === "TEXTAREA" && node.getAttribute("id") === id) {
                return node;
              }
              for (const child of node.childNodes || []) {
                const found = findTextareaById(child, id);
                if (found) return found;
              }
              return null;
            };
            const findButtonByText = (node, text) => {
              if (node.nodeType === 1 && node.nodeName === "BUTTON" && node.textContent.trim() === text) {
                return node;
              }
              for (const child of node.childNodes || []) {
                const found = findButtonByText(child, text);
                if (found) return found;
              }
              return null;
            };
            (async () => {
              const recoveredCohort = selectionFor(["research"], ["current", "former"]);
              const recoveredResponse = planResponseForRequest(
                {
                  raw_user_request: "find people",
                  cohort_selection: recoveredCohort,
                  target_locations: ["Canada"],
                  exclude_target_locations: ["Europe"],
                },
                { historyId: "hist-rec", reviewId: "review-9" },
              );
              addRoute("GET", "/api/cohort-selection/options", () => optionsPayload);
              addRoute("GET", "/api/frontend-history/", () =>
                recoveryEnvelopeFor("hist-rec", recoveredResponse));
              addRoute("POST", "/api/plan/submit", (body) =>
                planResponseForRequest(body, { historyId: "hist-rec", reviewId: "review-9" }));

              // Local copy simulates lost location state; the backend
              // recovery envelope is the source of truth.
              const localPlan = makePlan({
                cohortSelection: recoveredCohort,
                targetLocations: undefined,
                excludeTargetLocations: undefined,
              });
              searchHistoryStore.set("hist-rec", seedPlanHistoryItem("hist-rec", localPlan, "review-9"));
              routeParams.history = "hist-rec";

              const { container } = await mountApp();
              const planLocations = findByTestId(container, "plan-cohort-locations");

              // Revise the recovered plan through the REAL revision path.
              setInputValue(findTextareaById(container, "plan-revision"), "only senior people");
              await settle(2);
              clickEl(findButtonByText(container, "修改方案"));
              await settle();
              const submits = callsTo("/api/plan/submit");
              const revisionBody = submits.length ? submits[submits.length - 1].body : null;

              console.log(JSON.stringify({
                planLocationsText: planLocations ? planLocations.textContent : "",
                submitCallCount: submits.length,
                revisionBody,
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # The recovered request's locations are rehydrated into the review
        # surface (never silently default-US).
        self.assertIn("Canada", payload["planLocationsText"])
        self.assertIn("Europe", payload["planLocationsText"])

        # The revision submit carries the recovered request's locations —
        # not a silent server-default fallback.
        self.assertEqual(payload["submitCallCount"], 1)
        revision = payload["revisionBody"]
        self.assertEqual(revision["history_id"], "hist-rec")
        self.assertEqual(revision["target_locations"], ["Canada"])
        self.assertEqual(revision["exclude_target_locations"], ["Europe"])
        self.assertEqual(revision["cohort_selection"]["role_bucket_ids"], ["research"])

    def test_plan_review_location_authorization_and_confirmation(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            const findButtonByText = (node, text) => {
              if (node.nodeType === 1 && node.nodeName === "BUTTON" && node.textContent.trim().includes(text)) {
                return node;
              }
              for (const child of node.childNodes || []) {
                const found = findButtonByText(child, text);
                if (found) return found;
              }
              return null;
            };
            (async () => {
              const authorizedCohort = selectionFor(["research"], ["current", "former"]);
              const authorizedResponse = planResponseForRequest(
                {
                  raw_user_request: "find people",
                  cohort_selection: authorizedCohort,
                  target_locations: ["Canada"],
                  exclude_target_locations: ["Europe"],
                },
                {
                  historyId: "hist-auth",
                  reviewId: "review-7",
                  editableFields: ["target_locations"],
                },
              );
              addRoute("GET", "/api/cohort-selection/options", () => optionsPayload);
              addRoute("GET", "/api/frontend-history/", () =>
                recoveryEnvelopeFor("hist-auth", authorizedResponse));
              addRoute("POST", "/api/plan/review", () => ({ status: "approved" }));
              addRoute("POST", "/api/workflows", () => ({
                job_id: "job-1",
                status: "completed",
                stage: "completed",
              }));
              addRoute("GET", "/api/jobs/", () => ({
                candidates: [],
                layers: [],
                intentKeywords: [],
                totalCandidates: 0,
                manualReviewCount: 0,
              }));

              searchHistoryStore.set(
                "hist-auth",
                seedPlanHistoryItem(
                  "hist-auth",
                  makePlan({
                    cohortSelection: authorizedCohort,
                    targetLocations: ["Canada"],
                    excludeTargetLocations: ["Europe"],
                    reviewGate: {
                      status: "pending",
                      requiredBeforeExecution: true,
                      riskLevel: "low",
                      reasons: [],
                      confirmationItems: [],
                      editableFields: ["target_locations"],
                      suggestedActions: [],
                      scopeHints: [],
                      executionModeHints: [],
                    },
                  }),
                  "review-7",
                ),
              );
              routeParams.history = "hist-auth";

              const { container } = await mountApp();

              // Review finding 2: the locked cohort disables ONLY the cohort
              // option controls (their fieldset); the backend-authorized
              // location field stays editable and the unauthorized one stays
              // read-only.
              const rolesGrid = findByTestId(container, "plan-review-cohort-roles");
              const rolesFieldset = rolesGrid ? rolesGrid.parentNode : null;
              const targetInput = findByTestId(container, "plan-review-target-locations-input-input");
              const excludeInput = findByTestId(container, "plan-review-exclude-locations-input");
              const controls = {
                cohortRoleDisabled: Boolean(rolesFieldset && rolesFieldset.hasAttribute("disabled")),
                targetInputDisabled: Boolean(targetInput && targetInput.hasAttribute("disabled")),
                excludeInputDisabled: Boolean(excludeInput && excludeInput.hasAttribute("disabled")),
              };

              // Exercise the authorized control: append Mexico.
              setInputValue(targetInput, "Mexico");
              pressEnter(targetInput);
              await settle(2);

              // Confirm through the REAL approve + start path.
              clickEl(findByTestId(container, "plan-confirm-button"));
              await settle();
              const reviewCalls = callsTo("/api/plan/review");
              const workflowCalls = callsTo("/api/workflows");
              console.log(JSON.stringify({
                controls,
                reviewCallCount: reviewCalls.length,
                reviewBody: reviewCalls.length ? reviewCalls[0].body : null,
                workflowCallCount: workflowCalls.length,
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # Cohort locked but location authorized INDEPENDENTLY: the target
        # field is editable, the exclude field is not, cohort roles locked.
        self.assertTrue(payload["controls"]["cohortRoleDisabled"])
        self.assertFalse(payload["controls"]["targetInputDisabled"])
        self.assertTrue(payload["controls"]["excludeInputDisabled"])

        # The authorized edit reached the REAL review payload through the
        # authorized channel only; the unauthorized field is absent.
        self.assertEqual(payload["reviewCallCount"], 1)
        decision = payload["reviewBody"]["decision"]
        self.assertEqual(decision["target_locations"], ["Canada", "Mexico"])
        self.assertNotIn("exclude_target_locations", decision)

        # Confirmation proceeded to the workflow start.
        self.assertEqual(payload["workflowCallCount"], 1)

    def test_blocked_confirmation_makes_no_approval_or_start_request(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            (async () => {
              const unpinnedCohort = selectionFor(["research"], ["current", "former"]);
              const unpinnedResponse = planResponseForRequest(
                {
                  raw_user_request: "find people",
                  cohort_selection: unpinnedCohort,
                  target_locations: ["Canada"],
                },
                {
                  historyId: "hist-unpinned",
                  reviewId: "review-8",
                  editableFields: ["target_locations"],
                  manifest: {},
                  metadata: {},
                },
              );
              addRoute("GET", "/api/cohort-selection/options", () => optionsPayload);
              addRoute("GET", "/api/frontend-history/", () =>
                recoveryEnvelopeFor("hist-unpinned", unpinnedResponse));
              addRoute("POST", "/api/plan/review", () => ({ status: "approved" }));
              addRoute("POST", "/api/workflows", () => ({ job_id: "job-9", status: "running" }));

              searchHistoryStore.set(
                "hist-unpinned",
                seedPlanHistoryItem(
                  "hist-unpinned",
                  makePlan({
                    cohortSelection: unpinnedCohort,
                    cohortRegistryPin: undefined,
                    targetLocations: ["Canada"],
                    reviewGate: {
                      status: "pending",
                      requiredBeforeExecution: true,
                      riskLevel: "low",
                      reasons: [],
                      confirmationItems: [],
                      editableFields: ["target_locations"],
                      suggestedActions: [],
                      scopeHints: [],
                      executionModeHints: [],
                    },
                  }),
                  "review-8",
                ),
              );
              routeParams.history = "hist-unpinned";

              const { container } = await mountApp();
              const blocker = findByTestId(container, "plan-cohort-preview-blocked");
              const confirmButton = findByTestId(container, "plan-confirm-button");
              const confirmDisabled = Boolean(confirmButton && confirmButton.hasAttribute("disabled"));

              // Even a (synthetic) click on the blocked button must not reach
              // the approve/start path — the handler fails closed too.
              clickEl(confirmButton);
              await settle();

              console.log(JSON.stringify({
                blockerText: blocker ? blocker.textContent : "",
                confirmDisabled,
                reviewCallCount: callsTo("/api/plan/review").length,
                workflowCallCount: callsTo("/api/workflows").length,
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # Pin evidence unavailable -> blocked render + disabled button.
        self.assertIn("注册表 pin", payload["blockerText"])
        self.assertTrue(payload["confirmDisabled"])

        # Blocked confirmation made NO approval or start request.
        self.assertEqual(payload["reviewCallCount"], 0)
        self.assertEqual(payload["workflowCallCount"], 0)

    def test_location_mirror_reconciliation_preserves_absent_state(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            (async () => {
              const cohort = selectionFor(["research"], ["current", "former"]);
              // Review finding 1 (r4/rerun3): the canonical request
              // (`payload.request`) is the SOLE location owner. Partial
              // projections (every request_preview / intent_view variant)
              // compare ONLY the location keys they actually project — the
              // real backend preview never projects them, so its omission is
              // NOT an authoritative absent state (the rerun3 probe). A
              // preview that DOES project a key must agree byte-exactly.
              const mapError = (payload) =>
                captureError(() => api.__testMapPlanPayloadToDemoPlan(payload, "find people"));
              const mapLocations = (payload) => {
                const plan = api.__testMapPlanPayloadToDemoPlan(payload, "find people");
                return {
                  targetLocations: plan.targetLocations ?? null,
                  excludeTargetLocations: plan.excludeTargetLocations ?? null,
                  hasTargetKey: Object.prototype.hasOwnProperty.call(plan, "targetLocations"),
                  hasExcludeKey: Object.prototype.hasOwnProperty.call(plan, "excludeTargetLocations"),
                };
              };
              // Canonical absent vs a preview PROJECTING a present value is a
              // CONFLICT (the projected key disagrees with the owner).
              const absentVsPresentPreview = mapError({
                request: { raw_user_request: "find people", cohort_selection: cohort },
                request_preview: { cohort_selection: cohort, target_locations: ["Canada"] },
                plan: cohortPlanRecord(),
              });
              const absentVsPresentMetadataPreview = mapError({
                request: { raw_user_request: "find people", cohort_selection: cohort },
                plan: cohortPlanRecord(),
                metadata: { request_preview: { cohort_selection: cohort, target_locations: ["Canada"] } },
              });
              // PRODUCTION SHAPE (rerun3 finding 1 probe): canonical request
              // owns Canada; the real-backend preview omits both location
              // keys — the omission is NOT evidence and the plan maps fine.
              const productionPreviewOmission = mapLocations({
                request: { cohort_selection: cohort, target_locations: ["Canada"] },
                request_preview: { cohort_selection: cohort },
                plan: cohortPlanRecord(),
              });
              const optOutVsPreviewOmission = mapLocations({
                request: { cohort_selection: cohort, target_locations: [] },
                request_preview: { cohort_selection: cohort },
                plan: cohortPlanRecord(),
              });
              const excludeVsPreviewOmission = mapLocations({
                request: { cohort_selection: cohort, exclude_target_locations: ["Europe"] },
                request_preview: { cohort_selection: cohort },
                plan: cohortPlanRecord(),
              });
              // Every projecting preview mirror is compared, not just the
              // first truthy one: payload.request_preview agrees with the
              // canonical owner while metadata.request_preview projects a
              // stale value.
              const staleSecondPreviewMirror = mapError({
                request: { cohort_selection: cohort, target_locations: ["Canada"] },
                request_preview: { cohort_selection: cohort, target_locations: ["Canada"] },
                plan: cohortPlanRecord(),
                metadata: { request_preview: { cohort_selection: cohort, target_locations: ["United States"] } },
              });
              // Missing canonical ownership fails CLOSED (no borrowed mirror
              // owner): an explicit-Cohort plan without `payload.request` —
              // or with a non-object one — never reaches the review UI.
              const missingCanonicalRequest = mapError({
                request_preview: { cohort_selection: cohort, target_locations: ["Canada"] },
                plan: cohortPlanRecord(),
              });
              const nonObjectCanonicalRequest = mapError({
                request: "not-an-object",
                request_preview: { cohort_selection: cohort },
                plan: cohortPlanRecord(),
              });
              // COMPLETE request mirrors compare presence-intact: a stale
              // stored request conflicts; an agreeing one round-trips.
              const staleCompleteMirror = mapError({
                request: { cohort_selection: cohort, target_locations: ["Canada"] },
                plan: cohortPlanRecord(),
                metadata: { request: { cohort_selection: cohort, target_locations: ["United States"] } },
              });
              // Rerun4 finding 1: cohort_selection is REQUEST-OWNED. A
              // preview or metadata mirror carrying Cohort data while the
              // canonical request lacks it must fail closed — never a
              // borrowed explicit-Cohort plan (and never a valid-looking
              // registry pin manufactured from stale frontend evidence).
              const previewOwnedCohortBorrow = mapError({
                request: { raw_user_request: "find people" },
                request_preview: { cohort_selection: cohort },
                plan: cohortPlanRecord(),
              });
              const metadataOwnedCohortBorrow = mapError({
                request: { raw_user_request: "find people" },
                plan: cohortPlanRecord(),
                metadata: { request_preview: { cohort_selection: cohort } },
              });
              // The first-truthy preview ladder must not hide a conflicting
              // preview: payload.request_preview agrees with the canonical
              // owner while metadata.request_preview carries a DIFFERENT
              // cohort — every variant is enumerated, so this conflicts.
              const hiddenConflictingPreview = mapError({
                request: { cohort_selection: cohort },
                request_preview: { cohort_selection: cohort },
                plan: cohortPlanRecord(),
                metadata: {
                  request_preview: {
                    cohort_selection: selectionFor(["engineering"], ["current"]),
                  },
                },
              });
              // Presence-intact happy paths: every present mirror agrees.
              const agreedValues = api.__testMapPlanPayloadToDemoPlan(
                {
                  request: { cohort_selection: cohort, target_locations: ["Canada"] },
                  request_preview: { cohort_selection: cohort, target_locations: ["Canada"] },
                  plan: cohortPlanRecord(),
                  metadata: {
                    request: { cohort_selection: cohort, target_locations: ["Canada"] },
                    request_preview: { cohort_selection: cohort, target_locations: ["Canada"] },
                  },
                },
                "find people",
              );
              const agreedOptOut = api.__testMapPlanPayloadToDemoPlan(
                {
                  request: { cohort_selection: cohort, target_locations: [] },
                  request_preview: { cohort_selection: cohort, target_locations: [] },
                  plan: cohortPlanRecord(),
                },
                "find people",
              );
              const legacyAllAbsent = api.__testMapPlanPayloadToDemoPlan(
                {
                  request: { raw_user_request: "find people", cohort_selection: cohort },
                  request_preview: { cohort_selection: cohort },
                  plan: cohortPlanRecord(),
                },
                "find people",
              );

              // Rendered integration: a recovery envelope whose canonical
              // request owns the absent state while a stale request_preview
              // PROJECTS Canada must FAIL the recovery mapping — no plan
              // card, no approval/start calls.
              const staleResponse = planResponseForRequest(
                { raw_user_request: "find people", cohort_selection: cohort },
                { historyId: "hist-stale-mirror", reviewId: "review-31" },
              );
              staleResponse.request_preview = {
                cohort_selection: cohort,
                target_locations: ["Canada"],
              };
              addRoute("GET", "/api/cohort-selection/options", () => optionsPayload);
              addRoute("GET", "/api/frontend-history/", () =>
                recoveryEnvelopeFor("hist-stale-mirror", staleResponse));
              addRoute("POST", "/api/plan/review", () => ({ status: "approved" }));
              addRoute("POST", "/api/workflows", () => ({ job_id: "job-stale", status: "running" }));
              routeParams.history = "hist-stale-mirror";
              const { container } = await mountApp();
              await settle();
              console.log(JSON.stringify({
                absentVsPresentPreview,
                absentVsPresentMetadataPreview,
                productionPreviewOmission,
                optOutVsPreviewOmission,
                excludeVsPreviewOmission,
                staleSecondPreviewMirror,
                missingCanonicalRequest,
                nonObjectCanonicalRequest,
                staleCompleteMirror,
                previewOwnedCohortBorrow,
                metadataOwnedCohortBorrow,
                hiddenConflictingPreview,
                agreedValues: {
                  targetLocations: agreedValues.targetLocations ?? null,
                  hasTargetKey: Object.prototype.hasOwnProperty.call(agreedValues, "targetLocations"),
                },
                agreedOptOut: {
                  targetLocations: agreedOptOut.targetLocations ?? null,
                  hasTargetKey: Object.prototype.hasOwnProperty.call(agreedOptOut, "targetLocations"),
                },
                legacyAllAbsent: {
                  targetLocations: legacyAllAbsent.targetLocations ?? null,
                },
                rendered: {
                  hasPlanCard: Boolean(findByTestId(container, "plan-card")),
                  pageText: container.textContent.slice(0, 500),
                  historyCallCount: callsTo("/api/frontend-history/").length,
                  reviewCallCount: callsTo("/api/plan/review").length,
                  workflowCallCount: callsTo("/api/workflows").length,
                },
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # A preview PROJECTING a value that disagrees with the canonical owner
        # (in either direction, on either axis, in any preview mirror) is a
        # CONFLICT, not a silent adoption of the stale value. A stale
        # COMPLETE request mirror conflicts presence-intact as well.
        for key in (
            "absentVsPresentPreview",
            "absentVsPresentMetadataPreview",
            "staleSecondPreviewMirror",
            "staleCompleteMirror",
        ):
            self.assertIn("conflicting target_locations mirrors", payload[key], key)

        # Missing/non-object canonical ownership fails closed (never borrowed).
        self.assertIn("missing the canonical request owner", payload["missingCanonicalRequest"])
        self.assertIn("missing the canonical request owner", payload["nonObjectCanonicalRequest"])

        # Rerun4 finding 1: a preview/metadata mirror carrying Cohort data
        # without the canonical request owner fails closed (no borrowed
        # explicit-Cohort plan); a conflicting preview hidden behind the
        # first-truthy ladder is enumerated and conflicts.
        self.assertIn(
            "cohort_selection mirrors without the canonical request owner",
            payload["previewOwnedCohortBorrow"],
        )
        self.assertIn(
            "cohort_selection mirrors without the canonical request owner",
            payload["metadataOwnedCohortBorrow"],
        )
        self.assertIn("conflicting cohort_selection mirrors", payload["hiddenConflictingPreview"])

        # Production-shape preview omission is NOT a conflict: the canonical
        # request's values / opt-out / exclude round-trip exactly.
        self.assertEqual(payload["productionPreviewOmission"]["targetLocations"], ["Canada"])
        self.assertEqual(payload["optOutVsPreviewOmission"]["targetLocations"], [])
        self.assertTrue(payload["optOutVsPreviewOmission"]["hasTargetKey"])
        self.assertEqual(payload["excludeVsPreviewOmission"]["excludeTargetLocations"], ["Europe"])

        # Presence-intact agreement round-trips values and opt-out exactly;
        # all-absent stays absent (legacy request, server default applies).
        self.assertEqual(payload["agreedValues"]["targetLocations"], ["Canada"])
        self.assertTrue(payload["agreedValues"]["hasTargetKey"])
        self.assertEqual(payload["agreedOptOut"]["targetLocations"], [])
        self.assertTrue(payload["agreedOptOut"]["hasTargetKey"])
        self.assertIsNone(payload["legacyAllAbsent"]["targetLocations"])

        # Rendered: the stale-preview recovery envelope fails closed — no
        # plan card, the recovery failure is surfaced visibly (the exact
        # contract conflict text is asserted at mapper level above; the
        # harness loads each module in its own vm realm, so the rendered
        # fallback message is the generic recovery-failure copy), and zero
        # approval/start calls.
        rendered = payload["rendered"]
        self.assertFalse(rendered["hasPlanCard"])
        self.assertIn("历史记录恢复失败", rendered["pageText"])
        self.assertGreaterEqual(rendered["historyCallCount"], 1)
        self.assertEqual(rendered["reviewCallCount"], 0)
        self.assertEqual(rendered["workflowCallCount"], 0)

    def test_absent_restore_revision_omits_location_keys(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            const findTextareaById = (node, id) => {
              if (node.nodeType === 1 && node.nodeName === "TEXTAREA" && node.getAttribute("id") === id) {
                return node;
              }
              for (const child of node.childNodes || []) {
                const found = findTextareaById(child, id);
                if (found) return found;
              }
              return null;
            };
            const findButtonByText = (node, text) => {
              if (node.nodeType === 1 && node.nodeName === "BUTTON" && node.textContent.trim() === text) {
                return node;
              }
              for (const child of node.childNodes || []) {
                const found = findButtonByText(child, text);
                if (found) return found;
              }
              return null;
            };
            const findButtonByAriaLabel = (node, label) => {
              if (node.nodeType === 1 && node.nodeName === "BUTTON" && node.getAttribute("aria-label") === label) {
                return node;
              }
              for (const child of node.childNodes || []) {
                const found = findButtonByAriaLabel(child, label);
                if (found) return found;
              }
              return null;
            };
            (async () => {
              const restoreCohort = selectionFor(["research"], ["current", "former"]);
              const restoreResponse = planResponseForRequest(
                {
                  raw_user_request: "find people",
                  cohort_selection: restoreCohort,
                  target_locations: ["Canada"],
                  exclude_target_locations: ["Europe"],
                },
                {
                  historyId: "hist-restore",
                  reviewId: "review-41",
                  editableFields: ["target_locations", "exclude_target_locations"],
                },
              );
              addRoute("GET", "/api/cohort-selection/options", () => optionsPayload);
              addRoute("GET", "/api/frontend-history/", () =>
                recoveryEnvelopeFor("hist-restore", restoreResponse));
              addRoute("POST", "/api/plan/submit", (body) =>
                planResponseForRequest(body, { historyId: "hist-restore", reviewId: "review-41" }));

              searchHistoryStore.set(
                "hist-restore",
                seedPlanHistoryItem(
                  "hist-restore",
                  makePlan({
                    cohortSelection: restoreCohort,
                    targetLocations: ["Canada"],
                    excludeTargetLocations: ["Europe"],
                    reviewGate: {
                      status: "pending",
                      requiredBeforeExecution: true,
                      riskLevel: "low",
                      reasons: [],
                      confirmationItems: [],
                      editableFields: ["target_locations", "exclude_target_locations"],
                      suggestedActions: [],
                      scopeHints: [],
                      executionModeHints: [],
                    },
                  }),
                  "review-41",
                ),
              );
              routeParams.history = "hist-restore";
              const { container } = await mountApp();

              // Authorized edit 1: remove the LAST target tag. Clearing back
              // to the absent (server-default) state is a first-class edit —
              // the old plan value must NOT reappear.
              clickEl(findButtonByAriaLabel(container, "移除 Canada"));
              await settle(2);
              const tagsAfterClear = findAllByTestId(
                container,
                "plan-review-target-locations-input-tag",
              ).map((node) => node.textContent.replace(/×/g, "").trim());
              const defaultMarkerAfterClear = Boolean(
                findByTestId(container, "plan-review-target-locations-default"),
              );

              // Authorized edit 2: remove the only exclude tag.
              clickEl(findButtonByAriaLabel(container, "移除 Europe"));
              await settle(2);

              // Authorized edit 3: enter explicit opt-out, then EXIT it back
              // to the absent state.
              const optOutCheckbox = findByTestId(container, "plan-review-target-locations-optout");
              setCheckbox(optOutCheckbox, true);
              await settle(2);
              const optedOutMarker = Boolean(
                findByTestId(container, "plan-review-target-locations-opted-out"),
              );
              setCheckbox(findByTestId(container, "plan-review-target-locations-optout"), false);
              await settle(2);
              const defaultMarkerAfterOptOutExit = Boolean(
                findByTestId(container, "plan-review-target-locations-default"),
              );
              const optedOutMarkerAfterExit = Boolean(
                findByTestId(container, "plan-review-target-locations-opted-out"),
              );

              // Revise through the REAL revision path: the restored absent
              // state must round-trip as ABSENT keys (server default), not
              // resurrect the plan's Canada/Europe.
              setInputValue(findTextareaById(container, "plan-revision"), "reset locations");
              await settle(2);
              clickEl(findButtonByText(container, "修改方案"));
              await settle();
              const submits = callsTo("/api/plan/submit");
              console.log(JSON.stringify({
                tagsAfterClear,
                defaultMarkerAfterClear,
                optedOutMarker,
                defaultMarkerAfterOptOutExit,
                optedOutMarkerAfterExit,
                submitCallCount: submits.length,
                revisionBody: submits.length ? submits[submits.length - 1].body : null,
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # Clearing the last tag restores the absent/server-default render —
        # the frozen plan value does NOT reappear.
        self.assertEqual(payload["tagsAfterClear"], [])
        self.assertTrue(payload["defaultMarkerAfterClear"])

        # Opt-out round-trips distinctly: [] renders the opted-out state,
        # unchecking restores the absent/server-default state.
        self.assertTrue(payload["optedOutMarker"])
        self.assertTrue(payload["defaultMarkerAfterOptOutExit"])
        self.assertFalse(payload["optedOutMarkerAfterExit"])

        # The revision payload carries the restored ABSENT state: both
        # location keys are omitted (server default), never resurrected from
        # the plan mirror.
        self.assertEqual(payload["submitCallCount"], 1)
        revision = payload["revisionBody"]
        self.assertEqual(revision["history_id"], "hist-restore")
        self.assertNotIn("target_locations", revision)
        self.assertNotIn("exclude_target_locations", revision)
        self.assertEqual(revision["cohort_selection"]["role_bucket_ids"], ["research"])

    def test_unpinned_plan_manifest_never_borrows_mirror_pin(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            (async () => {
              const cohort = selectionFor(["research"], ["current", "former"]);
              // Review finding 3 (r3): pin assignment is per-manifest exact.
              // A PRESENT but unpinned canonical plan manifest owns NO pin —
              // it must never borrow one from a stale/other mirror.
              const mapPin = (payload) =>
                captureError(() => api.__testMapPlanPayloadToDemoPlan(payload, "find people"));
              const unpinnedCanonicalPinnedMirror = mapPin({
                request: { cohort_selection: cohort },
                plan: { acquisition_strategy: { provider_execution_manifest: {} } },
                metadata: { provider_execution_manifest: manifestPin() },
              });
              const pinnedCanonicalUnpinnedMirror = mapPin({
                request: { cohort_selection: cohort },
                plan: { acquisition_strategy: { provider_execution_manifest: manifestPin() } },
                metadata: { provider_execution_manifest: {} },
              });
              const halfPinnedCanonical = mapPin({
                request: { cohort_selection: cohort },
                plan: {
                  acquisition_strategy: {
                    provider_execution_manifest: { registry_version: REGISTRY_VERSION },
                  },
                },
                metadata: { provider_execution_manifest: manifestPin() },
              });
              // Rerun3 finding 5: a MISSING canonical plan manifest fails
              // closed — the pinned metadata mirror is never promoted into
              // the canonical slot (before r4 this produced a borrowed,
              // valid-looking pin).
              const missingCanonicalPinnedMetadata = mapPin({
                request: { cohort_selection: cohort },
                plan: { acquisition_strategy: {} },
                metadata: { provider_execution_manifest: manifestPin() },
              });
              const nonObjectCanonicalPinnedMetadata = mapPin({
                request: { cohort_selection: cohort },
                plan: { acquisition_strategy: { provider_execution_manifest: "not-an-object" } },
                metadata: { provider_execution_manifest: manifestPin() },
              });
              // Padded/mis-shaped pin bytes are invalid pin evidence — the
              // dedicated byte-exact validators never trim them into a match.
              const paddedCanonicalPin = mapPin({
                request: { cohort_selection: cohort },
                plan: {
                  acquisition_strategy: {
                    provider_execution_manifest: manifestPin({
                      registry_version: ` ${REGISTRY_VERSION} `,
                    }),
                  },
                },
                metadata: { provider_execution_manifest: manifestPin() },
              });
              const shortDigestCanonicalPin = mapPin({
                request: { cohort_selection: cohort },
                plan: {
                  acquisition_strategy: {
                    provider_execution_manifest: manifestPin({
                      registry_digest: "abcd",
                    }),
                  },
                },
                metadata: { provider_execution_manifest: manifestPin() },
              });
              // The options parser validates pin bytes byte-exactly too:
              // padded or mis-shaped registry pins are rejected, never
              // repaired into a match against the plan pin.
              const paddedOptionsVersionError = captureError(() =>
                cohortSelection.parseCohortSelectionOptionsPayload({
                  ...optionsPayload,
                  registry_version: ` ${REGISTRY_VERSION} `,
                }),
              );
              const paddedOptionsDigestError = captureError(() =>
                cohortSelection.parseCohortSelectionOptionsPayload({
                  ...optionsPayload,
                  registry_digest: ` ${REGISTRY_DIGEST} `,
                }),
              );
              const shortOptionsDigestError = captureError(() =>
                cohortSelection.parseCohortSelectionOptionsPayload({
                  ...optionsPayload,
                  registry_digest: "abcd",
                }),
              );
              // Non-Cohort plans own no registry pin at all.
              const nonCohortPlan = api.__testMapPlanPayloadToDemoPlan(
                {
                  request: { raw_user_request: "find people" },
                  plan: { acquisition_strategy: {} },
                  metadata: {},
                },
                "find people",
              );
              // Every manifest unpinned -> no pin evidence anywhere -> the
              // plan maps with an UNDEFINED pin (confirmation blocks).
              const allUnpinned = api.__testMapPlanPayloadToDemoPlan(
                {
                  request: { cohort_selection: cohort },
                  plan: { acquisition_strategy: { provider_execution_manifest: {} } },
                  metadata: {},
                },
                "find people",
              );
              // Canonical pinned + byte-exact mirrors -> the pin is owned.
              const agreedPinned = api.__testMapPlanPayloadToDemoPlan(
                {
                  request: { cohort_selection: cohort },
                  plan: { acquisition_strategy: { provider_execution_manifest: manifestPin() } },
                  metadata: { provider_execution_manifest: manifestPin() },
                },
                "find people",
              );

              // Rendered integration: a recovery envelope with an unpinned
              // canonical manifest plus a PINNED metadata mirror must fail
              // the mapping — no valid-looking plan, zero approval/start.
              const borrowResponse = planResponseForRequest(
                { raw_user_request: "find people", cohort_selection: cohort },
                {
                  historyId: "hist-borrow",
                  reviewId: "review-42",
                  editableFields: ["target_locations"],
                  manifest: {},
                  metadata: { provider_execution_manifest: manifestPin() },
                },
              );
              addRoute("GET", "/api/cohort-selection/options", () => optionsPayload);
              addRoute("GET", "/api/frontend-history/", () =>
                recoveryEnvelopeFor("hist-borrow", borrowResponse));
              addRoute("POST", "/api/plan/review", () => ({ status: "approved" }));
              addRoute("POST", "/api/workflows", () => ({ job_id: "job-borrow", status: "running" }));
              routeParams.history = "hist-borrow";
              const { container } = await mountApp();
              await settle();
              console.log(JSON.stringify({
                unpinnedCanonicalPinnedMirror,
                pinnedCanonicalUnpinnedMirror,
                halfPinnedCanonical,
                missingCanonicalPinnedMetadata,
                nonObjectCanonicalPinnedMetadata,
                paddedCanonicalPin,
                shortDigestCanonicalPin,
                paddedOptionsVersionError,
                paddedOptionsDigestError,
                shortOptionsDigestError,
                nonCohortPin: nonCohortPlan.cohortRegistryPin ?? null,
                allUnpinnedPin: allUnpinned.cohortRegistryPin ?? null,
                agreedPinnedPin: agreedPinned.cohortRegistryPin ?? null,
                rendered: {
                  hasPlanCard: Boolean(findByTestId(container, "plan-card")),
                  pageText: container.textContent.slice(0, 500),
                  reviewCallCount: callsTo("/api/plan/review").length,
                  workflowCallCount: callsTo("/api/workflows").length,
                },
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # Mixed pin evidence fails closed in BOTH directions; half-pinned is
        # invalid pin evidence.
        self.assertIn("conflicting cohort registry pins", payload["unpinnedCanonicalPinnedMirror"])
        self.assertIn("conflicting cohort registry pins", payload["pinnedCanonicalUnpinnedMirror"])
        self.assertIn("invalid cohort registry pin", payload["halfPinnedCanonical"])

        # Rerun3 finding 5: a missing or non-object canonical plan manifest
        # fails closed — the pinned metadata mirror is never promoted into
        # the canonical slot.
        self.assertIn(
            "missing the canonical provider execution manifest",
            payload["missingCanonicalPinnedMetadata"],
        )
        self.assertIn(
            "invalid canonical provider execution manifest",
            payload["nonObjectCanonicalPinnedMetadata"],
        )

        # Padded/mis-shaped pin bytes are invalid pin evidence (byte-exact
        # validators, no trim repair) — in BOTH the manifest reader and the
        # options parser.
        self.assertIn("invalid cohort registry pin", payload["paddedCanonicalPin"])
        self.assertIn("invalid cohort registry pin", payload["shortDigestCanonicalPin"])
        self.assertIn("invalid registry_version", payload["paddedOptionsVersionError"])
        self.assertIn("invalid registry_digest", payload["paddedOptionsDigestError"])
        self.assertIn("invalid registry_digest", payload["shortOptionsDigestError"])

        # Non-Cohort plans own no pin; all-unpinned stays unconfirmable (no
        # pin), agreed mirrors pin.
        self.assertIsNone(payload["nonCohortPin"])
        self.assertIsNone(payload["allUnpinnedPin"])
        self.assertEqual(
            payload["agreedPinnedPin"],
            {
                "registryVersion": "cohort_selection.registry.v1",
                "registryDigest": "9f2c1ab4d5e6478091a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708",
            },
        )

        # Rendered: the stale pinned mirror cannot manufacture a confirmable
        # plan — mapping fails, no plan card, the recovery failure surfaces
        # visibly, and zero approval/start calls.
        rendered = payload["rendered"]
        self.assertFalse(rendered["hasPlanCard"])
        self.assertIn("历史记录恢复失败", rendered["pageText"])
        self.assertEqual(rendered["reviewCallCount"], 0)
        self.assertEqual(rendered["workflowCallCount"], 0)

    def test_revision_first_blocks_overlapping_confirmation(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            const findButtonByText = (node, text) => {
              if (node.nodeType === 1 && node.nodeName === "BUTTON" && node.textContent.trim() === text) {
                return node;
              }
              for (const child of node.childNodes || []) {
                const found = findButtonByText(child, text);
                if (found) return found;
              }
              return null;
            };
            (async () => {
              const overlapCohort = selectionFor(["research"], ["current", "former"]);
              const initialResponse = planResponseForRequest(
                {
                  raw_user_request: "find people",
                  cohort_selection: overlapCohort,
                  target_locations: ["Canada"],
                },
                { historyId: "hist-ov1", reviewId: "701", editableFields: ["target_locations"] },
              );
              let resolveRevision;
              const revisionGate = new Promise((resolve) => {
                resolveRevision = resolve;
              });
              addRoute("GET", "/api/cohort-selection/options", () => optionsPayload);
              addRoute("GET", "/api/frontend-history/", () =>
                recoveryEnvelopeFor("hist-ov1", initialResponse));
              addRoute("POST", "/api/plan/submit", async (body) => {
                await revisionGate;
                return planResponseForRequest(body, {
                  historyId: "hist-ov1",
                  reviewId: "702",
                  editableFields: ["target_locations"],
                });
              });
              addRoute("POST", "/api/plan/review", () => ({ status: "approved" }));
              addRoute("POST", "/api/workflows", () => ({
                job_id: "job-ov1",
                status: "completed",
                stage: "completed",
              }));
              addRoute("GET", "/api/jobs/", () => ({
                candidates: [],
                layers: [],
                intentKeywords: [],
                totalCandidates: 0,
                manualReviewCount: 0,
              }));

              searchHistoryStore.set(
                "hist-ov1",
                seedPlanHistoryItem(
                  "hist-ov1",
                  makePlan({
                    cohortSelection: overlapCohort,
                    targetLocations: ["Canada"],
                    reviewGate: {
                      status: "pending",
                      requiredBeforeExecution: true,
                      riskLevel: "low",
                      reasons: [],
                      confirmationItems: [],
                      editableFields: ["target_locations"],
                      suggestedActions: [],
                      scopeHints: [],
                      executionModeHints: [],
                    },
                  }),
                  "701",
                ),
              );
              routeParams.history = "hist-ov1";
              const { container } = await mountApp();

              // Revision starts first (deferred backend response).
              clickEl(findButtonByText(container, "修改方案"));
              await settle(4);
              const confirmWhileRevising = findByTestId(container, "plan-confirm-button");
              const confirmDisabledWhileRevising = Boolean(
                confirmWhileRevising && confirmWhileRevising.hasAttribute("disabled"),
              );
              const revisingButton = findButtonByText(container, "更新中...");
              const reviseDisabledWhileRevising = Boolean(
                revisingButton && revisingButton.hasAttribute("disabled"),
              );
              // A synthetic click on the (cross-disabled) confirm button
              // must NOT start approval against the OLD identity.
              clickEl(confirmWhileRevising);
              await settle(4);
              const reviewCallsWhileRevising = callsTo("/api/plan/review").length;
              const workflowCallsWhileRevising = callsTo("/api/workflows").length;

              // The revision lands a NEW plan identity; confirmation of the
              // FRESH identity is then re-authorized.
              resolveRevision();
              await settle();
              clickEl(findByTestId(container, "plan-confirm-button"));
              await settle();
              const reviewCalls = callsTo("/api/plan/review");
              console.log(JSON.stringify({
                submitCallCount: callsTo("/api/plan/submit").length,
                confirmDisabledWhileRevising,
                reviseDisabledWhileRevising,
                reviewCallsWhileRevising,
                workflowCallsWhileRevising,
                reviewCallCount: reviewCalls.length,
                reviewBody: reviewCalls.length ? reviewCalls[0].body : null,
                workflowCallCount: callsTo("/api/workflows").length,
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # Exactly ONE plan action while the revision was in flight: the
        # overlapping confirmation was blocked (cross-disabled + mutex) and
        # made NO approval/start call against the stale identity.
        self.assertEqual(payload["submitCallCount"], 1)
        self.assertTrue(payload["confirmDisabledWhileRevising"])
        self.assertTrue(payload["reviseDisabledWhileRevising"])
        self.assertEqual(payload["reviewCallsWhileRevising"], 0)
        self.assertEqual(payload["workflowCallsWhileRevising"], 0)

        # After the revision landed, confirmation of the FRESH identity is
        # re-authorized: exactly one approval carrying the NEW review id and
        # exactly one workflow start.
        self.assertEqual(payload["reviewCallCount"], 1)
        self.assertEqual(payload["reviewBody"]["review_id"], 702)
        self.assertEqual(payload["workflowCallCount"], 1)

    def test_confirmation_first_blocks_overlapping_revision(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            const findButtonByText = (node, text) => {
              if (node.nodeType === 1 && node.nodeName === "BUTTON" && node.textContent.trim() === text) {
                return node;
              }
              for (const child of node.childNodes || []) {
                const found = findButtonByText(child, text);
                if (found) return found;
              }
              return null;
            };
            (async () => {
              const overlapCohort = selectionFor(["research"], ["current", "former"]);
              const initialResponse = planResponseForRequest(
                {
                  raw_user_request: "find people",
                  cohort_selection: overlapCohort,
                  target_locations: ["Canada"],
                },
                { historyId: "hist-ov2", reviewId: "703", editableFields: ["target_locations"] },
              );
              let resolveApprove;
              const approveGate = new Promise((resolve) => {
                resolveApprove = resolve;
              });
              addRoute("GET", "/api/cohort-selection/options", () => optionsPayload);
              addRoute("GET", "/api/frontend-history/", () =>
                recoveryEnvelopeFor("hist-ov2", initialResponse));
              addRoute("POST", "/api/plan/submit", (body) =>
                planResponseForRequest(body, { historyId: "hist-ov2", reviewId: "704" }));
              addRoute("POST", "/api/plan/review", async () => {
                await approveGate;
                return { status: "approved" };
              });
              addRoute("POST", "/api/workflows", () => ({
                job_id: "job-ov2",
                status: "completed",
                stage: "completed",
              }));
              addRoute("GET", "/api/jobs/", () => ({
                candidates: [],
                layers: [],
                intentKeywords: [],
                totalCandidates: 0,
                manualReviewCount: 0,
              }));

              searchHistoryStore.set(
                "hist-ov2",
                seedPlanHistoryItem(
                  "hist-ov2",
                  makePlan({
                    cohortSelection: overlapCohort,
                    targetLocations: ["Canada"],
                    reviewGate: {
                      status: "pending",
                      requiredBeforeExecution: true,
                      riskLevel: "low",
                      reasons: [],
                      confirmationItems: [],
                      editableFields: ["target_locations"],
                      suggestedActions: [],
                      scopeHints: [],
                      executionModeHints: [],
                    },
                  }),
                  "703",
                ),
              );
              routeParams.history = "hist-ov2";
              const { container } = await mountApp();

              // Confirmation starts first (approval deferred).
              clickEl(findByTestId(container, "plan-confirm-button"));
              await settle(4);
              const reviseWhileConfirming = findButtonByText(container, "修改方案");
              const reviseDisabledWhileConfirming = Boolean(
                reviseWhileConfirming && reviseWhileConfirming.hasAttribute("disabled"),
              );
              // A synthetic click on the (cross-disabled) revision button
              // must NOT persist a NEW plan while approval/start continues
              // for the captured one.
              clickEl(reviseWhileConfirming);
              await settle(4);
              const submitCallsWhileConfirming = callsTo("/api/plan/submit").length;

              resolveApprove();
              await settle();
              console.log(JSON.stringify({
                reviseDisabledWhileConfirming,
                submitCallsWhileConfirming,
                submitCallCount: callsTo("/api/plan/submit").length,
                reviewCallCount: callsTo("/api/plan/review").length,
                workflowCallCount: callsTo("/api/workflows").length,
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # Exactly ONE plan action under this interleaving: the overlapping
        # revision was blocked (cross-disabled + mutex) and never reached the
        # backend; the in-flight confirmation approved and started the SAME
        # captured identity exactly once.
        self.assertTrue(payload["reviseDisabledWhileConfirming"])
        self.assertEqual(payload["submitCallsWhileConfirming"], 0)
        self.assertEqual(payload["submitCallCount"], 0)
        self.assertEqual(payload["reviewCallCount"], 1)
        self.assertEqual(payload["workflowCallCount"], 1)

    def test_function_facet_disabled_without_canonical_summary(self) -> None:
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            const resultsBoard = loadTs("frontend-demo/src/components/ResultsBoardPanel.tsx");
            const boardProps = (dashboard) => ({
              dashboard,
              projectionId: "proj-1",
              historyId: "",
              jobId: "job-x",
              initialCandidateId: "",
              isHydratingCandidates: false,
              candidateHydrationError: "",
              reviewStatusMap: {},
              onSelectedCandidateChange: () => {},
              onOpenManualReview: () => {},
              onReviewStateChanged: () => {},
            });
            const boardCandidate = (patch) => ({
              ...baseCandidate,
              evidence: [],
              confidence: "high",
              avatarUrl: "",
              ...patch,
            });
            const candidates = [
              boardCandidate({
                id: "row-1",
                employmentStatus: "current",
                functionBucketIds: ["engineering"],
                functionBucketSource: "lane_membership",
              }),
              boardCandidate({
                id: "row-2",
                employmentStatus: "former",
                functionBucketIds: ["research"],
                functionBucketSource: "registry_evidence",
              }),
            ];
            // Legacy / no-canonical-summary board: candidate rows DO carry
            // server bucket ids, but there is NO canonical facet summary.
            const legacyDashboard = {
              candidates,
              layers: [],
              intentKeywords: [],
              totalCandidates: 2,
              manualReviewCount: 0,
              projectionId: "proj-1",
            };
            const legacyMarkup = render(el(resultsBoard.ResultsBoardPanel, boardProps(legacyDashboard)));
            // Canonical backend summary: the function options come from the
            // summary (labels included), not from the rows.
            const canonicalDashboard = {
              ...legacyDashboard,
              candidateFacetSummary: {
                candidateCount: 2,
                layers: [],
                recall: [],
                employment: [],
                locations: [],
                functions: [
                  { id: "research", label: "Researcher", count: 1 },
                  { id: "engineering", label: "Engineer", count: 1 },
                ],
              },
              candidateFacetSummaryScope: "global_full_population",
            };
            const canonicalMarkup = render(
              el(resultsBoard.ResultsBoardPanel, boardProps(canonicalDashboard)),
            );
            // Rerun4 finding 2 (component mirror agreement): the canonical
            // summary EXISTS and the top-level scope is valid, but the
            // board-runtime mirror is MISSING — one remaining mirror must not
            // become authoritative, so the facet stays disabled.
            const missingMirrorDashboard = {
              ...canonicalDashboard,
              boardRuntimeState: {
                expectedCandidateCount: 2,
                rowPublicationRevision: "rev-1",
                rowPublicationTier: "serving_projection_members",
                facetSummaryStatus: "complete",
                facetSummaryScope: "",
                facetSummaryCandidateCount: 2,
                filterContract: {
                  source: "serving_projection_reader",
                  facetCountScope: "exact_projection",
                  rowFilterScope: "projection_membership",
                  backendFilteredPagingSupported: true,
                },
              },
            };
            const missingMirrorMarkup = render(
              el(resultsBoard.ResultsBoardPanel, boardProps(missingMirrorDashboard)),
            );
            // ...and a PADDED board-runtime mirror is invalid evidence, not
            // a trim-repaired match.
            const paddedMirrorDashboard = {
              ...missingMirrorDashboard,
              boardRuntimeState: {
                ...missingMirrorDashboard.boardRuntimeState,
                facetSummaryScope: " global_full_population ",
              },
            };
            const paddedMirrorMarkup = render(
              el(resultsBoard.ResultsBoardPanel, boardProps(paddedMirrorDashboard)),
            );
            const functionSection = (markup) => {
              // The exact function-facet label (the canonicalFacetUnavailable
              // notice also mentions 职能筛选, so a bare 职能 match is wrong).
              const start = markup.indexOf("职能</span>");
              if (start < 0) return "";
              const end = markup.indexOf("</details>", start);
              return end >= 0 ? markup.slice(start, end) : markup.slice(start, start + 400);
            };
            console.log(JSON.stringify({
              legacyFunctionSection: functionSection(legacyMarkup),
              canonicalFunctionSection: functionSection(canonicalMarkup),
              missingMirrorFunctionSection: functionSection(missingMirrorMarkup),
              paddedMirrorFunctionSection: functionSection(paddedMirrorMarkup),
            }));
            """
        )
        payload = _run_node(script)

        legacy_section = payload["legacyFunctionSection"]
        # Facet disabled: no rebuilt options, an explicit unavailable message.
        self.assertIn("统计未生成", legacy_section)
        self.assertIn("facet-empty-message", legacy_section)
        self.assertNotIn('class="facet-option"', legacy_section)

        canonical_section = payload["canonicalFunctionSection"]
        # Canonical summary: options render from the backend summary only.
        self.assertIn("Researcher", canonical_section)
        self.assertIn("Engineer", canonical_section)
        self.assertNotIn("facet-empty-message", canonical_section)

        # Rerun4 finding 2: a missing or padded board-runtime mirror disables
        # the facet — one remaining/padded mirror never becomes authoritative.
        for key in ("missingMirrorFunctionSection", "paddedMirrorFunctionSection"):
            section = payload[key]
            self.assertIn("facet-empty-message", section, key)
            self.assertNotIn('class="facet-option"', section, key)
            self.assertNotIn("Researcher", section, key)

    def test_facet_scope_contracts_consumed_independently(self) -> None:
        """Rerun3 finding 2 + rerun4 finding 2: summary scope and
        filter-contract scope are independent backend-owned contracts —
        consumed from their own owners through ONE strict byte-exact adapter
        (closed values, no trimming, agreement across all documented
        mirrors), never minted (`exact_projection` default) or copied into
        each other, and disabled on missing, padded, or conflicting
        evidence."""
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            (async () => {
            // Production-shaped projection dashboard payload (the pinned
            // serving_projection_reader shape): membership revision + exact
            // read contract + consistent counts.
            const projectionPayload = (mutate) => {
              const payload = {
                projection: {
                  projection_id: "proj-1",
                  source_run_id: "run-1",
                  scope_label: "ACME members",
                  collection_id: "company:acme",
                  membership_revision: "rev-1",
                  visible_member_count: 2,
                  read_contract: {
                    source: "serving_projection_members",
                    fallback_used: false,
                    fail_closed: true,
                  },
                  counts: {
                    count_scope: "exact_projection",
                    result_count: 2,
                    candidate_count: 2,
                    visible_member_count: 2,
                    facet_count_scope: "exact_projection",
                  },
                  readiness: {},
                  provenance: { snapshot_id: "snap-1" },
                  updated_at: "2026-07-19T00:00:00Z",
                },
                total_candidates: 2,
                facet_summary: {
                  status: "complete",
                  count_scope: "exact_projection",
                  candidate_count: 2,
                  layers: [{ id: "layer_0", label: "Layer 0", count: 2 }],
                  functions: [{ id: "research", label: "Researcher", count: 2 }],
                },
                facet_summary_scope: "exact_projection",
              };
              if (mutate) mutate(payload);
              return payload;
            };
            const scopeTriple = (payload) => {
              const dashboard = api.__testProjectionPayloadToDashboard(payload);
              return {
                topLevelSummaryScope: dashboard.candidateFacetSummaryScope,
                boardSummaryScope: dashboard.boardRuntimeState.facetSummaryScope,
                filterFacetCountScope: dashboard.boardRuntimeState.filterContract.facetCountScope,
              };
            };
            // Independent owners: summary scope global_full_population while
            // the filter contract stays exact_projection — each field keeps
            // its OWN backend value (no copy between contracts).
            const agreeingScopes = scopeTriple(
              projectionPayload((payload) => {
                payload.facet_summary_scope = "global_full_population";
                payload.facet_summary.count_scope = "global_full_population";
              }),
            );
            // Missing summary-scope evidence: NOT minted into
            // exact_projection; the filter contract keeps its own owner.
            const missingSummaryScope = scopeTriple(
              projectionPayload((payload) => {
                delete payload.facet_summary_scope;
                delete payload.facet_summary.count_scope;
              }),
            );
            // Conflicting summary-scope mirrors: disabled, not promoted.
            const conflictingSummaryScope = scopeTriple(
              projectionPayload((payload) => {
                payload.facet_summary.count_scope = "global_full_population";
              }),
            );
            // Missing filter-contract scope: NOT copied from the summary
            // scope; the summary scope keeps its own owner.
            const missingFilterScope = scopeTriple(
              projectionPayload((payload) => {
                delete payload.projection.counts.facet_count_scope;
              }),
            );
            // Rerun4 finding 2: padded identity bytes are INVALID evidence,
            // never trimmed into a match — on either mirror.
            const paddedInnerScope = scopeTriple(
              projectionPayload((payload) => {
                payload.facet_summary.count_scope = " exact_projection ";
              }),
            );
            const paddedTopLevelScope = scopeTriple(
              projectionPayload((payload) => {
                payload.facet_summary_scope = " exact_projection ";
              }),
            );
            // The JOB candidate-page endpoint goes through the SAME strict
            // adapter: a top-level exact_projection with a conflicting inner
            // count_scope is NOT admitted (the top level no longer wins).
            addRoute("GET", "/api/jobs/job-scope/candidates", () => ({
              job_id: "job-scope",
              result_mode: "asset_population",
              offset: 0,
              limit: 24,
              returned_count: 0,
              total_candidates: 2,
              filtered_candidate_count: 2,
              has_more: false,
              candidates: [],
              board_runtime_state: {
                expected_candidate_count: 2,
                row_publication_revision: "rev-1",
                row_publication_tier: "serving_projection_members",
                facet_summary_status: "complete",
                facet_summary_scope: "exact_projection",
                facet_summary_candidate_count: 2,
                filter_contract: {
                  source: "serving_projection_reader",
                  facet_count_scope: "exact_projection",
                  row_filter_scope: "projection_membership",
                  backend_filtered_paging_supported: true,
                },
              },
              facet_summary: {
                status: "complete",
                count_scope: "global_full_population",
                candidate_count: 2,
              },
              facet_summary_scope: "exact_projection",
              filter_contract: {
                source: "serving_projection_reader",
                facet_count_scope: "exact_projection",
                row_filter_scope: "projection_membership",
                backend_filtered_paging_supported: true,
              },
            }));
            const jobPageConflicting = await api
              .getDashboardCandidatePage("job-scope", { forceRefresh: true })
              .then((page) => page.candidateFacetSummaryScope);
            console.log(JSON.stringify({
              agreeingScopes,
              missingSummaryScope,
              conflictingSummaryScope,
              missingFilterScope,
              paddedInnerScope,
              paddedTopLevelScope,
              jobPageConflicting,
            }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # Independent contracts: each field keeps its own backend-owned value.
        agreeing = payload["agreeingScopes"]
        self.assertEqual(agreeing["topLevelSummaryScope"], "global_full_population")
        self.assertEqual(agreeing["boardSummaryScope"], "global_full_population")
        self.assertEqual(agreeing["filterFacetCountScope"], "exact_projection")

        # Missing summary-scope evidence is NOT minted into exact_projection;
        # the summary scope maps unavailable while the independent filter
        # contract keeps its own backend-owned value.
        missing_summary = payload["missingSummaryScope"]
        self.assertEqual(missing_summary["topLevelSummaryScope"], "")
        self.assertEqual(missing_summary["boardSummaryScope"], "unavailable")
        self.assertEqual(missing_summary["filterFacetCountScope"], "exact_projection")

        # Conflicting summary-scope mirrors disable the scope (not promoted).
        conflict = payload["conflictingSummaryScope"]
        self.assertEqual(conflict["topLevelSummaryScope"], "")
        self.assertEqual(conflict["boardSummaryScope"], "unavailable")

        # Missing filter-contract scope is NOT copied from the summary scope.
        missing_filter = payload["missingFilterScope"]
        self.assertEqual(missing_filter["filterFacetCountScope"], "unavailable")
        self.assertEqual(missing_filter["boardSummaryScope"], "exact_projection")

        # Rerun4 finding 2: padded identity bytes are invalid evidence on
        # EITHER mirror — never trimmed into a canonical match.
        for key in ("paddedInnerScope", "paddedTopLevelScope"):
            self.assertEqual(payload[key]["topLevelSummaryScope"], "", key)
            self.assertEqual(payload[key]["boardSummaryScope"], "unavailable", key)

        # The job candidate-page endpoint uses the same strict adapter: a
        # conflicting inner count_scope disables the scope (the top-level
        # exact_projection no longer wins).
        self.assertEqual(payload["jobPageConflicting"], "")

    def test_transient_summary_gap_preserves_function_filter(self) -> None:
        """Rerun3 findings 4/8: a transient canonical-summary gap after a user
        function-filter edit must NOT wipe the selection, must NOT submit a
        widened backend filter, and must restore the narrowed filter when the
        summary returns — exercising the MOUNTED board through the real
        state/effect path (no static rendering)."""
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            (async () => {
              const resultsBoard = loadTs("frontend-demo/src/components/ResultsBoardPanel.tsx");
              const REV = "rev-1";
              const functionSummary = {
                candidateCount: 2,
                layers: [],
                recall: [],
                employment: [],
                locations: [],
                functions: [
                  { id: "research", label: "Researcher", count: 1 },
                  { id: "engineering", label: "Engineer", count: 1 },
                ],
              };
              const boardRuntimeFor = (facet) => ({
                expectedCandidateCount: 2,
                rowPublicationRevision: REV,
                // Production tier: without it the projection-revision
                // binding is vacuous and the kept-page branch is never
                // genuinely exercised (rerun4 review finding 4).
                rowPublicationTier: "serving_projection_members",
                facetSummaryStatus: facet ? "complete" : "unavailable",
                facetSummaryScope: facet ? "global_full_population" : "unavailable",
                facetSummaryCandidateCount: facet ? 2 : 0,
                filterContract: {
                  source: "serving_projection_reader",
                  facetCountScope: "exact_projection",
                  rowFilterScope: "projection_membership",
                  backendFilteredPagingSupported: true,
                },
              });
              const boardCandidate = (patch) => ({
                ...baseCandidate,
                evidence: [],
                confidence: "high",
                avatarUrl: "",
                ...patch,
              });
              const candidates = [
                boardCandidate({
                  id: "row-1",
                  employmentStatus: "current",
                  functionBucketIds: ["engineering"],
                  functionBucketSource: "lane_membership",
                }),
                boardCandidate({
                  id: "row-2",
                  employmentStatus: "former",
                  functionBucketIds: ["research"],
                  functionBucketSource: "registry_evidence",
                }),
              ];
              const dashboardFor = (withSummary) => ({
                candidates,
                layers: [],
                intentKeywords: [],
                totalCandidates: 2,
                manualReviewCount: 0,
                projectionId: "proj-1",
                candidateFacetSummary: withSummary ? functionSummary : undefined,
                candidateFacetSummaryScope: withSummary ? "global_full_population" : "",
                boardRuntimeState: boardRuntimeFor(withSummary),
              });
              const boardProps = (dashboard) => ({
                dashboard,
                projectionId: "proj-1",
                historyId: "",
                jobId: "",
                initialCandidateId: "",
                isHydratingCandidates: false,
                candidateHydrationError: "",
                reviewStatusMap: {},
                onSelectedCandidateChange: () => {},
                onOpenManualReview: () => {},
                onReviewStateChanged: () => {},
              });
              // Production-shaped projection candidate page (the pinned
              // serving reader shape; empty rows — the assertions target the
              // request filter, the notice, and the preserved selection).
              const pagePayload = {
                status: "ready",
                projection: {
                  projection_id: "proj-1",
                  source_run_id: "run-1",
                  membership_revision: REV,
                  visible_member_count: 2,
                  read_contract: {
                    source: "serving_projection_members",
                    fallback_used: false,
                    fail_closed: true,
                  },
                  counts: {
                    count_scope: "exact_projection",
                    result_count: 2,
                    candidate_count: 2,
                    visible_member_count: 2,
                    facet_count_scope: "exact_projection",
                  },
                  readiness: {},
                },
                total_candidates: 2,
                filtered_candidate_count: 1,
                offset: 0,
                limit: 24,
                has_more: false,
                next_offset: null,
                candidates: [],
                facet_summary: {
                  status: "complete",
                  count_scope: "exact_projection",
                  candidate_count: 2,
                },
                filter_contract: {
                  source: "serving_projection_reader",
                  facet_count_scope: "exact_projection",
                  row_filter_scope: "projection_membership",
                  backend_filtered_paging_supported: true,
                },
              };
              addRoute("GET", "/api/projections/proj-1/candidates", () => pagePayload);

              const container = miniWindow.document.createElement("div");
              miniWindow.document.body.appendChild(container);
              const root = ReactDOMClient.createRoot(container);
              const renderBoard = async (dashboard) => {
                await act(async () => {
                  root.render(el(resultsBoard.ResultsBoardPanel, boardProps(dashboard)));
                });
                await settle();
              };
              const pageRequestUrls = () =>
                callsTo("/api/projections/proj-1/candidates").map((call) => call.url || call.path);
              const findFacetOptionInput = (labelText) => {
                let found = null;
                const visit = (node) => {
                  if (found || !node) return;
                  if (
                    node.nodeType === 1 &&
                    node.nodeName === "LABEL" &&
                    String(node.getAttribute("class") || "").includes("facet-option") &&
                    node.textContent.includes(labelText)
                  ) {
                    found = (node.childNodes || []).find((child) => child.nodeName === "INPUT") || null;
                    return;
                  }
                  for (const child of node.childNodes || []) visit(child);
                };
                visit(container);
                return found;
              };

              // Phase 1: canonical summary available — defaults settle, the
              // initial (unfiltered) page request fires.
              await renderBoard(dashboardFor(true));
              const initialRequestCount = pageRequestUrls().length;

              // Phase 2: the user narrows the function facet to research only
              // (toggle engineering OFF the all-selected default).
              const engineeringInput = findFacetOptionInput("Engineer");
              setCheckbox(engineeringInput, false);
              await settle();
              const narrowedUrls = pageRequestUrls();

              // Phase 3: TRANSIENT GAP — same membership revision, summary
              // momentarily unavailable (polling/cross-endpoint race).
              await renderBoard(dashboardFor(false));
              await settle();
              const gapUrls = pageRequestUrls();
              const gapNotice = Boolean(findByTestId(container, "facet-gap-preserved-notice"));

              // Phase 4: the summary returns (same revision) — the preserved
              // selection reconciles against the restored canonical options
              // and the narrowed filter is resubmitted.
              await renderBoard(dashboardFor(true));
              await settle();
              const restoredUrls = pageRequestUrls();
              const restoredNotice = Boolean(findByTestId(container, "facet-gap-preserved-notice"));

              console.log(JSON.stringify({
                initialRequestCount,
                narrowedUrls,
                gapRequestCount: gapUrls.length,
                gapNotice,
                restoredUrls,
                restoredNotice,
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # Phase 2: the user edit submits the NARROWED backend filter.
        narrowed = payload["narrowedUrls"]
        self.assertGreater(len(narrowed), payload["initialRequestCount"])
        self.assertTrue(any("function_buckets=research" in url for url in narrowed), narrowed)

        # Phase 3: the transient gap fires NO new backend page request at
        # all — in particular never a widened one without function_buckets —
        # and the blocking preserved-intent notice is visible.
        self.assertEqual(payload["gapRequestCount"], len(narrowed))
        self.assertTrue(payload["gapNotice"])

        # Phase 4: the summary return resubmits the PRESERVED narrowed
        # filter (selection restored, never wiped), and the notice clears.
        restored = payload["restoredUrls"]
        self.assertEqual(len(restored), len(narrowed) + 1)
        self.assertIn("function_buckets=research", restored[-1])
        self.assertFalse(payload["restoredNotice"])

    def test_summary_gap_preserves_audit_and_layer_narrowing(self) -> None:
        """Rerun4 finding 3: audit-only and layer-only narrowing are
        preserved through a canonical-summary gap by the GENERIC
        last-resolved/applied filter signature — no widened request, the
        blocking notice appears, and the restored summary resubmits the
        preserved axis."""
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            (async () => {
              const resultsBoard = loadTs("frontend-demo/src/components/ResultsBoardPanel.tsx");
              const REV = "rev-1";
              const summary = {
                candidateCount: 2,
                layers: [
                  { id: "layer_0", label: "Layer 0", count: 2 },
                  { id: "layer_1", label: "Layer 1", count: 1 },
                ],
                recall: [],
                employment: [],
                locations: [],
                functions: [{ id: "research", label: "Researcher", count: 2 }],
              };
              const boardRuntimeFor = (facet) => ({
                expectedCandidateCount: 2,
                rowPublicationRevision: REV,
                rowPublicationTier: "serving_projection_members",
                facetSummaryStatus: facet ? "complete" : "unavailable",
                facetSummaryScope: facet ? "global_full_population" : "unavailable",
                facetSummaryCandidateCount: facet ? 2 : 0,
                filterContract: {
                  source: "serving_projection_reader",
                  facetCountScope: "exact_projection",
                  rowFilterScope: "projection_membership",
                  backendFilteredPagingSupported: true,
                },
              });
              const boardCandidate = (patch) => ({
                ...baseCandidate,
                evidence: [],
                confidence: "high",
                avatarUrl: "",
                ...patch,
              });
              const candidates = [
                boardCandidate({ id: "row-1", employmentStatus: "current" }),
                boardCandidate({ id: "row-2", employmentStatus: "former" }),
              ];
              const dashboardFor = (withSummary) => ({
                candidates,
                layers: [],
                intentKeywords: [],
                totalCandidates: 2,
                manualReviewCount: 0,
                projectionId: "proj-1",
                candidateFacetSummary: withSummary ? summary : undefined,
                candidateFacetSummaryScope: withSummary ? "global_full_population" : "",
                boardRuntimeState: boardRuntimeFor(withSummary),
              });
              const boardProps = (dashboard) => ({
                dashboard,
                projectionId: "proj-1",
                historyId: "",
                jobId: "",
                initialCandidateId: "",
                isHydratingCandidates: false,
                candidateHydrationError: "",
                reviewStatusMap: {},
                onSelectedCandidateChange: () => {},
                onOpenManualReview: () => {},
                onReviewStateChanged: () => {},
              });
              const pagePayload = {
                status: "ready",
                projection: {
                  projection_id: "proj-1",
                  source_run_id: "run-1",
                  membership_revision: REV,
                  visible_member_count: 2,
                  read_contract: {
                    source: "serving_projection_members",
                    fallback_used: false,
                    fail_closed: true,
                  },
                  counts: {
                    count_scope: "exact_projection",
                    result_count: 2,
                    candidate_count: 2,
                    visible_member_count: 2,
                    facet_count_scope: "exact_projection",
                  },
                  readiness: {},
                },
                total_candidates: 2,
                filtered_candidate_count: 1,
                offset: 0,
                limit: 24,
                has_more: false,
                next_offset: null,
                candidates: [],
                facet_summary: {
                  status: "complete",
                  count_scope: "exact_projection",
                  candidate_count: 2,
                },
                filter_contract: {
                  source: "serving_projection_reader",
                  facet_count_scope: "exact_projection",
                  row_filter_scope: "projection_membership",
                  backend_filtered_paging_supported: true,
                },
              };
              addRoute("GET", "/api/projections/proj-1/candidates", () => pagePayload);

              const container = miniWindow.document.createElement("div");
              miniWindow.document.body.appendChild(container);
              const root = ReactDOMClient.createRoot(container);
              const renderBoard = async (dashboard) => {
                await act(async () => {
                  root.render(el(resultsBoard.ResultsBoardPanel, boardProps(dashboard)));
                });
                await settle();
              };
              const pageRequestUrls = () =>
                callsTo("/api/projections/proj-1/candidates").map((call) => call.url || call.path);
              const findFacetOptionInput = (labelText) => {
                let found = null;
                const visit = (node) => {
                  if (found || !node) return;
                  if (
                    node.nodeType === 1 &&
                    node.nodeName === "LABEL" &&
                    String(node.getAttribute("class") || "").includes("facet-option") &&
                    node.textContent.includes(labelText)
                  ) {
                    found = (node.childNodes || []).find((child) => child.nodeName === "INPUT") || null;
                    return;
                  }
                  for (const child of node.childNodes || []) visit(child);
                };
                visit(container);
                return found;
              };
              const findLayerButton = (labelText) => {
                let found = null;
                const visit = (node) => {
                  if (found || !node) return;
                  if (
                    node.nodeType === 1 &&
                    node.nodeName === "BUTTON" &&
                    String(node.getAttribute("class") || "").includes("layer-tristate-option") &&
                    node.textContent.includes(labelText)
                  ) {
                    found = node;
                    return;
                  }
                  for (const child of node.childNodes || []) visit(child);
                };
                visit(container);
                return found;
              };

              // --- Audit-only narrowing (rerun4 probe axis) ---
              await renderBoard(dashboardFor(true));
              const auditInput = findFacetOptionInput("已核实候选人");
              setCheckbox(auditInput, false);
              await settle();
              const auditNarrowedUrls = pageRequestUrls();
              await renderBoard(dashboardFor(false));
              await settle();
              const auditGapCount = pageRequestUrls().length;
              const auditGapNotice = Boolean(findByTestId(container, "facet-gap-preserved-notice"));
              await renderBoard(dashboardFor(true));
              await settle();
              const auditRestoredUrls = pageRequestUrls();
              const auditRestoredNotice = Boolean(findByTestId(container, "facet-gap-preserved-notice"));

              // Back to the all-selected (inactive) audit state, then
              // --- Layer-only narrowing (no user-edit flag covers layers;
              // the generic filter signature must catch it) ---
              setCheckbox(findFacetOptionInput("已核实候选人"), true);
              await settle();
              clickEl(findLayerButton("Layer 1"));
              await settle();
              const layerNarrowedUrls = pageRequestUrls();
              await renderBoard(dashboardFor(false));
              await settle();
              const layerGapCount = pageRequestUrls().length;
              const layerGapNotice = Boolean(findByTestId(container, "facet-gap-preserved-notice"));
              await renderBoard(dashboardFor(true));
              await settle();
              const layerRestoredUrls = pageRequestUrls();
              const layerRestoredNotice = Boolean(findByTestId(container, "facet-gap-preserved-notice"));

              console.log(JSON.stringify({
                auditNarrowedUrls,
                auditGapCount,
                auditGapNotice,
                auditRestoredUrls,
                auditRestoredNotice,
                layerNarrowedUrls,
                layerGapCount,
                layerGapNotice,
                layerRestoredUrls,
                layerRestoredNotice,
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # Audit-only: narrowed request carries audit_statuses (minus the
        # unchecked option); the gap fires nothing and shows the notice; the
        # restore resubmits the preserved audit axis.
        audit_narrowed = payload["auditNarrowedUrls"]
        self.assertTrue(any("audit_statuses=" in url for url in audit_narrowed), audit_narrowed)
        self.assertFalse(any("verified_keep" in url for url in audit_narrowed), audit_narrowed)
        self.assertEqual(payload["auditGapCount"], len(audit_narrowed))
        self.assertTrue(payload["auditGapNotice"])
        audit_restored = payload["auditRestoredUrls"]
        self.assertEqual(len(audit_restored), len(audit_narrowed) + 1)
        self.assertIn("audit_statuses=", audit_restored[-1])
        self.assertNotIn("verified_keep", audit_restored[-1])
        self.assertFalse(payload["auditRestoredNotice"])

        # Layer-only: narrowed request carries layer_includes; the gap fires
        # nothing and shows the notice; the restore resubmits it.
        layer_narrowed = payload["layerNarrowedUrls"]
        self.assertTrue(any("layer_includes=layer_1" in url for url in layer_narrowed), layer_narrowed)
        self.assertEqual(payload["layerGapCount"], len(layer_narrowed))
        self.assertTrue(payload["layerGapNotice"])
        layer_restored = payload["layerRestoredUrls"]
        self.assertEqual(len(layer_restored), len(layer_narrowed) + 1)
        self.assertIn("layer_includes=layer_1", layer_restored[-1])
        self.assertFalse(payload["layerRestoredNotice"])

    def test_summary_gap_kept_page_requires_exact_identity(self) -> None:
        """Rerun4 finding 4: the preserved gap page is reused ONLY on an
        exact {revision, filterSignature, offset, limit} tuple match — a
        completed UNFILTERED page from the same revision while the narrowed
        request is still in flight yields the blocking empty state, and a
        nonzero-offset kept page is bound to its offset."""
        script = textwrap.dedent(
            _MINIDOM_PREAMBLE
            + _HARNESS_PREAMBLE
            + _FIXTURES_PREAMBLE
            + """
            (async () => {
              const resultsBoard = loadTs("frontend-demo/src/components/ResultsBoardPanel.tsx");
              const REV = "rev-1";
              const summaryFor = (count) => ({
                candidateCount: count,
                layers: [],
                recall: [],
                employment: [],
                locations: [],
                functions: [
                  { id: "research", label: "Researcher", count: count / 2 },
                  { id: "engineering", label: "Engineer", count: count / 2 },
                ],
              });
              const boardRuntimeFor = (facet, count) => ({
                expectedCandidateCount: count,
                rowPublicationRevision: REV,
                rowPublicationTier: "serving_projection_members",
                facetSummaryStatus: facet ? "complete" : "unavailable",
                facetSummaryScope: facet ? "global_full_population" : "unavailable",
                facetSummaryCandidateCount: facet ? count : 0,
                filterContract: {
                  source: "serving_projection_reader",
                  facetCountScope: "exact_projection",
                  rowFilterScope: "projection_membership",
                  backendFilteredPagingSupported: true,
                },
              });
              const boardCandidate = (patch) => ({
                ...baseCandidate,
                evidence: [],
                confidence: "high",
                avatarUrl: "",
                ...patch,
              });
              const candidates = [
                boardCandidate({
                  id: "row-1",
                  employmentStatus: "current",
                  functionBucketIds: ["engineering"],
                  functionBucketSource: "lane_membership",
                }),
                boardCandidate({
                  id: "row-2",
                  employmentStatus: "former",
                  functionBucketIds: ["research"],
                  functionBucketSource: "registry_evidence",
                }),
              ];
              const dashboardFor = (withSummary, count, projId = "proj-1") => ({
                candidates,
                layers: [],
                intentKeywords: [],
                totalCandidates: count,
                manualReviewCount: 0,
                projectionId: projId,
                candidateFacetSummary: withSummary ? summaryFor(count) : undefined,
                candidateFacetSummaryScope: withSummary ? "global_full_population" : "",
                boardRuntimeState: boardRuntimeFor(withSummary, count),
              });
              const boardProps = (dashboard, projId = "proj-1") => ({
                dashboard,
                projectionId: projId,
                historyId: "",
                jobId: "",
                initialCandidateId: "",
                isHydratingCandidates: false,
                candidateHydrationError: "",
                reviewStatusMap: {},
                onSelectedCandidateChange: () => {},
                onOpenManualReview: () => {},
                onReviewStateChanged: () => {},
              });
              const pagePayloadFor = (count, offset, projId = "proj-1") => ({
                status: "ready",
                projection: {
                  projection_id: projId,
                  source_run_id: "run-1",
                  membership_revision: REV,
                  visible_member_count: count,
                  read_contract: {
                    source: "serving_projection_members",
                    fallback_used: false,
                    fail_closed: true,
                  },
                  counts: {
                    count_scope: "exact_projection",
                    result_count: count,
                    candidate_count: count,
                    visible_member_count: count,
                    facet_count_scope: "exact_projection",
                  },
                  readiness: {},
                },
                total_candidates: count,
                filtered_candidate_count: count,
                offset,
                limit: 24,
                has_more: offset + 24 < count,
                next_offset: offset + 24 < count ? offset + 24 : null,
                candidates: [
                  {
                    candidate_id: "m-1",
                    employment_scope: "current",
                    public_summary: { display_name: "Member One" },
                  },
                  {
                    candidate_id: "m-2",
                    employment_scope: "former",
                    public_summary: { display_name: "Member Two" },
                  },
                ],
                facet_summary: {
                  status: "complete",
                  count_scope: "exact_projection",
                  candidate_count: count,
                },
                filter_contract: {
                  source: "serving_projection_reader",
                  facet_count_scope: "exact_projection",
                  row_filter_scope: "projection_membership",
                  backend_filtered_paging_supported: true,
                },
              });
              const requestOffset = (url) =>
                Number(new URL(url, "http://stub.local").searchParams.get("offset") || 0);
              const findFacetOptionInput = (labelText, rootNode) => {
                let found = null;
                const visit = (node) => {
                  if (found || !node) return;
                  if (
                    node.nodeType === 1 &&
                    node.nodeName === "LABEL" &&
                    String(node.getAttribute("class") || "").includes("facet-option") &&
                    node.textContent.includes(labelText)
                  ) {
                    found = (node.childNodes || []).find((child) => child.nodeName === "INPUT") || null;
                    return;
                  }
                  for (const child of node.childNodes || []) visit(child);
                };
                visit(rootNode);
                return found;
              };
              const findButtonByText = (rootNode, text) => {
                let found = null;
                const visit = (node) => {
                  if (found || !node) return;
                  if (
                    node.nodeType === 1 &&
                    node.nodeName === "BUTTON" &&
                    node.textContent.trim().includes(text)
                  ) {
                    found = node;
                    return;
                  }
                  for (const child of node.childNodes || []) visit(child);
                };
                visit(rootNode);
                return found;
              };
              const mountBoard = () => {
                const container = miniWindow.document.createElement("div");
                miniWindow.document.body.appendChild(container);
                const root = ReactDOMClient.createRoot(container);
                return { container, root };
              };
              const renderBoard = async (root, dashboard, projId = "proj-1") => {
                await act(async () => {
                  root.render(el(resultsBoard.ResultsBoardPanel, boardProps(dashboard, projId)));
                });
                await settle();
              };
              const noticeText = (container) => {
                const notice = findByTestId(container, "facet-gap-preserved-notice");
                return notice ? notice.textContent : "";
              };

              // --- Scenario A: completed UNFILTERED page + narrowed request
              // still in flight when the gap starts (the rerun4 probe) ---
              let resolveNarrowed;
              const narrowedGate = new Promise((resolve) => {
                resolveNarrowed = resolve;
              });
              addRoute("GET", "/api/projections/proj-1/candidates", (body, pathname, url) =>
                String(url).includes("function_buckets")
                  ? narrowedGate.then(() => pagePayloadFor(2, 0))
                  : pagePayloadFor(2, 0),
              );
              const mountA = mountBoard();
              await renderBoard(mountA.root, dashboardFor(true, 2));
              // Narrow the function facet to research-only; the narrowed
              // request is DEFERRED behind the gate.
              setCheckbox(findFacetOptionInput("Engineer", mountA.container), false);
              await settle(2);
              await renderBoard(mountA.root, dashboardFor(false, 2));
              await settle();
              const blockedNotice = noticeText(mountA.container);
              // The deferred narrowed response then lands — but the effect's
              // cleanup CANCELLED the pre-gap request, so it must NOT be
              // misread as the kept page: the blocking empty state stays.
              resolveNarrowed();
              await settle();
              const stillBlockedAfterLanding = noticeText(mountA.container);

              // --- Scenario B: the nonzero-offset kept page is reused ONLY
              // on its exact tuple. The user narrows and pages to offset 24
              // (the narrowed page completes there); the gap starts with the
              // page position preserved, so the offset-24 page matches the
              // {revision, signature, offset, limit} tuple and the kept-page
              // copy shows; navigating back to page 1 inside the gap breaks
              // the offset component and the blocking empty state must show —
              // never the mismatched page — and no new request fires. A
              // DISTINCT projection id keeps this mount's facet-session
              // context clean (mount A's preserved session must not leak in).
              addRoute("GET", "/api/projections/proj-2/candidates", (body, pathname, url) =>
                pagePayloadFor(48, requestOffset(String(url)), "proj-2"),
              );
              const mountB = mountBoard();
              await renderBoard(mountB.root, dashboardFor(true, 48, "proj-2"), "proj-2");
              setCheckbox(findFacetOptionInput("Engineer", mountB.container), false);
              await settle();
              clickEl(findButtonByText(mountB.container, "下一页"));
              await settle();
              const preGapRequestCount = callsTo("/api/projections/proj-2/candidates").length;
              await renderBoard(mountB.root, dashboardFor(false, 48, "proj-2"), "proj-2");
              await settle();
              const keptOffsetNotice = noticeText(mountB.container);
              clickEl(findButtonByText(mountB.container, "上一页"));
              await settle();
              const offsetMismatchNotice = noticeText(mountB.container);
              const gapRequestCount = callsTo("/api/projections/proj-2/candidates").length;

              console.log(JSON.stringify({
                blockedNotice,
                stillBlockedAfterLanding,
                keptOffsetNotice,
                offsetMismatchNotice,
                preGapRequestCount,
                gapRequestCount,
              }));
            })().catch((error) => {
              console.error(error);
              process.exit(1);
            });
            """
        )
        payload = _run_node(script)

        # Scenario A: with only the older UNFILTERED page completed, the gap
        # shows the blocking EMPTY state (not the "keeping filtered result"
        # copy); the deferred narrowed response landing mid-gap is cancelled
        # by the effect cleanup and must NOT be misread as the kept page.
        self.assertIn("已保留你的筛选设置", payload["blockedNotice"])
        self.assertIn("暂不显示候选人", payload["blockedNotice"])
        self.assertNotIn("保持索引不可用前", payload["blockedNotice"])
        self.assertIn("暂不显示候选人", payload["stillBlockedAfterLanding"])
        self.assertNotIn("保持索引不可用前", payload["stillBlockedAfterLanding"])

        # Scenario B (nonzero offset): the offset-24 narrowed page matches the
        # tuple and shows the kept-page copy; paging away inside the gap
        # breaks the offset component and flips to the blocking empty state
        # (never the mismatched page), and no new request fires either way.
        self.assertIn("保持索引不可用前最近一次同修订", payload["keptOffsetNotice"])
        self.assertIn("已保留你的筛选设置", payload["offsetMismatchNotice"])
        self.assertIn("暂不显示候选人", payload["offsetMismatchNotice"])
        self.assertNotIn("保持索引不可用前", payload["offsetMismatchNotice"])
        self.assertEqual(payload["gapRequestCount"], payload["preGapRequestCount"])


if __name__ == "__main__":
    unittest.main()
