import { reactive } from "vue";

const baseElement = document.querySelector("base");
const baseHref = baseElement?.getAttribute("href") || "/";
const basePath = normalizeBasePath(new URL(baseHref, window.location.origin).pathname);

export const route = reactive(parseRoute(window.location.pathname));

export function hrefFor(path = "") {
  const normalized = normalizePath(path);
  return `${basePath}${normalized}` || "/";
}

export function navigate(path, options = {}) {
  const href = hrefFor(path);
  if (options.replace) {
    window.history.replaceState(null, "", href);
  } else {
    window.history.pushState(null, "", href);
  }
  syncRoute();
}

export function installRouter() {
  const onPopState = () => syncRoute();
  window.addEventListener("popstate", onPopState);
  return () => window.removeEventListener("popstate", onPopState);
}

function syncRoute() {
  Object.assign(route, parseRoute(window.location.pathname));
}

function parseRoute(pathname) {
  const relative = stripBasePath(pathname);
  const parts = relative.split("/").filter(Boolean).map(decodePart);

  if (parts.length === 0) return {name: "dashboard", key: "dashboard"};
  if (parts[0] === "topics" && parts.length === 1) {
    return {name: "topics", key: "topics"};
  }
  if (parts[0] === "topics" && parts.length === 2) {
    return {name: "topic", topic: parts[1], key: `topic:${parts[1]}`};
  }
  if (parts[0] === "jobs" && parts.length === 2) {
    return {name: "job", id: parts[1], key: `job:${parts[1]}`};
  }
  if (["storage", "metrics", "settings"].includes(parts[0]) && parts.length === 1) {
    return {name: parts[0], key: parts[0]};
  }
  return {name: "not-found", key: `not-found:${relative}`};
}

function stripBasePath(pathname) {
  if (!basePath) return normalizePath(pathname);
  if (pathname === basePath) return "/";
  if (pathname.startsWith(`${basePath}/`)) {
    return normalizePath(pathname.slice(basePath.length));
  }
  return normalizePath(pathname);
}

function normalizeBasePath(path) {
  const normalized = normalizePath(path);
  return normalized === "/" ? "" : normalized.replace(/\/$/, "");
}

function normalizePath(path) {
  const value = String(path || "").trim();
  if (!value || value === "/") return "/";
  return `/${value.replace(/^\/+|\/+$/g, "")}`;
}

function decodePart(value) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
}
