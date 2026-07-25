const DEFAULT_API_UPSTREAM_BASE = "https://api.111874.xyz";

function normalizeUpstreamBase(rawValue) {
  const trimmed = String(rawValue || "").trim().replace(/\/+$/, "");
  if (!trimmed) {
    throw new Error("Missing API upstream base.");
  }
  const parsed = new URL(trimmed);
  if (!/^https?:$/i.test(parsed.protocol)) {
    throw new Error(`Unsupported upstream protocol: ${parsed.protocol}`);
  }
  return parsed.toString().replace(/\/+$/, "");
}

function resolveUpstreamBase(env) {
  return normalizeUpstreamBase(env?.API_UPSTREAM_BASE || DEFAULT_API_UPSTREAM_BASE);
}

function buildUpstreamUrl(requestUrl, upstreamBase) {
  const incomingUrl = new URL(requestUrl);
  const targetUrl = new URL(`${upstreamBase}${incomingUrl.pathname}${incomingUrl.search}`);
  if (incomingUrl.origin === targetUrl.origin) {
    throw new Error("Proxy loop detected.");
  }
  return { incomingUrl, targetUrl };
}

function buildProxyRequest(request, upstreamBase) {
  const { incomingUrl, targetUrl } = buildUpstreamUrl(request.url, upstreamBase);
  const headers = new Headers(request.headers);
  headers.delete("host");
  headers.set("x-forwarded-host", incomingUrl.host);
  headers.set("x-forwarded-proto", incomingUrl.protocol.replace(":", ""));
  headers.set("x-sourcing-agent-proxy", "cloudflare-pages-function");
  const connectingIp = request.headers.get("cf-connecting-ip");
  if (connectingIp) {
    headers.set("x-forwarded-for", connectingIp);
  }
  return new Request(targetUrl.toString(), {
    method: request.method,
    headers,
    redirect: "manual",
    body: request.method === "GET" || request.method === "HEAD" ? undefined : request.body,
  });
}

export async function proxyRequest(request, env) {
  const upstreamBase = resolveUpstreamBase(env);
  return fetch(buildProxyRequest(request, upstreamBase));
}
