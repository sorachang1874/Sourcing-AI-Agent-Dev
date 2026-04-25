function normalizeUpstreamBase(rawValue) {
  const trimmed = String(rawValue || "").trim().replace(/\/+$/, "");
  if (!trimmed) {
    throw new Error("Missing API_UPSTREAM_BASE. Set it to the hosted backend origin, for example https://api.111874.xyz");
  }
  const parsed = new URL(trimmed);
  if (!/^https?:$/i.test(parsed.protocol)) {
    throw new Error(`Unsupported API_UPSTREAM_BASE protocol: ${parsed.protocol}`);
  }
  return parsed.toString().replace(/\/+$/, "");
}

function shouldProxyPath(pathname) {
  return pathname === "/health" || pathname === "/api" || pathname.startsWith("/api/");
}

function buildProxyRequest(request, upstreamBase) {
  const incomingUrl = new URL(request.url);
  const targetUrl = new URL(`${upstreamBase}${incomingUrl.pathname}${incomingUrl.search}`);
  if (incomingUrl.origin === targetUrl.origin) {
    throw new Error("Proxy loop detected: API_UPSTREAM_BASE must not point back to the same origin as the frontend route.");
  }
  const headers = new Headers(request.headers);
  headers.set("x-forwarded-host", incomingUrl.host);
  headers.set("x-forwarded-proto", incomingUrl.protocol.replace(":", ""));
  headers.set("x-sourcing-agent-proxy", "cloudflare-demo-api");
  const connectingIp = request.headers.get("cf-connecting-ip");
  if (connectingIp) {
    headers.set("x-forwarded-for", connectingIp);
  }
  const init = {
    method: request.method,
    headers,
    redirect: "manual",
    body: request.method === "GET" || request.method === "HEAD" ? undefined : request.body,
  };
  return new Request(targetUrl.toString(), init);
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    if (!shouldProxyPath(url.pathname)) {
      return new Response("Not found", { status: 404 });
    }

    let upstreamBase;
    try {
      upstreamBase = normalizeUpstreamBase(env.API_UPSTREAM_BASE);
    } catch (error) {
      return new Response(error instanceof Error ? error.message : "Invalid API_UPSTREAM_BASE", { status: 500 });
    }

    try {
      return await fetch(buildProxyRequest(request, upstreamBase));
    } catch (error) {
      return new Response(error instanceof Error ? error.message : "Proxy request failed", { status: 502 });
    }
  },
};
