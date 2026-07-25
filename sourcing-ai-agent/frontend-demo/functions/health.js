import { proxyRequest } from "../cloudflare_api_proxy.mjs";

export async function onRequest(context) {
  try {
    return await proxyRequest(context.request, context.env);
  } catch (error) {
    return new Response(error instanceof Error ? error.message : "Proxy request failed", {
      status: 502,
    });
  }
}
