import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

function isSameOriginApiBaseUrl(value: string): boolean {
  const normalized = String(value || "").trim().toLowerCase();
  return (
    !normalized ||
    normalized === "/" ||
    ["same-origin", "same_origin", "sameorigin", "relative", "origin-relative"].includes(normalized)
  );
}

function normalizeProxyTarget(value: string): string {
  return String(value || "").trim().replace(/\/+$/, "");
}

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const configuredApiBaseUrl = String(env.VITE_API_BASE_URL || "").trim();
  const configuredProxyTarget = normalizeProxyTarget(
    String(env.VITE_DEV_PROXY_TARGET || env.VITE_API_PROXY_TARGET || "").trim(),
  );
  const proxyTarget = configuredProxyTarget || normalizeProxyTarget(configuredApiBaseUrl) || "http://localhost:8765";
  const sameOriginProxy = isSameOriginApiBaseUrl(configuredApiBaseUrl)
    ? {
        "/api": {
          target: proxyTarget,
          changeOrigin: true,
          secure: false,
        },
      }
    : undefined;
  return {
    plugins: [react()],
    server: {
      host: "127.0.0.1",
      port: 4173,
      proxy: sameOriginProxy,
    },
    preview: {
      host: "127.0.0.1",
      port: 4173,
      proxy: sameOriginProxy,
    },
  };
});
