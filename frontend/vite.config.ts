import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Every backend process is reached through this proxy, so the browser only
// ever talks to one origin and the API needs no CORS configuration. The
// consumer and the ingest poller each serve their own Prometheus endpoint on
// a separate port, which is why they get prefixes of their own.
const API = process.env.API_URL ?? "http://localhost:8000";
const CONSUMER = process.env.CONSUMER_METRICS_URL ?? "http://localhost:9100";
const INGEST = process.env.INGEST_METRICS_URL ?? "http://localhost:9200";

const strip = (prefix: string) => (path: string) => path.replace(prefix, "");

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      "/api": { target: API, changeOrigin: true, rewrite: strip("/api") },
      "/consumer": { target: CONSUMER, changeOrigin: true, rewrite: strip("/consumer") },
      "/ingest": { target: INGEST, changeOrigin: true, rewrite: strip("/ingest") },
    },
  },
});
