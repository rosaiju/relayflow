import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Dev server proxies /api to the local FastAPI process. Both bind to localhost only.
export default defineConfig({
  plugins: [react()],
  server: {
    host: "127.0.0.1",
    port: 5173,
    proxy: { "/api": process.env.RELAYFLOW_API_URL ?? "http://127.0.0.1:8000" },
  },
  preview: { host: "127.0.0.1", port: 4173 },
});
