import { tanstackStart } from "@tanstack/react-start/plugin/vite";
import viteReact from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  plugins: [
    // SPA mode: the page renders in the browser; the server only serves the
    // shell and the `/api/chat` server route.
    tanstackStart({ spa: { enabled: true } }),
    viteReact(),
  ],
});
