import { createFileRoute } from "@tanstack/react-router";
import { handler } from "../../server/chat.server.ts";

// Start's server route only forwards the request: effect-agent-harness's router decodes
// it, and streams the run back (POST) or answers the thread's hydrate (GET).
export const Route = createFileRoute("/api/chat")({
  server: {
    handlers: {
      GET: ({ request }) => handler(request),
      POST: ({ request }) => handler(request),
    },
  },
});
