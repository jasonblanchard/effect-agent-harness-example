import { createFileRoute } from "@tanstack/react-router";
import { handler } from "../../server/chat.server.ts";

// `POST /api/chat/cancel`, the page's Stop button. The `_` keeps the route out of
// `/api/chat`'s children. Start only forwards the request to the same handler.
export const Route = createFileRoute("/api/chat_/cancel")({
  server: {
    handlers: {
      POST: ({ request }) => handler(request),
    },
  },
});
