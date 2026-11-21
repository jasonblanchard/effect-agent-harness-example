import { Effect, Layer, Schema, Stream } from "effect";
import { AgUiSchema } from "effect/ai";
import { HttpRouter, HttpServer } from "effect/http";
import {
  HttpApi,
  HttpApiBuilder,
  HttpApiEndpoint,
  HttpApiGroup,
  HttpApiSchema,
} from "effect/http-api";
import { convertMessagesToModelMessages, modelMessagesToUIMessages } from "@tanstack/ai";
import type { ModelMessage } from "@tanstack/ai";
import { EventLog, History } from "effect-agent-harness";
import { AssistantWorkflow } from "./assistant.server.ts";
import { Authentication, AuthenticationLayer } from "./auth.server.ts";
import { WorkflowClientLayer } from "./cluster.server.ts";
import { EventLogStoreLayer } from "./event-log.server.ts";
import { ThreadStoreLayer } from "./thread-store.server.ts";

const ChatQuery = {
  threadId: Schema.String,
  runId: Schema.optional(Schema.String),
  offset: Schema.optional(Schema.String),
};

const ResumeHeaders = { "last-event-id": Schema.optional(Schema.String) };

/** What TanStack AI's `stop()` posts to `cancelUrl`. */
const CancelBody = Schema.Struct({ threadId: Schema.String, runId: Schema.String });

/** An event from the log as an SSE frame: its cursor is the frame's `id`, which a reconnecting client sends back as `Last-Event-ID`. */
const StoredEvent = Schema.Struct({
  id: Schema.String,
  event: Schema.String,
  data: Schema.fromJsonString(Schema.toCodecJson(AgUiSchema.Event)),
});

/** A run's events, followed from the log. */
const RunEvents = HttpApiSchema.WithHeaders(HttpApiSchema.StreamSse({ events: StoredEvent }), {
  "cache-control": Schema.Literal("no-cache"),
});

/** TanStack AI's server hydrate: the thread's `UIMessage`s, its open approvals, and the run still going, if any. */
const Hydrate = Schema.Struct({
  messages: Schema.Array(Schema.Unknown),
  interrupts: Schema.NullOr(Schema.Unknown),
  activeRun: Schema.NullOr(Schema.Unknown),
});

/** How long a run's event stream stays open with nothing new. Longer than the slowest model or tool step. */
const IDLE_TIMEOUT = "2 minutes";

/**
 * A read thread as TanStack's `UIMessage`s, which its hydrate requires: it speaks
 * AG-UI on the run stream, but not here. `convertMessagesToModelMessages` takes
 * AG-UI rows, folding each `reasoning` message into the assistant message after
 * it, though its type doesn't say so, hence the cast. Reasoning with no assistant
 * message directly after it (a step that produced only reasoning) is dropped.
 */
const toUIMessages = (messages: History.Restored["messages"]) =>
  modelMessagesToUIMessages(convertMessagesToModelMessages([...messages] as Array<ModelMessage>));

// The chat's endpoints, as TanStack AI calls them. They are not `AgUiServer.endpoint`: every run executes on the
// worker, and the response follows its event log with cursors as SSE ids, so a client reconnects with
// `Last-Event-ID`. Every endpoint runs the auth middleware, but nothing checks that the thread is the user's: the
// thread store doesn't record an owner.
class ChatApi extends HttpApi.make("chat").add(
  HttpApiGroup.make("chat")
    // GET /api/chat?threadId=…: TanStack AI's server hydrate (`persistence:
    // true`). On mount, the page restores the thread's messages and its open
    // approvals from here, and learns of a run still going (`activeRun`).
    //
    // GET /api/chat?threadId=…&offset=-1&runId=…: TanStack AI's join of that
    // run, replayed from the start. `-1` is its marker for "from the start",
    // which the log would reject as a cursor.
    .add(
      HttpApiEndpoint.get("read", "/api/chat", {
        query: ChatQuery,
        headers: ResumeHeaders,
        success: [Hydrate, RunEvents],
        error: EventLog.InvalidCursor.pipe(HttpApiSchema.status(400)),
      }),
    )
    // POST /api/chat: TanStack AI's `fetchServerSentEvents` posts an AG-UI
    // `RunAgentInput` and reads the AG-UI events back as SSE. The worker executes
    // the run and this response only follows its event log, so a disconnect (a
    // reload, or a closed tab) ends the response and the run carries on: only
    // the cancel endpoint below stops it. A run that stops for an approval is
    // answered by the next run's `resume` entries. The frames carry cursors, so
    // after a dropped connection the client sends the same request again with
    // `Last-Event-ID`. Starting that run again joins its existing execution.
    .add(
      HttpApiEndpoint.post("send", "/api/chat", {
        payload: AgUiSchema.RunAgentInput,
        headers: ResumeHeaders,
        success: RunEvents,
        error: EventLog.InvalidCursor.pipe(HttpApiSchema.status(400)),
      }),
    )
    // POST /api/chat/cancel: the page's Stop button. TanStack AI's `stop()`
    // aborts its own stream and posts the run's ids here (`cancelUrl`). The
    // cancel is held by the workflow engine, so it reaches the run on whichever
    // worker executes it, even one that hasn't started it yet or resumes it
    // after a crash. The run ends with `RUN_FINISHED` outcome `cancelled`, its
    // partial reply is committed, and the thread is free for the next message.
    // It returns at once, and cancelling a run that has ended does nothing.
    .add(
      HttpApiEndpoint.post("cancel", "/api/chat/cancel", {
        payload: CancelBody,
        success: HttpApiSchema.NoContent,
      }),
    )
    .middleware(Authentication),
) {}

const ChatHandlers = HttpApiBuilder.group(
  ChatApi,
  "chat",
  Effect.fn(function* (handlers) {
    const log = yield* EventLog.EventLog;

    // A run's log as SSE, after `after`. The worker may be down, so the stream
    // ends once nothing has arrived for a while. A store failure is the
    // server's fault (500), and aborts a stream already going; an unissued
    // cursor is a 400.
    const follow = (threadId: string, runId: string, after: string | undefined) =>
      log.subscribe(threadId, runId, after).pipe(
        Effect.catchTag("EventLogError", Effect.die),
        Effect.map((events) =>
          HttpApiSchema.withHeaders({
            body: events.pipe(
              Stream.timeout(IDLE_TIMEOUT),
              Stream.map(({ cursor, event }) => ({ id: cursor, event: "message", data: event })),
              Stream.orDie,
            ),
            headers: { "cache-control": "no-cache" as const },
          }),
        ),
      );

    return handlers
      .handle("read", ({ query: { threadId, runId, offset }, headers }) => {
        if (runId !== undefined) {
          return follow(
            threadId,
            runId,
            headers["last-event-id"] ?? (offset === "-1" ? undefined : offset),
          );
        }
        return History.restore(threadId, { rejoin: true }).pipe(
          Effect.orDie,
          Effect.map(({ messages, interrupts, activeRun }) => ({
            messages: toUIMessages(messages),
            interrupts: interrupts === undefined ? null : interrupts,
            activeRun: activeRun === undefined ? null : activeRun,
          })),
        );
      })
      .handle("send", ({ payload, headers }) =>
        Effect.andThen(AssistantWorkflow.start(payload), () =>
          follow(payload.threadId, payload.runId, headers["last-event-id"]),
        ),
      )
      .handle("cancel", ({ payload }) => AssistantWorkflow.cancel(payload));
  }),
);

// The handlers read the stores and the workflow engine from each request's context, so they must be in the layer's
// output (`Layer.provideMerge`), which the web handler provides to every request. `Layer.provide` would hide them.
const ChatRoutes = HttpApiBuilder.layer(ChatApi).pipe(
  Layer.provide(ChatHandlers),
  Layer.provide(AuthenticationLayer),
  Layer.provideMerge(Layer.mergeAll(ThreadStoreLayer, EventLogStoreLayer, WorkflowClientLayer)),
  Layer.provide(HttpServer.layerServices),
);

/** The chat endpoints as a web handler (`Request => Promise<Response>`), mounted by `src/routes/api/chat.ts`. */
export const { handler } = HttpRouter.toWebHandler(ChatRoutes, { disableLogger: true });
