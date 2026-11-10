import { Effect, Layer, Schema, Stream } from "effect";
import { HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { convertMessagesToModelMessages, modelMessagesToUIMessages } from "@tanstack/ai";
import type { ModelMessage } from "@tanstack/ai";
import { AgUi, EventLog, History, Sse } from "effect-agent-harness";
import { AssistantWorkflow } from "./assistant.server.ts";
import { AuthMiddlewareLayer } from "./auth.server.ts";
import { WorkflowClientLayer } from "./cluster.server.ts";
import { EventLogStoreLayer } from "./event-log.server.ts";
import { ThreadStoreLayer } from "./thread-store.server.ts";

const ChatQuery = Schema.Struct({
  threadId: Schema.String,
  runId: Schema.optional(Schema.String),
  offset: Schema.optional(Schema.String),
});

const ResumeHeaders = Schema.Struct({
  "last-event-id": Schema.optional(Schema.String),
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

const ChatRoutes = HttpRouter.use((router) =>
  Effect.gen(function* () {
    const log = yield* EventLog.EventLog;

    // Streams a run's log as SSE, after `after`. The worker may be down, so the
    // stream ends once nothing has arrived for a while.
    const follow = (threadId: string, runId: string, after: string | undefined) =>
      Effect.flatMap(log.subscribe(threadId, runId, after), (events) =>
        events.pipe(Stream.timeout(IDLE_TIMEOUT), Sse.encodeStored, Sse.response),
      );

    // GET /api/chat?threadId=…: TanStack AI's server hydrate (`persistence:
    // true`). On mount, the page restores the thread's messages and its open
    // approvals from here, and learns of a run still going (`activeRun`).
    //
    // GET /api/chat?threadId=…&offset=-1&runId=…: TanStack AI's join of that
    // run, replayed from the start. `-1` is its marker for "from the start",
    // which the log would reject as a cursor.
    yield* router.add(
      "GET",
      "/api/chat",
      Effect.gen(function* () {
        const { threadId, runId, offset } = yield* HttpServerRequest.schemaSearchParams(ChatQuery);

        if (runId !== undefined) {
          const { "last-event-id": lastEventId } =
            yield* HttpServerRequest.schemaHeaders(ResumeHeaders);
          return yield* follow(
            threadId,
            runId,
            lastEventId ?? (offset === "-1" ? undefined : offset),
          );
        }

        const { messages, interrupts, activeRun } = yield* History.restore(threadId, {
          rejoin: true,
        });

        return yield* HttpServerResponse.json({
          messages: toUIMessages(messages),
          interrupts: interrupts === undefined ? null : interrupts,
          activeRun: activeRun === undefined ? null : activeRun,
        });
      }).pipe(
        Effect.catchTags({
          SchemaError: () =>
            Effect.succeed(HttpServerResponse.text("Missing threadId", { status: 400 })),
          InvalidCursor: (error) =>
            Effect.succeed(HttpServerResponse.text(error.message, { status: 400 })),
        }),
      ),
    );

    // POST /api/chat: TanStack AI's `fetchServerSentEvents` posts an AG-UI
    // `RunAgentInput` and reads the AG-UI events back as SSE. The worker executes
    // the run and this response only follows its event log, so a disconnect (a
    // reload, or the client's `stop()`) ends the response and the run carries
    // on. A run that stops for an approval is answered by the next run's
    // `resume` entries. The frames carry cursors, so after a dropped connection
    // the client sends the same request again with `Last-Event-ID`. Starting
    // that run again joins its existing execution.
    yield* router.add(
      "POST",
      "/api/chat",
      Effect.gen(function* () {
        const input = yield* HttpServerRequest.schemaBodyJson(AgUi.RunAgentInput);
        const { "last-event-id": lastEventId } =
          yield* HttpServerRequest.schemaHeaders(ResumeHeaders);

        yield* AssistantWorkflow.start(input);
        return yield* follow(input.threadId, input.runId, lastEventId);
      }).pipe(
        Effect.catch((error) =>
          Effect.succeed(
            HttpServerResponse.text(`Invalid request: ${error.message}`, {
              status: 400,
            }),
          ),
        ),
      ),
    );
  }),
).pipe(
  Layer.provide(AuthMiddlewareLayer),
  Layer.provideMerge(Layer.mergeAll(ThreadStoreLayer, EventLogStoreLayer, WorkflowClientLayer)),
);

/** The chat endpoint as a web handler (`Request => Promise<Response>`), mounted by `src/routes/api/chat.ts`. */
export const { handler } = HttpRouter.toWebHandler(ChatRoutes, {
  disableLogger: true,
});
