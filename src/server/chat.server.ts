import { AnthropicClient, AnthropicLanguageModel } from "@effect/ai-anthropic";
import { Config, DateTime, Effect, Layer, Schema } from "effect";
import { Toolkit } from "effect/ai";
import { FetchHttpClient, HttpRouter, HttpServerRequest, HttpServerResponse } from "effect/http";
import { convertMessagesToModelMessages, modelMessagesToUIMessages } from "@tanstack/ai";
import type { ModelMessage } from "@tanstack/ai";
import {
  Agent,
  AgUi,
  Checkpoint,
  EventLog,
  History,
  Runner,
  Sse,
  Thread,
} from "effect-agent-harness";
import { SendEmail, sendEmail } from "../tools/email.tool.ts";
import { GetWeather, getWeather } from "../tools/weather.tool.ts";

// The agent's behaviour, independent of the model provider and of any store.
const AssistantToolkit = Toolkit.make(GetWeather, SendEmail);

const assistant = Agent.define({
  toolkit: AssistantToolkit,
  system: Effect.gen(function* () {
    const today = DateTime.formatIsoDateUtc(yield* DateTime.now);
    return `
      You are a helpful AI assistant. Be friendly, conversational, and concise.
      Reply in plain text: the chat window does not render Markdown.
      Today is ${today}.
    `;
  }),
});

// Reasoning is switched on here, in the provider's config: effect-agent-harness streams
// whatever the model produces, to every client. Without `display`, the API
// returns thinking blocks with no text. `@effect/ai-anthropic`'s type doesn't
// declare it yet, hence the separate constant.
const thinking = { type: "adaptive", display: "summarized" } as const;

const ModelLayer = AnthropicLanguageModel.layer({
  model: "claude-haiku-5-5",
  config: { thinking },
}).pipe(
  Layer.provide(
    AnthropicClient.layerConfig({
      apiKey: Config.Redacted("ANTHROPIC_API_KEY"),
    }),
  ),
  Layer.provide(FetchHttpClient.layer),
);

const ToolsLayer = AssistantToolkit.toLayer({ "get-weather": getWeather, "send-email": sendEmail });

// The server owns the transcript (`History.layerServer`), because the hydrate
// route below reads each thread back from the store after a reload. Threads
// live in memory for the life of the process, and a crashed run is not
// replayed. `Thread` stays in the output for the hydrate route.
const StoresLayer = Layer.mergeAll(
  History.layerServer.pipe(Layer.provideMerge(Thread.layerMemory)),
  Checkpoint.layerNone,
  EventLog.layerMemory,
);

const HydrateQuery = Schema.Struct({ threadId: Schema.String });

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
    const runAgent = Runner.makeAttached(assistant);

    // GET /api/chat?threadId=…: TanStack AI's server hydrate (`persistence:
    // true`). On mount, the page restores the thread's messages and its open
    // approvals from here, so a reload continues the thread. Runs are
    // request-scoped, so a reload cancels the run in flight: there is never
    // an `activeRun` to rejoin.
    yield* router.add(
      "GET",
      "/api/chat",
      Effect.gen(function* () {
        const { threadId } = yield* HttpServerRequest.schemaSearchParams(HydrateQuery);
        const { messages, interrupts } = yield* History.restore(threadId);
        return yield* HttpServerResponse.json({
          messages: toUIMessages(messages),
          interrupts: interrupts === undefined ? null : interrupts,
          activeRun: null,
        });
      }).pipe(
        Effect.catchTag("SchemaError", () =>
          Effect.succeed(HttpServerResponse.text("Missing threadId", { status: 400 })),
        ),
      ),
    );

    // POST /api/chat: TanStack AI's `fetchServerSentEvents` posts an AG-UI
    // `RunAgentInput` and reads the AG-UI events back as SSE. A disconnect
    // (the client's `stop()`) cancels the run. A run that stops for an
    // approval is answered by the next run's `resume` entries.
    yield* router.add(
      "POST",
      "/api/chat",
      Effect.gen(function* () {
        const input = yield* HttpServerRequest.schemaBodyJson(AgUi.RunAgentInput);
        return yield* runAgent(input).pipe(Sse.encode, Sse.response);
      }).pipe(
        Effect.catch((error) =>
          Effect.succeed(
            HttpServerResponse.text(`Invalid RunAgentInput: ${error.message}`, { status: 400 }),
          ),
        ),
      ),
    );
  }),
).pipe(Layer.provideMerge(Layer.mergeAll(ModelLayer, ToolsLayer, StoresLayer)));

/** The chat endpoint as a web handler (`Request => Promise<Response>`), mounted by `src/routes/api/chat.ts`. */
export const { handler } = HttpRouter.toWebHandler(ChatRoutes, { disableLogger: true });
