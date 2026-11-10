import { AnthropicClient, AnthropicLanguageModel } from "@effect/ai-anthropic";
import { NodeRuntime } from "@effect/platform-node";
import { Config, Layer } from "effect";
import { FetchHttpClient } from "effect/http";
import { History } from "effect-agent-harness";
import { AssistantWorkflow, ToolsLayer } from "./assistant.server.ts";
import { WorkflowRunnerLayer } from "./cluster.server.ts";
import { EventLogStoreLayer } from "./event-log.server.ts";
import { ThreadStoreLayer } from "./thread-store.server.ts";

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

// Executes runs. When this process stops, another one picks up its unfinished
// runs from Postgres: finished steps are replayed, and the step in progress runs again.
const WorkerLayer = AssistantWorkflow.layer.pipe(
  Layer.provide(
    Layer.mergeAll(
      ModelLayer,
      ToolsLayer,
      EventLogStoreLayer,
      History.layerServer.pipe(Layer.provide(ThreadStoreLayer)),
    ),
  ),
  Layer.provide(WorkflowRunnerLayer),
);

Layer.launch(WorkerLayer).pipe(NodeRuntime.runMain);
