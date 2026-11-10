import { DateTime, Effect } from "effect";
import { Toolkit } from "effect/ai";
import { Agent, Runner } from "effect-agent-harness";
import { SendEmail, sendEmail } from "../tools/email.tool.ts";
import { GetWeather, getWeather } from "../tools/weather.tool.ts";
import { CurrentUser } from "./auth.server.ts";

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

export const ToolsLayer = AssistantToolkit.toLayer({
  "get-weather": getWeather,
  "send-email": sendEmail,
});

/**
 * The agent as a durable workflow. The web process calls `start`, and the worker
 * registers `layer`. The name is persisted with every run, so don't rename it
 * while runs are in flight. `start` reads `CurrentUser` from the request.
 */
export const AssistantWorkflow = Runner.makeWorkflow(assistant, {
  name: "assistant",
  dependencies: [CurrentUser],
});
