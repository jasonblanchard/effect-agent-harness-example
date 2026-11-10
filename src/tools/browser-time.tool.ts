import { toolDefinition } from "@tanstack/ai";
import { Schema } from "effect";

/**
 * A frontend tool: the browser runs it, not the server. `useChat` advertises
 * it in each run's `tools`; when the model calls it, the run ends with the call
 * pending, the browser answers it, and TanStack AI sends the result in the
 * next run.
 */
export const getBrowserTime = toolDefinition({
  name: "get-browser-time",
  description: "Get the current date, time and time zone of the user's browser",
  // No arguments. Not `Schema.Struct({})`: its JSON Schema is `{ not: { type: "null" } }`, and the
  // provider rejects tool parameters that aren't `type: "object"`.
  inputSchema: Schema.toStandardJSONSchemaV1(Schema.Record(Schema.String, Schema.Never)),
}).client(() => ({
  time: new Date().toString(),
  timeZone: Intl.DateTimeFormat().resolvedOptions().timeZone,
}));
