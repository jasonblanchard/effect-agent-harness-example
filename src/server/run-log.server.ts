import { Config, Effect, Layer } from "effect";
import { RunLogStoreLayer as DurableStreamsLayer } from "./run-log.durable-streams.server.ts";
import { RunLogStoreLayer as RedisLayer } from "./run-log.redis.server.ts";

/**
 * The `RunLog` store that `RUN_LOG_STORE` names: `redis` (the default) or
 * `durable-streams`. The web process and the worker must name the same one,
 * and a run's events stay in the store it started on.
 */
export const RunLogStoreLayer = Layer.unwrap(
  Effect.gen(function* () {
    const store = yield* Config.Literals(
      ["redis", "durable-streams"],
      "RUN_LOG_STORE",
    ).pipe(Config.withDefault("redis"));
    return store === "redis" ? RedisLayer : DurableStreamsLayer;
  }),
);
