import { Config, Effect, Layer } from "effect";
import { EventLogStoreLayer as DurableStreamsLayer } from "./event-log.durable-streams.server.ts";
import { EventLogStoreLayer as RedisLayer } from "./event-log.redis.server.ts";

/**
 * The `EventLog` store that `EVENT_LOG_STORE` names: `redis` (the default) or
 * `durable-streams`. The web process and the worker must name the same one,
 * and a run's events stay in the store it started on.
 */
export const EventLogStoreLayer = Layer.unwrap(
  Effect.gen(function* () {
    const store = yield* Config.Literals(
      ["redis", "durable-streams"],
      "EVENT_LOG_STORE",
    ).pipe(Config.withDefault("redis"));
    return store === "redis" ? RedisLayer : DurableStreamsLayer;
  }),
);
