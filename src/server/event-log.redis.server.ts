import { NodeRedis } from "@effect/platform-node";
import { Config, Effect, Layer, Option, Schema, Stream } from "effect";
import { Redis } from "effect/persistence";
import { AgUi, EventLog } from "effect-agent-harness";

/**
 * The `EventLog` on Redis Streams, shared between the worker that appends and
 * the web process that streams.
 *
 * Each run is one stream. Redis gives every entry an id that increases within
 * the stream, so an event's cursor is its entry id. Reading after a cursor is
 * `XRANGE` from just past it, and following a run is `XREAD BLOCK` from its
 * last entry.
 *
 * An entry records whether its event is terminal, and `append` is a Lua script
 * that drops any event after a terminal one. So a step that appended the
 * terminal event and is replayed after a crash can't extend a finished run.
 */

/** How long one `XREAD BLOCK` waits before it is sent again. */
const BLOCK_MILLIS = "30000";

/**
 * The most runs one process can follow at once. Each follower holds a
 * connection while it blocks, and a follower past the limit waits for one.
 */
const MAX_CONNECTIONS = 100;

/** An entry as Redis returns it: its id, and its fields as a flat name/value list. */
const Entry = Schema.Tuple([Schema.String, Schema.Array(Schema.String)]);
const Entries = Schema.Array(Entry);
/**
 * `XREAD` on one stream: `null` when the block timed out. Over RESP3, which
 * node-redis speaks, the streams come back as a map from key to entries; over
 * RESP2 as a list of [key, entries] pairs.
 */
const ReadReply = Schema.NullOr(
  Schema.Union([
    Schema.Record(Schema.String, Entries),
    Schema.Array(Schema.Tuple([Schema.String, Entries])),
  ]),
);

const decodeEntries = Schema.decodeUnknownEffect(Entries);
const decodeReadReply = Schema.decodeUnknownEffect(ReadReply);

/** Appends unless the stream already ends with a terminal event. Returns the cursor: the new entry's id, or the terminal one's. */
const appendScript = Redis.script(
  (key: string, event: string, terminal: boolean) => [
    key,
    event,
    terminal ? "1" : "0",
  ],
  {
    numberOfKeys: 1,
    lua: `
      local last = redis.call("XREVRANGE", KEYS[1], "+", "-", "COUNT", 1)[1]
      if last then
        local fields = last[2]
        for i = 1, #fields, 2 do
          if fields[i] == "terminal" and fields[i + 1] == "1" then
            return last[1]
          end
        end
      end
      return redis.call("XADD", KEYS[1], "*", "event", ARGV[1], "terminal", ARGV[2])
    `,
  },
).withReturnType<string>();

/** A stream entry id, as `XADD *` makes them. */
const ENTRY_ID = /^[0-9]+-[0-9]+$/;

const toEventLogError = (cause: unknown) =>
  new EventLog.EventLogError({ cause });

const toStored = ([id, fields]: typeof Entry.Type): EventLog.Stored => {
  const json = fields[fields.indexOf("event") + 1];
  return {
    cursor: id,
    // Written only by `append`, from an `AgUi.Event`. Not decoded: the library has
    // no schema for the `AgUi.Event` union, only for each event.
    event: JSON.parse(json ?? "null") as AgUi.Event,
  };
};

export const EventLogStoreLayer = Layer.effect(
  EventLog.EventLog,
  Effect.gen(function* () {
    const redis = yield* Redis.Redis;
    const append = redis.eval(appendScript);

    // JSON, so no choice of ids can make two runs share a key.
    const streamKey = (threadId: string, runId: string) =>
      `run-events:${JSON.stringify([threadId, runId])}`;

    // The run's entries after `after`, or all of them.
    const range = (key: string, after: string | undefined) =>
      redis
        .send("XRANGE", key, after === undefined ? "-" : `(${after}`, "+")
        .pipe(
          Effect.flatMap(decodeEntries),
          Effect.map((entries) => entries.map(toStored)),
        );

    // The event at `cursor`, or `InvalidCursor` if the run has no entry with that id.
    const atCursor = (key: string, cursor: string) =>
      Effect.gen(function* () {
        if (!ENTRY_ID.test(cursor)) {
          return yield* new EventLog.InvalidCursor({ cursor });
        }
        const [entry] = yield* redis
          .send("XRANGE", key, cursor, cursor)
          .pipe(Effect.flatMap(decodeEntries));
        if (entry === undefined) {
          return yield* new EventLog.InvalidCursor({ cursor });
        }
        return toStored(entry);
      });

    // The entries after `after`, waiting up to `BLOCK_MILLIS` for one. Blocks on
    // a connection of its own, so other commands don't queue behind it.
    const readNew = (key: string, after: string) =>
      Effect.scoped(
        Effect.flatMap(redis.reserve, (connection) =>
          connection.send(
            "XREAD",
            "BLOCK",
            BLOCK_MILLIS,
            "STREAMS",
            key,
            after,
          ),
        ),
      ).pipe(
        Effect.flatMap(decodeReadReply),
        Effect.map((reply) =>
          (reply === null
            ? []
            : Array.isArray(reply)
            ? reply.map(([, entries]) => entries)
            : Object.values(reply)
          ).flatMap((entries) => entries.map(toStored)),
        ),
      );

    return EventLog.EventLog.of({
      append: (threadId, runId, event) =>
        append(
          streamKey(threadId, runId),
          JSON.stringify(event),
          AgUi.isTerminal(event),
        ).pipe(
          Effect.map((cursor) => ({ cursor, event })),
          Effect.mapError(toEventLogError),
        ),

      read: (threadId, runId, after) =>
        Effect.gen(function* () {
          const key = streamKey(threadId, runId);
          if (after !== undefined) {
            yield* atCursor(key, after);
          }
          return yield* range(key, after);
        }).pipe(
          Effect.catchIf(
            (error) => error._tag !== "InvalidCursor",
            (error) => Effect.fail(toEventLogError(error)),
          ),
        ),

      subscribe: (threadId, runId, after) =>
        Effect.gen(function* () {
          const key = streamKey(threadId, runId);
          const start =
            after === undefined ? undefined : yield* atCursor(key, after);
          const history = yield* range(key, after);
          // The run's last event so far, or nothing if it hasn't appended yet.
          const last = history.at(-1) ?? start;
          if (last !== undefined && AgUi.isTerminal(last.event)) {
            return Stream.fromIterable(history);
          }

          const live = Stream.paginate(last?.cursor ?? "0-0", (from) =>
            Effect.map(readNew(key, from), (page) => {
              const end = page.at(-1);
              return [
                page,
                end === undefined
                  ? Option.some(from)
                  : page.some((stored) => AgUi.isTerminal(stored.event))
                  ? Option.none<string>()
                  : Option.some(end.cursor),
              ] as const;
            }),
          );
          return Stream.concat(Stream.fromIterable(history), live).pipe(
            Stream.takeUntil((stored) => AgUi.isTerminal(stored.event)),
            Stream.mapError(toEventLogError),
          );
        }).pipe(
          Effect.catchIf(
            (error) => error._tag !== "InvalidCursor",
            (error) => Effect.fail(toEventLogError(error)),
          ),
        ),
    });
  }),
).pipe(
  Layer.provide(
    NodeRedis.layerConfig({
      url: Config.String("REDIS_URL"),
      maxConnections: Config.succeed(MAX_CONNECTIONS),
    }),
  ),
);
