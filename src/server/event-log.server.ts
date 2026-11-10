import { PgClient } from "@effect/sql-pg";
import { Effect, Layer, Option, PubSub, Queue, Schedule, Stream } from "effect";
import { AgUi, EventLog } from "effect-agent-harness";
import { DatabaseLayer } from "./database.server.ts";

const CHANNEL = "run_events";

/** How often a subscriber checks for events without a notification, in case one was lost. */
const POLL_INTERVAL = "1 second";

interface Row {
  readonly cursor: string;
  // Written only by `append`, from an `AgUi.Event`.
  readonly event: AgUi.Event;
}

const toEventLogError = (cause: unknown) => new EventLog.EventLogError({ cause });

/** The `EventLog` on Postgres, shared between the worker that appends and the web process that streams. */
export const EventLogStoreLayer = Layer.effect(
  EventLog.EventLog,
  Effect.gen(function* () {
    const sql = yield* PgClient.PgClient;

    // Wakes this process's subscribers. Sliding, so a burst of events is one wake-up.
    const appended = yield* PubSub.sliding<void>(1);
    yield* sql.listen(CHANNEL).pipe(
      Effect.flatMap((notifications) =>
        Effect.forever(
          Effect.andThen(Queue.take(notifications), PubSub.publish(appended, undefined)),
        ),
      ),
      Effect.scoped,
      Effect.retry(Schedule.spaced(POLL_INTERVAL)),
      Effect.forkScoped,
    );

    const eventsAfter = (threadId: string, runId: string, after: string | undefined) =>
      sql<Row>`
        SELECT id::text AS cursor, event
        FROM run_events
        WHERE thread_id = ${threadId} AND run_id = ${runId} AND id > COALESCE(${after ?? null}::text::bigint, 0)
        ORDER BY id
      `;

    const checkCursor = (threadId: string, runId: string, cursor: string | undefined) =>
      cursor === undefined
        ? Effect.void
        : Effect.flatMap(
            sql`SELECT 1 FROM run_events WHERE thread_id = ${threadId} AND run_id = ${runId} AND id::text = ${cursor}`,
            (rows) =>
              rows.length === 0 ? Effect.fail(new EventLog.InvalidCursor({ cursor })) : Effect.void,
          );

    return EventLog.EventLog.of({
      // The notification is sent when this statement commits, so a woken reader can see the event.
      append: (threadId, runId, event) =>
        Effect.gen(function* () {
          const [row] = yield* sql<{ readonly cursor: string }>`
            WITH inserted AS (
              INSERT INTO run_events (thread_id, run_id, event)
              VALUES (${threadId}, ${runId}, ${sql.json(event)})
              RETURNING id::text AS cursor
            )
            SELECT cursor, pg_notify(${CHANNEL}, '') FROM inserted
          `;
          if (row === undefined) {
            return yield* Effect.die("INSERT returned no row");
          }
          return { cursor: row.cursor, event };
        }).pipe(Effect.mapError(toEventLogError)),

      read: (threadId, runId, after) =>
        checkCursor(threadId, runId, after).pipe(
          Effect.andThen(eventsAfter(threadId, runId, after)),
          Effect.catchTag("SqlError", (error) => Effect.fail(toEventLogError(error))),
        ),

      subscribe: (threadId, runId, after) =>
        Effect.gen(function* () {
          yield* checkCursor(threadId, runId, after);
          const [last] = yield* sql<Row>`
            SELECT id::text AS cursor, event
            FROM run_events
            WHERE thread_id = ${threadId} AND run_id = ${runId}
            ORDER BY id DESC
            LIMIT 1
          `;
          if (last !== undefined && last.cursor === after && AgUi.isTerminal(last.event)) {
            return Stream.empty;
          }

          return Stream.unwrap(
            Effect.gen(function* () {
              // Subscribed before the first read, so no event falls between the two.
              const wake = yield* PubSub.subscribe(appended);
              return Stream.paginate(after, (cursor) =>
                Effect.gen(function* () {
                  const rows = yield* eventsAfter(threadId, runId, cursor);
                  const newest = rows.at(-1);
                  if (newest === undefined) {
                    yield* Effect.timeoutOption(PubSub.take(wake), POLL_INTERVAL);
                    return [[], Option.some(cursor)] as const;
                  }
                  return [
                    rows,
                    AgUi.isTerminal(newest.event)
                      ? Option.none()
                      : Option.some<string | undefined>(newest.cursor),
                  ] as const;
                }),
              );
            }),
          ).pipe(Stream.mapError(toEventLogError));
        }).pipe(Effect.catchTag("SqlError", (error) => Effect.fail(toEventLogError(error)))),
    });
  }),
).pipe(Layer.provide(DatabaseLayer));
