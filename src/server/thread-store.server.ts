import { PgClient } from "@effect/sql-pg";
import { Effect, Layer, Schema } from "effect";
import { SqlSchema } from "effect/sql";
import { Messages, Thread } from "effect-agent-harness";
import { DatabaseLayer } from "./database.server.ts";

const EntryJson = Schema.toCodecJson(Messages.Entry);
const encodeEntries = Schema.encodeEffect(Schema.Array(EntryJson));

const toThreadError = (cause: unknown) => new Thread.ThreadError({ cause });

/** The `Thread` store on Postgres. Includes the connection pool and runs migrations when built. */
export const ThreadStoreLayer = Layer.effect(
  Thread.Thread,
  Effect.gen(function* () {
    const sql = yield* PgClient.PgClient;

    const loadEntries = SqlSchema.findAll({
      Request: Schema.String,
      Result: EntryJson,
      execute: (threadId) => sql`
        SELECT entry_id AS id, run_id AS "runId", message
        FROM thread_entries
        WHERE thread_id = ${threadId}
        ORDER BY seq
      `,
    });

    return Thread.Thread.of({
      load: (threadId) => loadEntries(threadId).pipe(Effect.mapError(toThreadError)),

      // Upserting locks the thread's row, so concurrent claims take turns.
      claim: (threadId, runId) =>
        sql
          .withTransaction(
            Effect.gen(function* () {
              const [row] = yield* sql<{ readonly holder: string | null }>`
                INSERT INTO threads (id) VALUES (${threadId})
                ON CONFLICT (id) DO UPDATE SET id = excluded.id
                RETURNING CASE WHEN claim_expires_at > now() THEN active_run_id END AS holder
              `;
              const holder = row?.holder ?? null;
              if (holder !== null && holder !== runId) {
                return yield* new Thread.ThreadBusy({ activeRunId: holder });
              }
              // Must outlast any run: nothing renews a claim while its run is going.
              yield* sql`
                UPDATE threads
                SET active_run_id = ${runId}, claim_expires_at = now() + interval '15 minutes'
                WHERE id = ${threadId}
              `;
            }),
          )
          .pipe(Effect.catchTag("SqlError", (error) => Effect.fail(toThreadError(error)))),

      activeRun: (threadId) =>
        sql<{ readonly active_run_id: string }>`
          SELECT active_run_id FROM threads WHERE id = ${threadId} AND claim_expires_at > now()
        `.pipe(
          Effect.map(([row]) => row?.active_run_id),
          Effect.mapError(toThreadError),
        ),

      release: (threadId, runId) =>
        sql`
          UPDATE threads
          SET active_run_id = NULL, claim_expires_at = NULL
          WHERE id = ${threadId} AND active_run_id = ${runId}
        `.pipe(Effect.asVoid, Effect.mapError(toThreadError)),

      commit: (threadId, runId, entries) =>
        Effect.gen(function* () {
          const encoded = yield* encodeEntries(entries);
          yield* sql`INSERT INTO threads (id) VALUES (${threadId}) ON CONFLICT (id) DO NOTHING`;
          yield* sql`
            INSERT INTO thread_entries (thread_id, entry_id, run_id, message)
            SELECT ${threadId}, entry ->> 'id', ${runId}, entry -> 'message'
            FROM jsonb_array_elements(${sql.json(encoded)}) WITH ORDINALITY AS entries (entry, position)
            ORDER BY position
            ON CONFLICT (thread_id, entry_id) DO NOTHING
          `;
        }).pipe(sql.withTransaction, Effect.mapError(toThreadError)),

      runEnded: () => Effect.void,
    });
  }),
).pipe(Layer.provide(DatabaseLayer));
