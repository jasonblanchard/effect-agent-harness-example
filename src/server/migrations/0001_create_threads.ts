import { Effect } from "effect";
import { SqlClient } from "effect/sql";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // One row per thread, holding its claim: the run allowed to write to it.
  yield* sql`
    CREATE TABLE threads (
      id text PRIMARY KEY,
      active_run_id text,
      claim_expires_at timestamptz,
      created_at timestamptz NOT NULL DEFAULT now(),
      CHECK ((active_run_id IS NULL) = (claim_expires_at IS NULL))
    )
  `;

  // A thread's messages. `seq` is the commit order, and the unique constraint
  // makes a repeated commit a no-op.
  yield* sql`
    CREATE TABLE thread_entries (
      seq bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      thread_id text NOT NULL REFERENCES threads (id) ON DELETE CASCADE,
      entry_id text NOT NULL,
      run_id text NOT NULL,
      message jsonb NOT NULL,
      UNIQUE (thread_id, entry_id)
    )
  `;

  yield* sql`CREATE INDEX thread_entries_thread_id_seq_idx ON thread_entries (thread_id, seq)`;
});
