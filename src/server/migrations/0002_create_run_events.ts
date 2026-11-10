import { Effect } from "effect";
import { SqlClient } from "effect/sql";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  // Each run's AG-UI events. `id` is the event's cursor: a run has one writer at
  // a time, so ids become visible to readers in order within a run.
  yield* sql`
    CREATE TABLE run_events (
      id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
      thread_id text NOT NULL,
      run_id text NOT NULL,
      event jsonb NOT NULL
    )
  `;

  yield* sql`CREATE INDEX run_events_thread_id_run_id_id_idx ON run_events (thread_id, run_id, id)`;
});
