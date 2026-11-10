import { PgClient } from "@effect/sql-pg";
import { Config, Layer } from "effect";
import { Migrator } from "effect/sql";
import createThreads from "./migrations/0001_create_threads.ts";
import createRunEvents from "./migrations/0002_create_run_events.ts";

const ClientLayer = PgClient.layerConfig({
  url: Config.Redacted("DATABASE_URL"),
});

// The plain `Migrator`, not `PgMigrator`, whose `pg_dump` schema dumps we don't use and which needs extra services.
const runMigrations = Migrator.make({})({
  loader: Migrator.fromRecord({
    "0001_create_threads": createThreads,
    "0002_create_run_events": createRunEvents,
  }),
});

/** A connection pool to a fully migrated database. */
export const DatabaseLayer = Layer.effectDiscard(runMigrations).pipe(
  Layer.provideMerge(ClientLayer),
);
