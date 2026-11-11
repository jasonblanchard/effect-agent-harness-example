# effect-agent-harness suggestions

Gaps in the library's public interface, found while making `/api/chat` keep running after the client disconnects. The example now starts each run with `Runner.makeDetached` and streams the response from the `EventLog`.

Library paths are relative to `../effect-agent-harness/packages/effect-agent-harness`. Line numbers are from 2026-11-09 and will drift.

Suggestions 1 and 3 both need the runner to track its live runs, so they could share that work.

## 1. Cancel a detached run

**What we were trying to do:** keep the Stop button working after moving to detached runs. See `src/routes/index.tsx:124` and `src/server/chat.server.ts:118` here, and `src/Runner.ts:373` in the library.

**Why it's awkward:** `start` returns `void` (`src/Runner.ts:384`) and forks into a scope shared by every run (`src/Runner.ts:409`). The only way to interrupt a run is to close that scope, which interrupts all of them. So Stop now only stops the page from reading. The model keeps running, and the thread rejects new messages with `ThreadBusy` until the run ends.

**Proposal:** add a `cancel` to the detached runner. TanStack AI's connection adapters already have a `cancelRun` hook that a route could serve.

```ts
const runner = yield * Runner.makeDetached(assistant);

yield *
  router.add(
    "POST",
    "/api/chat/cancel",
    Effect.gen(function* () {
      const { threadId, runId } = yield* HttpServerRequest.schemaBodyJson(CancelInput);
      yield* runner.cancel(threadId, runId);
      return HttpServerResponse.empty({ status: 204 });
    }),
  );
```

**Trade-offs:**

- The runner has to track its live fibers, so `cancel` only reaches runs in the same process.
- Cancelling must release the thread claim, under the invariant of one active run per thread.
- Cancelling must also write a terminal event that closes any open spans, under the invariant that every emitted sequence is valid AG-UI. Otherwise subscribers wait forever.

## 2. Report a thread's active run

**What we were trying to do:** let a refreshed page rejoin a run that is still streaming. See `activeRun: null` at `src/server/chat.server.ts:98` here, and `src/Thread.ts:69`, `src/History.ts:113` and `src/internal/conversions.ts:533` in the library.

**Why it's impossible:** `Thread` can claim and release a run but can't read which run holds the claim. `History.restore` returns messages and open interrupts, with no active run. TanStack AI rejoins using `activeRun.runId` from the hydrate response, so without it a refreshed page shows only finished steps. It has no way to follow the live stream.

**Proposal:** return the active run from `History.restore`.

```ts
const { messages, interrupts, activeRun } = yield * History.restore(threadId);
// activeRun: { readonly runId: string } | undefined

return (
  yield *
  HttpServerResponse.json({
    messages: toUIMessages(messages),
    interrupts: interrupts === undefined ? null : interrupts,
    activeRun: activeRun === undefined ? null : activeRun,
  })
);
```

A route could then serve TanStack's join request from `EventLog.subscribe`.

**Trade-offs:**

- It adds a required member to `Thread`, such as `activeRun(threadId)`, so custom stores must implement it.
- A durable store could report a dead run if a crashed run's claim hasn't expired yet. The route would be waiting on a log that never ends.

**Status:** in progress in the library as the OpenSpec change `report-active-run` (`Thread.activeRun` and `History.restore(threadId, { rejoin: true })`). The example now uses it.

## 3. Start a detached run and stream it in one call

**What we were trying to do:** start a run and stream its events from the same route. See `src/server/chat.server.ts:118-120` here, and `src/Runner.ts:366` and `src/Runner.ts:384` in the library.

**Why it's awkward:** every route that should survive disconnects repeats the same `start` then `subscribe` sequence, plus an `Effect.orDie` for an `InvalidCursor` error that can't happen without a cursor. The docs also say not to start the same run twice concurrently, and the library doesn't enforce that. A retried POST with the same `runId` would execute the run twice.

**Proposal:** add one call that starts the run, or joins it if it's already live.

```ts
const input = yield * HttpServerRequest.schemaBodyJson(AgUi.RunAgentInput);
return yield * runner.stream(input).pipe(Sse.encodeStored, Sse.response);
```

**Trade-offs:**

- Joining needs the same per-process tracking as suggestion 1.
- It's only safe within one process. Across several, `makeWorkflow` remains the option, since its engine deduplicates runs by thread id and run id.

## 4. Renew a thread's claim while its run is going

**What we were trying to do:** give the Postgres `Thread` store a claim expiry that frees crashed runs without taking a thread from a live one. See `claim` in `src/server/thread-store.server.ts` here, and the claim docs in `src/Thread.ts` (`claim` is at line 69) in the library.

**Why it's awkward:** nothing tells a store that a run is still alive, so the expiry is a guess that has to be "longer than any run takes". The example uses 15 minutes. Too long keeps a crashed run's thread locked, and too short lets a second run take over a live one.

**Proposal:** add a store member that the runner calls during a run, for example at each step.

```ts
renew: (threadId, runId) =>
  sql`
    UPDATE threads SET claim_expires_at = now() + interval '2 minutes'
    WHERE id = ${threadId} AND active_run_id = ${runId}
  `.pipe(Effect.asVoid, Effect.mapError(toThreadError)),
```

**Trade-offs:**

- It's another required store member, or an optional one the runner has to check for.
- Renewing per step means the expiry must still outlast the slowest model or tool step, unless renewal runs on a timer.

## 5. A shared test suite for custom `Thread` stores

**What we were trying to do:** check that the Postgres store honours the `Thread` contract. See `src/server/thread-store.server.ts` here, and `test/Thread.test.ts` in the library.

**Why it's awkward:** the library's tests only exercise `layerMemory`, so every custom store has to rewrite those guarantees by hand. Concurrent claims, commit order and idempotent commits are easy to get wrong and to leave untested.

**Proposal:** export the contract as a reusable suite.

```ts
import { ThreadContract } from "effect-agent-harness/testing";

ThreadContract.run("Postgres", ThreadStoreLayer);
```

**Trade-offs:**

- It adds a test entry point to maintain, and that entry point must not pull test dependencies into the main package.
- Claim expiry can't be covered generically, because each store controls its own clock.

## 6. Tell clients when a re-run step replaces its partial output

**What we were trying to do:** keep the stream readable when the worker dies mid-reply and the workflow resumes. See `src/server/worker.server.ts` here, and `src/Agent.ts:359` and `src/Runner.ts:346` in the library.

**Why it's awkward:** the step that was interrupted runs again, and it mints its message id inside that step. `AgUi.closeOpen` ends the partial message and the re-run starts a new one with a different id. A client following the stream can't tell the old text was replaced, so it shows the partial text and then the regenerated reply. In the test, a worker killed mid-reply produced two text messages on the live stream and one stored message. A page reload shows only the final reply.

**Proposal:** memoise the id before the model call, so a re-run reuses it, and have a resumed run tell clients to discard that message's content.

```ts
// Emitted by the library when a resumed run re-runs a step.
{ type: "CUSTOM", name: "effect-agent-harness/message-reset", value: { messageId } }
```

**Trade-offs:**

- Clients need a handler for the event, and AG-UI has no standard one for this. Without a handler they see the same result as today.
- Memoising the id adds a checkpoint per step.
- The new sequence must still be valid AG-UI, under the invariant that every emitted sequence is. A message id reused after its `TEXT_MESSAGE_END` needs checking against `@ag-ui/client`'s sequence checks.

## 7. Make a workflow's history explicit

**What we were trying to do:** run the agent on a workflow with the transcript stored on the server. See `History.layerServer` at `src/server/worker.server.ts:37` here, and `src/Runner.ts:165` and `src/Runner.ts:473` in the library.

**Why it's awkward:** `History` is read as an optional service, so it isn't part of the type of the workflow's `layer`. A worker that forgets `History.layerServer` typechecks and quietly falls back to the client history. Nothing is stored, and hydrate returns an empty thread.

**Proposal:** take the history as an option of `makeWorkflow`.

```ts
export const AssistantWorkflow = Runner.makeWorkflow(assistant, {
  name: "assistant",
  history: History.layerServer,
});
```

**Trade-offs:**

- It's one more option on a runner where the client history is rarely wanted. Making it required would break existing callers.
- The library already says a workflow's history can't vary per run, so an option matches how it behaves. `makeAttached` and `makeDetached` would still read theirs from context, which makes the runners less uniform.

## Durable Streams event log

Suggestions 8 to 11 come from storing the `EventLog` on [Durable Streams](https://electric.ax/docs/streams/) (Electric Streams), on the `electric-streams` branch. The store is `src/server/event-log.server.ts`. Line numbers are from 2026-11-10.

## 8. Derive a store's cursors from an offset log

**What we were trying to do:** store each run's AG-UI events in one Durable Stream, and serve `read` and `subscribe` from it. See `toStored` and `checkCursor` at `src/server/event-log.server.ts:51` and `:127` here, and the contract at `src/EventLog.ts:7` in the library.

**Why it's awkward:** `EventLog` requires a cursor for every event, but append-only logs, including Durable Streams, Kafka-style logs and S2, give one offset per response, not per message. To bridge the gap, the store:

- records `prev`, the stream's offset when it was appended, in each message. An event's cursor is the next message's `prev`, or the response's `Stream-Next-Offset` for the last event in the response.
- sends `prev` as `Stream-Seq`, which works as compare-and-set, so a second writer can't record a wrong `prev`.
- checks cursors itself, because the server accepts any well-formed offset, including one inside a message. At the tail, the check needs an extra read to tell a real cursor from the stream's initial offset.
- builds `subscribe` from a catch-up read and a long-poll loop that ends on close or on the terminal event.

That is about two thirds of the file, and none of it is specific to this app. The README invites consumers to bring their own durable stream, so each of them would write it again.

**Proposal:** a builder that derives the whole `EventLog` from a few offset-log operations.

```ts
export const EventLogStoreLayer = Layer.effect(
  EventLog.EventLog,
  EventLog.fromOffsetLog({
    // `prev` is the offset before this append, to store with the event and to guard concurrent writers.
    append: (key, message, { prev, terminal }) => Effect<{ next: string }, EventLogError>,
    // One page from `offset`, waiting at the tail when `live`. `None` if the run has no log.
    readFrom: (key, offset, { live }) =>
      Effect<
        Option<{ messages: ReadonlyArray<Message>; next: string; closed: boolean }>,
        EventLogError
      >,
    create: (key) => Effect<void, EventLogError>,
  }),
);
```

The library would then own cursor derivation and validation, history-then-live delivery, and completion on the terminal event.

**Trade-offs:**

- The library commits to one scheme (`prev` in each message) and to a message envelope that stores must persist.
- A store with no compare-and-set like `Stream-Seq` can't guard concurrent writers this way. The builder would have to say so.
- The `EventLog` interface doesn't change, so `layerMemory` and the Postgres store are unaffected.

## 9. A shared test suite for custom `EventLog` stores

**What we were trying to do:** check that the Durable Streams store keeps the `EventLog` contract. The library's only tests are in `test/EventLog.test.ts`, and they're written against `layerMemory`.

**Why it's awkward:** we ported them by hand into a throwaway script. Some assume decimal cursors (`"0"`, `"2"`), and all share fixed ids (`t1`/`r1`), which a persistent store can't reuse between runs. The README states the invariants (`README.md:741`), but a store has nothing executable to check itself against. This is the `EventLog` side of suggestion 5.

**Proposal:** export the suite, parameterised by the store's layer.

```ts
import { eventLogConformance } from "effect-agent-harness/testing";

eventLogConformance(EventLogStoreLayer, {
  // Fresh ids per test, for stores that persist between runs.
  freshRun: () => ({ threadId: crypto.randomUUID(), runId: "r1" }),
});
```

It should:

- compare cursors only by equality and by append order, never by format.
- include well-formed cursors that were never issued.
- include a second writer racing the first, which our port added.

**Trade-offs:**

- It adds a public entry point. It would also tie the library to `@effect/vitest`, unless the cases are exported as plain effects for the consumer to register.
- Changes to the suite become semver-relevant.

## 10. Export a schema for the `AgUi.Event` union

**What we were trying to do:** read stored events back from Durable Streams. See `src/server/event-log.server.ts:108` here, and `src/AgUi.ts:130` in the library.

**Why it's awkward:** `AgUiSchema` has a schema for each event but none for the union, and `AgUi.Event` is only a type. So a store can't decode what it reads back, and casts (`as unknown as ReadonlyArray<Message>`). The Postgres store casts too. Data from an external HTTP service is where validation matters most.

**Proposal:** generate the union alongside its members.

```ts
const Message = Schema.Struct({ prev: Schema.NullOr(Schema.String), event: AgUiSchema.Event });
const messages = yield * Schema.decodeUnknownEffect(Schema.Array(Message))(yield * response.json);
```

**Trade-offs:**

- `AgUiSchema.ts` is generated, so this is a change to `scripts/generate-schema.ts`.
- Decoding costs some CPU on every read. Stores that trust their own writes can keep skipping it.

## 11. Make appends idempotent across step replays

**What we were trying to do:** behave correctly when a worker crashes mid-run and another replays the step. See `src/server/event-log.server.ts:193` here, and `src/EventLog.ts:77` in the library.

**Why it's awkward:** two behaviours the store relies on are visible only in the source:

- A replayed step appends its events again, so the Postgres store stores duplicates.
- Nothing is appended after the terminal event. Only a comment inside `layerMemory` says so, not the `EventLog` interface.

Relying on the second, our store closes the stream with the terminal event and drops later appends. That removes duplicates only for the run's final step. For earlier steps, the store can't tell a replayed event from a new one.

**Proposal:** document both behaviours on `EventLog.append`, and give each append a key that stays the same across replays.

```ts
readonly append: (
  threadId: string,
  runId: string,
  event: AgUi.Event,
  options: { readonly key: string }, // e.g. `${stepIndex}:${eventIndex}`, the same on replay
) => Effect.Effect<Stored, EventLogError>;
```

A Durable Streams store could map the key onto its idempotent producer (`Producer-Id`, `Producer-Epoch` and `Producer-Seq`). A SQL store could use a unique index. Either way, every step would get exactly-once appends.

**Trade-offs:**

- It widens `append`. Making the key optional keeps existing stores compiling, and they can ignore it.
- The `Runner` must derive keys that are deterministic across replays, which depends on how it numbers steps and the events within them.

## Not the library's

- **`@durable-streams/server` 0.3.9:** a long-poll woken by an append that also closes the stream (`Stream-Closed: true` with a body) answers `204` with `Stream-Closed: true`, an unchanged `Stream-Next-Offset`, and no data. The protocol says the response should carry the appended data. This is worth reporting upstream. Workaround: `src/server/event-log.server.ts:257`.
- **pnpm 12:** it refuses to install `@durable-streams/server` until the build scripts of `lmdb` and `msgpackr-extract` are allowed or denied. Their prebuilt binaries work, so `pnpm-workspace.yaml` denies the scripts with `allowBuilds`.
