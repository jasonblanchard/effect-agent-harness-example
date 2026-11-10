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
