import { Config, Data, Effect, Layer, Option, Stream } from "effect";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";
import type { HttpClientResponse } from "effect/http";
import type { AgUiSchema } from "effect/ai";
import { Agent, RunLog } from "effect-agent-harness";

/**
 * The `RunLog` on Durable Streams (https://electric.ax/docs/streams/), shared
 * between the worker that appends and the web process that streams.
 *
 * Each run is one JSON stream. Durable Streams gives an offset per *response*,
 * not per message, but the log must give every event a cursor. So each message
 * records `prev`, the stream's offset when it was appended, and an event's
 * cursor is the offset after it: the next message's `prev`, or the response's
 * `Stream-Next-Offset` for the last one. Reading from a cursor is then reading
 * the stream, from that offset.
 *
 * The appender sends `prev` as `Stream-Seq` too, which the server requires to
 * increase. A second appender to the same run (a worker that lost a race, or
 * one with a stale `prev`) gets a 409 instead of recording a wrong `prev`.
 *
 * The terminal event closes the stream, so readers learn the run is over from
 * the server, and a replayed step can't append after it.
 */

interface Message {
  /** The stream's offset before this message, or `null` for the run's first event. */
  readonly prev: string | null;
  readonly event: AgUiSchema.Event;
}

/** One response of a read. */
interface Page {
  readonly messages: ReadonlyArray<Message>;
  /** `Stream-Next-Offset`: where the next read starts. */
  readonly next: string;
  readonly upToDate: boolean;
  readonly closed: boolean;
}

/** Another append got there first: `Stream-Seq` didn't increase, or the stream is closed. */
class AppendConflict extends Data.TaggedError("AppendConflict") {}

class UnexpectedResponse extends Data.TaggedError("UnexpectedResponse")<{
  readonly status: number;
  readonly url: string;
}> {}

const toRunLogError = (cause: unknown) =>
  new RunLog.RunLogError({ cause });

/** Each event with its cursor: the offset after it. */
const toStored = (page: Page): Array<RunLog.Stored> =>
  page.messages.map((message, index) => ({
    cursor: page.messages[index + 1]?.prev ?? page.next,
    event: message.event,
  }));

/** How many times `append` re-reads the tail after losing a race. */
const APPEND_ATTEMPTS = 3;

export const RunLogStoreLayer = Layer.effect(
  RunLog.RunLog,
  Effect.gen(function* () {
    const baseUrl = yield* Config.String("DURABLE_STREAMS_URL");
    const client = yield* HttpClient.HttpClient;

    // Encoded, so no choice of ids can make two runs share a path.
    const streamUrl = (threadId: string, runId: string) =>
      `${baseUrl}/run-events/${encodeURIComponent(
        threadId,
      )}/${encodeURIComponent(runId)}`;

    const unexpected = (response: HttpClientResponse.HttpClientResponse) =>
      Effect.fail(
        new UnexpectedResponse({
          status: response.status,
          url: response.request.url,
        }),
      );

    // Creates the stream if it's missing. A 409 means it exists and is closed.
    const create = (url: string) =>
      Effect.flatMap(
        client.execute(
          HttpClientRequest.put(url).pipe(
            HttpClientRequest.setHeader("content-type", "application/json"),
          ),
        ),
        (response) =>
          [200, 201, 409].includes(response.status)
            ? Effect.void
            : unexpected(response),
      );

    // One response from `offset`, long-polling at the tail when `live`. `None` if the stream doesn't exist.
    const getPage = (url: string, offset: string, live: boolean) =>
      Effect.gen(function* () {
        const response = yield* client.execute(
          HttpClientRequest.get(url).pipe(
            HttpClientRequest.setUrlParams(
              live ? { offset, live: "long-poll" } : { offset },
            ),
          ),
        );
        if (response.status === 404) {
          return Option.none<Page>();
        }
        if (response.status === 400) {
          // Durable Streams' answer to a malformed offset.
          return yield* new RunLog.InvalidCursor({ cursor: offset });
        }
        if (response.status !== 200 && response.status !== 204) {
          return yield* unexpected(response);
        }
        const next = response.headers["stream-next-offset"];
        if (next === undefined) {
          return yield* unexpected(response);
        }
        return Option.some<Page>({
          // Written only by `append`, as a `Message`, so not decoded.
          messages:
            response.status === 200
              ? ((yield* response.json) as unknown as ReadonlyArray<Message>)
              : [],
          next,
          upToDate: response.headers["stream-up-to-date"] === "true",
          closed: response.headers["stream-closed"] === "true",
        });
      });

    /**
     * Fails unless `after` is the offset right after one of the run's events.
     * The server accepts any well-formed offset, rounding one inside a message
     * down to its start, so the check is ours: the event after a real cursor
     * was appended at it, and a cursor with nothing after it is the tail of a
     * stream with events.
     */
    const checkCursor = (url: string, after: string, first: Page) =>
      Effect.gen(function* () {
        const [head] = first.messages;
        const issued =
          head !== undefined
            ? head.prev === after
            : first.next === after &&
              Option.exists(
                yield* getPage(url, "-1", false),
                (page) => page.messages.length > 0,
              );
        if (!issued) {
          return yield* new RunLog.InvalidCursor({ cursor: after });
        }
      });

    // Everything after `after`, up to the tail. `None` if the stream doesn't exist.
    const catchUp = (url: string, after: string | undefined) =>
      Effect.gen(function* () {
        const first = yield* getPage(url, after ?? "-1", false);
        if (Option.isNone(first)) {
          return after === undefined
            ? Option.none<Page>()
            : yield* new RunLog.InvalidCursor({ cursor: after });
        }
        if (after !== undefined) {
          yield* checkCursor(url, after, first.value);
        }
        let page = first.value;
        const messages = [...page.messages];
        while (!page.upToDate && !page.closed) {
          const more = yield* getPage(url, page.next, false);
          if (Option.isNone(more)) {
            return yield* Effect.die(
              `The stream ${url} was deleted while it was read`,
            );
          }
          page = more.value;
          messages.push(...page.messages);
        }
        return Option.some<Page>({ ...page, messages });
      });

    // Where this process appends next to each run. Dropped when an append conflicts.
    const positions = new Map<
      string,
      {
        readonly prev: string;
        readonly first: boolean;
        readonly closed: boolean;
      }
    >();

    const position = (url: string) =>
      Effect.gen(function* () {
        const known = positions.get(url);
        if (known !== undefined) {
          return known;
        }
        yield* create(url);
        const tail = yield* catchUp(url, undefined);
        if (Option.isNone(tail)) {
          return yield* Effect.die(
            `The stream ${url} was deleted as it was created`,
          );
        }
        return {
          prev: tail.value.next,
          first: tail.value.messages.length === 0,
          closed: tail.value.closed,
        };
      });

    const appendOnce = (url: string, event: AgUiSchema.Event) =>
      Effect.gen(function* () {
        const { prev, first, closed } = yield* position(url);
        if (closed) {
          // A step that already appended the terminal event ran again after a
          // crash. The run is over, so drop the duplicate; its cursor is the end.
          return { cursor: prev, event };
        }
        const terminal = Agent.isTerminal(event);
        const message: Message = { prev: first ? null : prev, event };
        const response = yield* client.execute(
          HttpClientRequest.post(url).pipe(
            HttpClientRequest.setHeaders({
              "stream-seq": prev,
              ...(terminal ? { "stream-closed": "true" } : {}),
            }),
            HttpClientRequest.bodyJsonUnsafe(message),
          ),
        );
        if (response.status === 409) {
          positions.delete(url);
          return yield* new AppendConflict();
        }
        const next = response.headers["stream-next-offset"];
        if (
          (response.status !== 200 && response.status !== 204) ||
          next === undefined
        ) {
          return yield* unexpected(response);
        }
        positions.set(url, { prev: next, first: false, closed: terminal });
        return { cursor: next, event };
      });

    return RunLog.RunLog.of({
      append: (threadId, runId, event) =>
        appendOnce(streamUrl(threadId, runId), event).pipe(
          Effect.retry({
            while: (error) => error._tag === "AppendConflict",
            times: APPEND_ATTEMPTS,
          }),
          Effect.mapError(toRunLogError),
        ),

      read: (threadId, runId, after) =>
        catchUp(streamUrl(threadId, runId), after).pipe(
          Effect.map(Option.match({ onNone: () => [], onSome: toStored })),
          Effect.catchIf(
            (error) => error._tag !== "InvalidCursor",
            (error) => Effect.fail(toRunLogError(error)),
          ),
        ),

      subscribe: (threadId, runId, after) =>
        Effect.gen(function* () {
          const url = streamUrl(threadId, runId);
          let caughtUp = yield* catchUp(url, after);
          if (Option.isNone(caughtUp)) {
            // The run hasn't appended yet. Create its stream to wait on it.
            yield* create(url);
            caughtUp = yield* catchUp(url, undefined);
          }
          if (Option.isNone(caughtUp)) {
            return yield* Effect.die(
              `The stream ${url} was deleted as it was created`,
            );
          }
          const history = Stream.fromIterable(toStored(caughtUp.value));
          if (caughtUp.value.closed) {
            return history;
          }

          // Long-polls from the tail until the stream closes. An empty page is a
          // long-poll that timed out. The reference server (0.3.9) answers a
          // long-poll woken by a closing append with an empty 204, leaving out
          // the appended data, so a closed page is read again without `live`.
          const nextPage = (offset: string) =>
            Effect.flatMap(getPage(url, offset, true), (page) =>
              Option.exists(page, (page) => page.closed)
                ? getPage(url, offset, false)
                : Effect.succeed(page),
            );
          const live = Stream.paginate(caughtUp.value.next, (offset) =>
            Effect.flatMap(nextPage(offset), (page) =>
              Option.isNone(page)
                ? Effect.die(
                    `The stream ${url} was deleted while it was followed`,
                  )
                : Effect.succeed([
                    toStored(page.value),
                    page.value.closed
                      ? Option.none<string>()
                      : Option.some(page.value.next),
                  ] as const),
            ),
          );
          return Stream.concat(history, live).pipe(
            // The stream closes with its terminal event; this ends a run whose close was lost.
            Stream.takeUntil((stored) => Agent.isTerminal(stored.event)),
            Stream.mapError(toRunLogError),
          );
        }).pipe(
          Effect.catchIf(
            (error) => error._tag !== "InvalidCursor",
            (error) => Effect.fail(toRunLogError(error)),
          ),
        ),
    });
  }),
).pipe(Layer.provide(FetchHttpClient.layer));
