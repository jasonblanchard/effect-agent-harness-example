import { createFileRoute } from "@tanstack/react-router";
import { fetchServerSentEvents, useChat, type UIMessage } from "@tanstack/ai-react";
import { Schema } from "effect";
import { useEffect, useState, type FormEvent } from "react";
import { getBrowserTime } from "../tools/browser-time.tool.ts";

export const Route = createFileRoute("/")({
  // The thread id lives in the URL (`/?thread=…`), so a reload continues the thread.
  validateSearch: Schema.toStandardSchemaV1(
    Schema.Struct({ thread: Schema.optional(Schema.String) }),
  ),
  component: Chat,
});

function Chat() {
  const { thread } = Route.useSearch();
  const navigate = Route.useNavigate();
  // A page without a thread id starts a new thread.
  useEffect(() => {
    if (thread === undefined) {
      void navigate({ search: { thread: crypto.randomUUID() }, replace: true });
    }
  }, [thread, navigate]);
  return thread === undefined ? null : <ChatThread key={thread} threadId={thread} />;
}

function ChatThread({ threadId }: { readonly threadId: string }) {
  // The server owns the thread's history. With `persistence: true`, the client
  // loads it on mount (`GET /api/chat?threadId=…`), open approvals included,
  // rejoins a run still going, and still sends its whole history on every run. Don't set `history`:
  // paging makes the client send only new messages, which the server rejects.
  // `tools` are frontend tools: the browser runs them and answers in the next run.
  const { messages, sendMessage, isLoading, error, stop, interrupts, resumeInterruptsUnsafe } =
    useChat({
      // The URL carries the thread id because the request to join a run doesn't add it.
      // With `cancelUrl`, `stop()` also posts the run's ids there, which cancels the
      // workflow run; aborting the stream alone wouldn't reach the worker.
      connection: fetchServerSentEvents(`/api/chat?threadId=${encodeURIComponent(threadId)}`, {
        cancelUrl: "/api/chat/cancel",
      }),
      threadId,
      persistence: true,
      tools: [getBrowserTime],
    });
  const [input, setInput] = useState("");

  // effect-agent-harness's approval interrupts are plain AG-UI interrupts, without TanStack's
  // own binding metadata, so TanStack lists them as `unbound` and offers no
  // `resolveInterrupt`. Answer them with AG-UI `resume` entries instead: the
  // payload is effect-agent-harness's `Agent.ApprovalResponse`.
  const approvals = interrupts.filter((interrupt) => interrupt.reason === "tool-approval");
  const [decisions, setDecisions] = useState<Record<string, boolean>>({});
  const decide = (interruptId: string, approved: boolean) => {
    const next = { ...decisions, [interruptId]: approved };
    // The server rejects a run that leaves an approval unanswered, so resume
    // only once every pending approval has a decision.
    if (approvals.every((approval) => approval.id in next)) {
      setDecisions({});
      void resumeInterruptsUnsafe(
        approvals.map((approval) => ({
          interruptId: approval.id,
          status: "resolved" as const,
          payload: { approved: next[approval.id] === true },
        })),
      );
    } else {
      setDecisions(next);
    }
  };

  const submit = (event: FormEvent) => {
    event.preventDefault();
    const text = input.trim();
    if (!text) return;
    setInput("");
    void sendMessage(text);
  };

  return (
    <main className="chat">
      <header>
        <h1>effect-agent-harness × TanStack AI</h1>
        <p>
          Ask about the weather anywhere, what time it is where you are (your browser answers), or
          ask it to send an email (it asks you first).
        </p>
      </header>
      <ol className="messages">
        {messages.map((message) => (
          <Message key={message.id} message={message} />
        ))}
        {isLoading && <li className="thinking">…</li>}
      </ol>
      {!isLoading &&
        approvals.map((approval) => (
          <div key={approval.id} className="approval">
            <span>{approval.message}</span>
            {approval.id in decisions ? (
              <span>{decisions[approval.id] ? "Approved" : "Denied"}</span>
            ) : (
              <>
                <button type="button" onClick={() => decide(approval.id, true)}>
                  Approve
                </button>
                <button
                  type="button"
                  className="secondary"
                  onClick={() => decide(approval.id, false)}
                >
                  Deny
                </button>
              </>
            )}
          </div>
        ))}
      {error && <p className="error">{error.message}</p>}
      <form onSubmit={submit}>
        <input
          value={input}
          onChange={(event) => setInput(event.target.value)}
          placeholder={
            approvals.length > 0 ? "Answer the approval first" : "What's the weather in Paris?"
          }
          disabled={approvals.length > 0}
          aria-label="Message"
          autoFocus
        />
        {/* The thread is busy until the run ends, so a streaming run offers Stop instead of Send. */}
        {isLoading ? (
          <button type="button" className="secondary" onClick={stop}>
            Stop
          </button>
        ) : (
          <button type="submit">Send</button>
        )}
      </form>
    </main>
  );
}

function Message({ message }: { readonly message: UIMessage }) {
  // The client adds an empty assistant message when a run starts.
  if (message.parts.length === 0) return null;
  return (
    <li className={`message ${message.role}`}>
      {message.parts.map((part, index) => {
        switch (part.type) {
          case "text":
            return <p key={index}>{part.content}</p>;
          case "thinking":
            return (
              <details key={index} className="tool">
                <summary>💭 Reasoning</summary>
                <p>{part.content}</p>
              </details>
            );
          case "tool-call":
            return (
              <details key={part.id} className="tool">
                <summary>
                  🔧 {part.name} {part.arguments}
                </summary>
                {part.output !== undefined && <pre>{JSON.stringify(part.output, null, 2)}</pre>}
              </details>
            );
          default:
            return null;
        }
      })}
    </li>
  );
}
