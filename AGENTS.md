# effect-agent-harness-example

An example app whose purpose is to test and develop [`effect-agent-harness`](../effect-agent-harness/packages/effect-agent-harness), an Effect-based AG-UI agent library. It is a browser chat (TanStack Start + TanStack AI) on an `effect-agent-harness` backend, calling Anthropic.

The app is a means to an end. What matters most is what building it teaches us about the library's public interface.

## The linked library

`effect-agent-harness` is linked from the sibling checkout at `../effect-agent-harness/packages/effect-agent-harness`, through `link:` entries in `package.json` and `pnpm-workspace.yaml`. Its `exports` point at `src/index.ts`, so library edits show up here without a build.

- **Public interface:** `src/index.ts` in the library, which re-exports `AgUi`, `AgUiSchema`, `Messages`, `Agent`, `Sse`, `Thread`, `History`, `Checkpoint`, `EventLog` and `Runner` as namespaces. Anything else, including `src/internal/`, is private. Don't import it from here.
- **Library docs:** read its `README.md` and `AGENTS.md` (at both the monorepo root and the package) before relying on behaviour. They record the invariants the library promises.
- **One copy of `effect`:** an override in `pnpm-workspace.yaml` points this app at the library's installed `effect`. Keep it: a workflow built by one copy can't be run by an engine from another, and `Schema` fails across copies too. `@effect/platform-node-shared` is pinned for the same reason, because its newest release needs a newer `effect`.

## Inspect the source, then give feedback

Read the library source carefully before using any part of it. Don't guess an API from its name or from memory, because the library is young and changing. Check the actual signatures, the module's doc comments, and its tests in the library's `test/` directory.

If something is impossible or awkward through the public interface, say so and suggest how the library could make it possible or better. This feedback is a primary output of work in this repo, so raise it even when a workaround exists and even when nobody asked.

Signs of a gap worth reporting:

- A cast, a re-declared type, or a hand-written adapter at the boundary with the library.
- Boilerplate that every consumer of the library would have to repeat.
- A needed capability that exists only in `src/internal/`.
- Behaviour that only the library's source reveals, and its types or docs don't.
- A comment in this repo explaining why the code has to work around something.

For each suggestion, give:

1. What we were trying to do, and where (`file:line` here and in the library).
2. Why the current interface makes it impossible or awkward.
3. A concrete proposal, with the API sketched as code that a consumer would write.
4. Trade-offs, including any conflict with the invariants in the library's `AGENTS.md`.

First confirm the problem belongs to the library. Friction can also come from TanStack AI, `@effect/ai-anthropic` or Effect itself, and those should be reported as such.

**Don't change the library from this repo unless asked.** Propose the change and let the user decide. If you are asked to change it, follow the library's own `AGENTS.md`: it has its own commands, conventions and OpenSpec process, and it is a colocated jj + git repo.

When a workaround is unavoidable, keep it small and add a comment saying what the library is missing.

## Layout

- `src/server/assistant.server.ts` — the agent, its tool handlers and its workflow, shared by both processes.
- `src/server/chat.server.ts` — the web process's `/api/chat` routes. They start workflows and stream their event logs.
- `src/server/worker.server.ts` — the worker process, which executes runs and holds the model layer.
- `src/server/auth.server.ts` — fake authentication: the `CurrentUser` run context and the middleware that provides it to each request.
- `src/server/cluster.server.ts` — the workflow engine, as a runner for the worker and a client for the web process.
- `src/server/event-log.server.ts` — the library's `EventLog` store, on Postgres.
- `src/server/thread-store.server.ts` — the library's `Thread` store, on Postgres.
- `src/server/database.server.ts` — the connection pool and the list of migrations, which run when the layer is built.
- `src/server/migrations/` — one file per migration. Register new ones in `database.server.ts`.
- `src/routes/api/chat.ts` — the TanStack Start server route, which only forwards to the handler above.
- `src/routes/index.tsx` — the chat page.
- `src/tools/` — tool definitions and handlers. `browser-time.tool.ts` is the browser-side one.
- `src/routeTree.gen.ts` — generated. Don't edit it.

## Commands

Use pnpm.

| Command       | What it does                            |
| ------------- | --------------------------------------- |
| `pnpm dev`    | Dev server on port 3001, loading `.env` |
| `pnpm worker` | Workflow worker, loading `.env`         |
| `pnpm build`  | Production build                        |
| `pnpm tsc`    | Typecheck                               |

`devenv up` runs the web process, the worker and Postgres together. It's defined in `flake.nix` and available in the shell that direnv loads. Postgres listens on `127.0.0.1:5432` with a database named `effect_agent_harness_example`. A run only makes progress while a worker is running, and it resumes from Postgres after one restarts.

`pnpm lint` works. `pnpm format` and `pnpm format:check` currently fail because their `-c` flag has no path, so run `pnpm exec oxfmt -c .oxfmtrc.json` instead.

The server needs `ANTHROPIC_API_KEY` in `.env`, and `DATABASE_URL`, which the flake's shell sets. Don't print or commit `.env`.

## Conventions

- Follow the library's Effect conventions. This is Effect v4, and the authoritative guide is `node_modules/effect/CLAUDE.md` plus its `ai-docs/`. Prefer those over memory and over docs written for Effect v3.
- `effect` and `@effect/ai-anthropic` are pinned to the same version as the library's `effect`. Keep them in step.
- Relative imports use the `.ts` extension. Type-only imports use `import type`.
- Tools should normally use `failureMode: "return"`, so failures go back to the model instead of ending the run.
