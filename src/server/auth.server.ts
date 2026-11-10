import { Effect, Schema } from "effect";
import { HttpRouter } from "effect/http";
import { Runner } from "effect-agent-harness";

/**
 * The signed-in user of a run. It's a `RunContext` because the workflow persists
 * it with the run, so a worker that resumes the run still knows who started it.
 */
export class CurrentUser extends Runner.RunContext<CurrentUser>()("app/CurrentUser", {
  schema: Schema.Struct({ id: Schema.String, email: Schema.String }),
}) {}

/** Fake authentication: it checks nothing, and every request is this user. */
const authenticate = Effect.succeed({ id: "user-1", email: "demo@example.com" });

/** Provides `CurrentUser` to each request of the routes it's applied to. */
export const AuthMiddlewareLayer = HttpRouter.middleware<{ provides: CurrentUser }>()(
  (httpEffect) =>
    Effect.flatMap(authenticate, (user) => Effect.provideService(httpEffect, CurrentUser, user)),
).layer;
