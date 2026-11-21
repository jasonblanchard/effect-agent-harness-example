import { Effect, Layer, Schema } from "effect";
import { HttpApiMiddleware } from "effect/http-api";
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

/**
 * Provides `CurrentUser` to every endpoint it's attached to. A real one would
 * declare a `security` scheme and an `Unauthorized` error, as the library's ag-ui
 * example does.
 */
export class Authentication extends HttpApiMiddleware.Service<
  Authentication,
  { provides: CurrentUser; requires: never }
>()("app/Authentication") {}

export const AuthenticationLayer = Layer.succeed(
  Authentication,
  Authentication.of((httpEffect) =>
    Effect.flatMap(authenticate, (user) => Effect.provideService(httpEffect, CurrentUser, user)),
  ),
);
