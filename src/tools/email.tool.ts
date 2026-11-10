import { Effect, Schema } from "effect";
import { Tool } from "effect/ai";

export const SendEmail = Tool.make("send-email", {
  description: "Send an email on the user's behalf",
  parameters: Schema.Struct({
    to: Schema.String.annotate({ description: "Recipient email address" }),
    subject: Schema.String,
    body: Schema.String,
  }),
  success: Schema.Struct({ sent: Schema.Boolean }),
  failure: Schema.Struct({ message: Schema.String }),
  failureMode: "return",
  // The run stops with an AG-UI interrupt until the user approves the call.
  needsApproval: true,
});

/** A stand-in that only prints the email: this is an example. */
export const sendEmail = ({ to, subject }: { readonly to: string; readonly subject: string }) =>
  Effect.sync(() => {
    console.log(`📧 (not really) sending "${subject}" to ${to}`);
    return { sent: true };
  });
