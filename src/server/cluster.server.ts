import { NodeClusterSocket } from "@effect/platform-node";
import { Layer } from "effect";
import { ClusterWorkflowEngine } from "effect/cluster";
import { DatabaseLayer } from "./database.server.ts";

/** For the worker: hosts the cluster's shards and executes workflows. */
export const WorkflowRunnerLayer = ClusterWorkflowEngine.layer.pipe(
  Layer.provideMerge(NodeClusterSocket.layer({ storage: "sql" })),
  Layer.provide(DatabaseLayer),
);

/** For the web process: starts workflows without executing any. Starts are stored, so they wait for a worker. */
export const WorkflowClientLayer = ClusterWorkflowEngine.layer.pipe(
  Layer.provide(NodeClusterSocket.layer({ clientOnly: true, storage: "sql" })),
  Layer.provide(DatabaseLayer),
);
