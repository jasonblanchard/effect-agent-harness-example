// A local Durable Streams server for development: the protocol's Node reference
// server, file-backed so runs survive a restart. In production, point
// DURABLE_STREAMS_URL at Electric Cloud or the Caddy-based server instead.
import { DurableStreamTestServer } from "@durable-streams/server";

const port = Number(process.env.DURABLE_STREAMS_PORT ?? 4437);
const dataDir = process.env.DURABLE_STREAMS_DATA_DIR ?? ".durable-streams";

const server = new DurableStreamTestServer({ port, host: "127.0.0.1", dataDir });
await server.start();
console.log(`Durable Streams on http://127.0.0.1:${port}/v1/stream, data in ${dataDir}`);

const stop = () => void server.stop().then(() => process.exit(0));
process.on("SIGINT", stop);
process.on("SIGTERM", stop);
