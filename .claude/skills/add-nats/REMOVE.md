# Remove NATS channel

Every step is idempotent.

1. Delete `import './nats.js';` from `src/channels/index.ts`.
2. Remove the `NATS_*` lines from `.env`.
3. Delete this install's nats messaging groups (`ncl messaging-groups list`, then `delete`).
4. Rebuild and restart: `pnpm run build && bash setup/lib/restart.sh`.
5. Optional, on the bus: delete the durable consumer `inbox_<self with dots as _>` from stream `AGENTS`.

The adapter source (`src/channels/nats.ts`) stays in the fork's tree.
