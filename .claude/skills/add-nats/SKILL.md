---
name: add-nats
description: Add the NATS channel so this NanoClaw install can message agents in other installs (solutions) over the Central Agent Platform's NATS JetStream bus. Use when wiring a solution to the platform bus, adding a remote agent as a destination, or debugging cross-install agent messages.
---

# Add NATS channel (Central Agent Platform bus)

Agent-to-agent messaging between NanoClaw installs. Each install has a bus
identity (`NATS_SELF`, e.g. `life-ops.main`) and an inbox subject
`agents.<self>.inbox` in the JetStream stream `AGENTS`. Each remote agent is one
messaging group (`channel_type='nats'`, `platform_id=<remote bus identity>`);
local agents reach it with `send_message({to: "<destination name>"})`.

The adapter lives in this fork's tree (`src/channels/nats.ts`), not on the
upstream `channels` branch, so no copy step is needed.

## 1. Register the adapter

```nc:append to:src/channels/index.ts
import './nats.js';
```

## 2. Pin the client

```nc:dep manager:pnpm
@nats-io/transport-node@3.5.0
@nats-io/jetstream@3.5.0
```

## 3. Build and validate

```nc:run effect:build
pnpm run build
```
```nc:run effect:test
pnpm exec vitest run src/channels/nats.test.ts
```

## Enable

Add to `.env` (the token is in the Mac Keychain as `platform-nats-token`; never
commit it):

```bash
NATS_URL=nats://10.10.0.2:4222      # bus on the VPN; in-cluster: nats://nats.platform.svc:4222
NATS_TOKEN=<token>
NATS_SELF=life-ops.main             # this install's bus identity: letters, digits, _ - and dots
NATS_ALLOW=client-a.builder         # comma-separated remote identities allowed in, or *
NATS_MAX_HOPS=6                     # optional: agent->agent relay limit (loop guard)
```

Restart: `bash setup/lib/restart.sh`. The log shows `NATS channel connected`.
Without URL, token or self the adapter is skipped (not an error).

## Wire a remote agent

For each remote agent this install talks to (here `client-a.builder`):

```bash
ncl messaging-groups create --channel-type nats --platform-id client-a.builder --name "Client A builder"
ncl wirings create --messaging-group <mg-id> --agent-group <local-agent-group-id>
ncl members add --agent-group <local-agent-group-id> --user nats:client-a.builder
ncl destinations add --agent-group <local-agent-group-id> --name client-a-builder --messaging-group <mg-id>
```

The remote side does the same with this install's identity. Both identities must
be in each other's `NATS_ALLOW`.

## Verify

`pnpm exec tsx scripts/nats-live-test.ts` (with `NATS_URL`/`NATS_TOKEN` set) runs
a two-install round trip plus the hop-limit and allowlist checks against the
real bus.

## Troubleshooting

- `dropping message ... reason="not-allowed"`: add the sender to `NATS_ALLOW`.
- `reason="too-many-hops"`: two agents kept replying to each other; raise
  `NATS_MAX_HOPS` only if the conversation is intended.
- Messages wait in the stream for 7 days while an install is offline and are
  delivered when it reconnects.
