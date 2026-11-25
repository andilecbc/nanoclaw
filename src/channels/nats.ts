/**
 * NATS channel — agent-to-agent messaging across NanoClaw installs over a
 * NATS JetStream bus (Central Agent Platform).
 *
 * Every install has a bus identity `NATS_SELF` (e.g. `life-ops.main`). Its
 * inbox is the subject `agents.<self>.inbox`, held in the JetStream stream
 * `AGENTS` (subjects `agents.>`), read by the durable consumer
 * `inbox_<self>`. A remote agent is one messaging group whose platformId is
 * that agent's bus identity; local agents reach it with the normal
 * `send_message({to: "<destination name>"})` tool.
 *
 * Wire format (JSON body, one message per NATS message):
 *   { "v": 1, "id": "<unique id>", "from": "<sender self>", "text": "...", "hops": 0 }
 *
 * Safety:
 *  - Only senders listed in NATS_ALLOW (comma-separated bus identities, or
 *    `*`) are routed; everything else is acked and dropped with a warning.
 *  - `hops` counts agent→agent relays. A reply to a message that arrived
 *    with N hops is published with N+1; inbound messages over NATS_MAX_HOPS
 *    (default 6) are dropped, which breaks agent ping-pong loops.
 *  - Inbound messages are acked only after the router accepted them
 *    (at-least-once); failures are nak'd for redelivery. Duplicates are
 *    suppressed by message id (in-process window) and, on publish retries,
 *    by JetStream's Nats-Msg-Id dedupe (the host-wide deliveryId).
 *
 * Config (.env): NATS_URL, NATS_TOKEN, NATS_SELF, NATS_ALLOW, NATS_MAX_HOPS.
 * The factory returns null (channel skipped) when URL, token or self is missing.
 */
import { connect, type NatsConnection } from '@nats-io/transport-node';
import {
  AckPolicy,
  jetstream,
  jetstreamManager,
  type ConsumerMessages,
  type JetStreamClient,
} from '@nats-io/jetstream';

import { readEnvFile } from '../env.js';
import { log } from '../log.js';
import type { ChannelAdapter, ChannelDefaults, ChannelSetup, OutboundMessage } from './adapter.js';
import { registerChannelAdapter } from './channel-registry.js';

export const NATS_STREAM = 'AGENTS';
export const SELF_RE = /^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$/;
const DEFAULT_MAX_HOPS = 6;
const SEEN_LIMIT = 2000;

export interface BusMessage {
  v: 1;
  id: string;
  from: string;
  text: string;
  hops: number;
}

export interface NatsConfig {
  url: string;
  token: string;
  self: string;
  allow: Set<string> | '*';
  maxHops: number;
}

/**
 * Remote agents are trusted peers named in NATS_ALLOW, so every message is
 * for the agent (pattern '.'). There are no threads or mentions on the bus.
 */
const NATS_DEFAULTS: ChannelDefaults = {
  dm: { engageMode: 'pattern', engagePattern: '.', threads: false, unknownSenderPolicy: 'strict' },
  group: { engageMode: 'pattern', engagePattern: '.', threads: false, unknownSenderPolicy: 'strict' },
  mentions: 'never',
};

export function inboxSubject(self: string): string {
  return `agents.${self}.inbox`;
}

export function durableName(self: string): string {
  return `inbox_${self.replace(/\./g, '_')}`;
}

export function readNatsConfig(env: Record<string, string | undefined>): NatsConfig | null {
  const url = env.NATS_URL?.trim();
  const token = env.NATS_TOKEN?.trim();
  const self = env.NATS_SELF?.trim();
  if (!url || !token || !self) return null;
  if (!SELF_RE.test(self)) {
    log.warn('NATS: NATS_SELF is not a valid bus identity (letters, digits, _ - and dots); channel disabled', { self });
    return null;
  }
  const allowRaw = (env.NATS_ALLOW ?? '').trim();
  const allow =
    allowRaw === '*'
      ? '*'
      : new Set(
          allowRaw
            .split(',')
            .map((s) => s.trim())
            .filter(Boolean),
        );
  const maxHops = Number.parseInt(env.NATS_MAX_HOPS ?? '', 10);
  return { url, token, self, allow, maxHops: Number.isFinite(maxHops) && maxHops >= 0 ? maxHops : DEFAULT_MAX_HOPS };
}

/** Parse and validate one bus message body. Returns null for anything malformed. */
export function parseBusMessage(data: Uint8Array): BusMessage | null {
  let obj: unknown;
  try {
    obj = JSON.parse(new TextDecoder().decode(data));
  } catch {
    return null;
  }
  if (!obj || typeof obj !== 'object') return null;
  const m = obj as Record<string, unknown>;
  if (m.v !== 1 || typeof m.id !== 'string' || typeof m.from !== 'string' || typeof m.text !== 'string') return null;
  if (!SELF_RE.test(m.from)) return null;
  const hops = typeof m.hops === 'number' && Number.isInteger(m.hops) && m.hops >= 0 ? m.hops : 0;
  return { v: 1, id: m.id, from: m.from, text: m.text, hops };
}

/** Decide whether an inbound bus message may be routed. */
export function admit(cfg: NatsConfig, msg: BusMessage): 'ok' | 'self' | 'not-allowed' | 'too-many-hops' {
  if (msg.from === cfg.self) return 'self';
  if (cfg.allow !== '*' && !cfg.allow.has(msg.from)) return 'not-allowed';
  if (msg.hops > cfg.maxHops) return 'too-many-hops';
  return 'ok';
}

function isNotFound(err: unknown): boolean {
  return err instanceof Error && /not found/i.test(err.message);
}

function extractText(message: OutboundMessage): string | null {
  const content = message.content as Record<string, unknown> | string | undefined;
  if (typeof content === 'string') return content;
  if (content && typeof content === 'object' && typeof content.text === 'string') return content.text;
  return null;
}

export function createNatsAdapter(cfg: NatsConfig): ChannelAdapter {
  let nc: NatsConnection | null = null;
  let js: JetStreamClient | null = null;
  let messages: ConsumerMessages | null = null;
  // Hop count of the latest inbound message per remote, so a reply carries +1.
  const lastHops = new Map<string, number>();
  // Recently routed message ids (insertion-ordered) for redelivery dedupe.
  const seen = new Set<string>();

  const remember = (id: string) => {
    seen.add(id);
    if (seen.size > SEEN_LIMIT) seen.delete(seen.values().next().value as string);
  };

  async function consumeLoop(config: ChannelSetup): Promise<void> {
    if (!messages) return;
    for await (const m of messages) {
      const msg = parseBusMessage(m.data);
      if (!msg) {
        log.warn('NATS: dropping malformed message', { subject: m.subject });
        m.term();
        continue;
      }
      const verdict = admit(cfg, msg);
      if (verdict !== 'ok') {
        log.warn('NATS: dropping message', { from: msg.from, id: msg.id, reason: verdict });
        m.ack();
        continue;
      }
      if (seen.has(msg.id)) {
        m.ack();
        continue;
      }
      try {
        lastHops.set(msg.from, msg.hops);
        await config.onInbound(msg.from, null, {
          id: `nats-${msg.id}`,
          kind: 'chat',
          timestamp: new Date().toISOString(),
          isGroup: false,
          content: { text: msg.text, sender: msg.from, senderId: `nats:${msg.from}` },
        });
        remember(msg.id);
        m.ack();
      } catch (err) {
        log.error('NATS: onInbound failed, will be redelivered', { from: msg.from, id: msg.id, err });
        m.nak(5000);
      }
    }
  }

  const adapter: ChannelAdapter = {
    name: 'nats',
    channelType: 'nats',
    supportsThreads: false,
    defaults: NATS_DEFAULTS,

    async setup(config: ChannelSetup): Promise<void> {
      nc = await connect({
        servers: cfg.url,
        token: cfg.token,
        name: `nanoclaw:${cfg.self}`,
        maxReconnectAttempts: -1,
      });
      const jsm = await jetstreamManager(nc);
      try {
        await jsm.streams.info(NATS_STREAM);
      } catch (err) {
        if (!isNotFound(err)) throw err;
        await jsm.streams.add({ name: NATS_STREAM, subjects: ['agents.>'], max_age: 7 * 24 * 3600 * 1e9 });
        log.info('NATS: created stream', { stream: NATS_STREAM });
      }
      const durable = durableName(cfg.self);
      try {
        await jsm.consumers.info(NATS_STREAM, durable);
      } catch (err) {
        if (!isNotFound(err)) throw err;
        await jsm.consumers.add(NATS_STREAM, {
          durable_name: durable,
          filter_subject: inboxSubject(cfg.self),
          ack_policy: AckPolicy.Explicit,
          max_deliver: 10,
        });
      }
      js = jetstream(nc);
      const consumer = await js.consumers.get(NATS_STREAM, durable);
      messages = await consumer.consume();
      void consumeLoop(config).catch((err) => log.error('NATS: consume loop ended', { err }));
      log.info('NATS channel connected', { self: cfg.self, inbox: inboxSubject(cfg.self) });
    },

    async teardown(): Promise<void> {
      try {
        await messages?.close();
      } catch {
        /* already closed */
      }
      messages = null;
      if (nc) await nc.drain().catch(() => nc?.close());
      nc = null;
      js = null;
    },

    isConnected(): boolean {
      return nc !== null && !nc.isClosed();
    },

    async deliver(platformId, _threadId, message: OutboundMessage): Promise<string | undefined> {
      if (!js) throw new Error('NATS channel is not connected');
      if (!SELF_RE.test(platformId)) {
        log.warn('NATS: refusing to deliver to invalid bus identity', { platformId });
        return undefined;
      }
      const text = extractText(message);
      if (text === null) return undefined;
      const id = message.deliveryId ?? `${cfg.self}-${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
      const body: BusMessage = { v: 1, id, from: cfg.self, text, hops: (lastHops.get(platformId) ?? -1) + 1 };
      const ack = await js.publish(inboxSubject(platformId), JSON.stringify(body), { msgID: id });
      return `${ack.stream}:${ack.seq}`;
    },
  };

  return adapter;
}

registerChannelAdapter('nats', {
  factory: () => {
    const cfg = readNatsConfig(readEnvFile(['NATS_URL', 'NATS_TOKEN', 'NATS_SELF', 'NATS_ALLOW', 'NATS_MAX_HOPS']));
    return cfg ? createNatsAdapter(cfg) : null;
  },
  defaults: NATS_DEFAULTS,
});
