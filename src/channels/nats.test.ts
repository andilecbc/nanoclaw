import { describe, expect, it } from 'vitest';

import { admit, durableName, inboxSubject, parseBusMessage, readNatsConfig } from './nats.js';
import './index.js';
import { getRegisteredChannelNames } from './channel-registry.js';

const enc = (o: unknown) => new TextEncoder().encode(JSON.stringify(o));

describe('nats channel', () => {
  it('registers through the real barrel', () => {
    expect(getRegisteredChannelNames()).toContain('nats');
  });

  it('builds subjects and durable names from the bus identity', () => {
    expect(inboxSubject('life-ops.main')).toBe('agents.life-ops.main.inbox');
    expect(durableName('life-ops.main')).toBe('inbox_life-ops_main');
  });

  it('returns null config when required keys are missing or self is invalid', () => {
    expect(readNatsConfig({ NATS_URL: 'nats://x', NATS_TOKEN: 't' })).toBeNull();
    expect(readNatsConfig({ NATS_URL: 'nats://x', NATS_TOKEN: 't', NATS_SELF: 'bad subject' })).toBeNull();
    const cfg = readNatsConfig({
      NATS_URL: 'nats://x',
      NATS_TOKEN: 't',
      NATS_SELF: 'a.main',
      NATS_ALLOW: 'b.main, c.main',
    });
    expect(cfg?.maxHops).toBe(6);
    expect(cfg?.allow).toEqual(new Set(['b.main', 'c.main']));
  });

  it('parses valid bus messages and rejects malformed ones', () => {
    expect(parseBusMessage(enc({ v: 1, id: '1', from: 'b.main', text: 'hi', hops: 2 }))).toEqual({
      v: 1,
      id: '1',
      from: 'b.main',
      text: 'hi',
      hops: 2,
    });
    expect(parseBusMessage(new TextEncoder().encode('not json'))).toBeNull();
    expect(parseBusMessage(enc({ v: 2, id: '1', from: 'b', text: 'x' }))).toBeNull();
    expect(parseBusMessage(enc({ v: 1, id: '1', from: 'has space', text: 'x' }))).toBeNull();
  });

  it('admits only allowed senders within the hop limit', () => {
    const cfg = readNatsConfig({
      NATS_URL: 'u',
      NATS_TOKEN: 't',
      NATS_SELF: 'a.main',
      NATS_ALLOW: 'b.main',
      NATS_MAX_HOPS: '2',
    })!;
    const m = (from: string, hops: number) => ({ v: 1 as const, id: 'x', from, text: 't', hops });
    expect(admit(cfg, m('b.main', 0))).toBe('ok');
    expect(admit(cfg, m('a.main', 0))).toBe('self');
    expect(admit(cfg, m('z.main', 0))).toBe('not-allowed');
    expect(admit(cfg, m('b.main', 3))).toBe('too-many-hops');
  });
});
