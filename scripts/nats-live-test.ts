/**
 * Live round-trip test of the NATS channel against a real bus.
 * Usage: NATS_URL=nats://127.0.0.1:14222 NATS_TOKEN=... pnpm exec tsx scripts/nats-live-test.ts
 * Creates throwaway durables inbox_test_a / inbox_test_b and removes them afterwards.
 */
import { connect } from '@nats-io/transport-node';
import { jetstreamManager } from '@nats-io/jetstream';

import type { ChannelSetup, InboundMessage } from '../src/channels/adapter.js';
import { createNatsAdapter, durableName, NATS_STREAM, readNatsConfig } from '../src/channels/nats.js';

const url = process.env.NATS_URL!;
const token = process.env.NATS_TOKEN!;

function setupRecorder(received: Array<{ from: string; msg: InboundMessage }>): ChannelSetup {
  return {
    onInbound: (platformId, _t, msg) => {
      received.push({ from: platformId, msg });
    },
    onInboundEvent: () => {},
    onMetadata: () => {},
    onAction: () => {},
  } as ChannelSetup;
}

const wait = (ms: number) => new Promise((r) => setTimeout(r, ms));
async function until(cond: () => boolean, ms = 5000) {
  const t = Date.now();
  while (!cond() && Date.now() - t < ms) await wait(50);
  return cond();
}

const atA: Array<{ from: string; msg: InboundMessage }> = [];
const atB: Array<{ from: string; msg: InboundMessage }> = [];
const a = createNatsAdapter(
  readNatsConfig({ NATS_URL: url, NATS_TOKEN: token, NATS_SELF: 'test.a', NATS_ALLOW: 'test.b', NATS_MAX_HOPS: '1' })!,
);
const b = createNatsAdapter(
  readNatsConfig({ NATS_URL: url, NATS_TOKEN: token, NATS_SELF: 'test.b', NATS_ALLOW: 'test.a', NATS_MAX_HOPS: '1' })!,
);
let ok = true;
const check = (name: string, pass: boolean) => {
  console.log(`${pass ? 'PASS' : 'FAIL'}  ${name}`);
  ok &&= pass;
};

try {
  await a.setup(setupRecorder(atA));
  await b.setup(setupRecorder(atB));

  await a.deliver('test.b', null, { kind: 'chat', content: { text: 'hello from A' }, deliveryId: `t-${Date.now()}-1` });
  check('A -> B delivered', await until(() => atB.length === 1));
  const inB = atB[0]?.msg.content as { text: string; senderId: string };
  check('B sees text and sender nats:test.a', inB?.text === 'hello from A' && inB?.senderId === 'nats:test.a');

  await b.deliver('test.a', null, { kind: 'chat', content: { text: 'reply from B' }, deliveryId: `t-${Date.now()}-2` });
  check('B -> A reply delivered', await until(() => atA.length === 1));

  // A replies again: hops becomes 2 > NATS_MAX_HOPS=1 at B -> dropped (loop guard).
  await a.deliver('test.b', null, { kind: 'chat', content: { text: 'loop?' }, deliveryId: `t-${Date.now()}-3` });
  await wait(1500);
  check('third relay dropped by hop limit', atB.length === 1);

  const nc = await connect({ servers: url, token });
  const raw = JSON.stringify({ v: 1, id: 'stranger-1', from: 'evil.x', text: 'hi', hops: 0 });
  await nc.publish('agents.test.b.inbox', raw);
  await wait(1500);
  check('sender not in NATS_ALLOW dropped', atB.length === 1);

  await a.teardown();
  await b.teardown();
  const jsm = await jetstreamManager(nc);
  for (const self of ['test.a', 'test.b']) await jsm.consumers.delete(NATS_STREAM, durableName(self)).catch(() => {});
  await nc.drain();
} catch (err) {
  console.error('ERROR', err);
  ok = false;
}
console.log(ok ? 'ALL PASSED' : 'FAILURES');
process.exit(ok ? 0 : 1);
