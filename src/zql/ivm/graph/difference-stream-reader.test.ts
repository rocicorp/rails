import {expect, test} from 'vitest';
import {DifferenceStreamReader} from './difference-stream-reader.js';
import {NoOp} from './operators/operator.js';
import {InvariantViolation} from '../../error/asserts.js';
import {Multiset} from '../multiset.js';
import {DifferenceStreamWriter} from './difference-stream-writer.js';

test('cannot set two operators', () => {
  const r = new DifferenceStreamReader(new DifferenceStreamWriter());
  r.setOperator(new NoOp());
  expect(() => r.setOperator(new NoOp())).toThrow(InvariantViolation);
});

test('calling notify without calling run throws', () => {
  const r = new DifferenceStreamReader(new DifferenceStreamWriter());
  r.setOperator(new NoOp());
  expect(() => r.notify(1)).toThrow(InvariantViolation);
});

test('calling notify with a mismatched version throws', () => {
  const r = new DifferenceStreamReader(new DifferenceStreamWriter());
  r.setOperator(new NoOp());
  r.run(1);
  expect(() => r.notify(2)).toThrow(InvariantViolation);
});

test('run runs the operator', () => {
  let ran = false;
  const op = {
    run() {
      ran = true;
    },
    notify() {},
    notifyCommitted() {},
    destroy() {},
    messageUpstream() {},
  };
  const r = new DifferenceStreamReader(new DifferenceStreamWriter());
  r.setOperator(op);
  expect(ran).toBe(false);
  r.run(1);
  expect(ran).toBe(true);
});

test('notifyCommitted passes along to the operator', () => {
  let ran = false;
  const op = {
    run() {},
    notify() {},
    notifyCommitted() {
      ran = true;
    },
    destroy() {},
    messageUpstream() {},
  };
  const r = new DifferenceStreamReader(new DifferenceStreamWriter());
  r.setOperator(op);
  r.run(1);
  r.notify(1);
  expect(ran).toBe(false);
  r.notifyCommitted(1);
  expect(ran).toBe(true);
});

test('notify throws if the operator is missing', () => {
  const r = new DifferenceStreamReader(new DifferenceStreamWriter());
  expect(() => r.notify(1)).toThrow(InvariantViolation);
});

test('notifyCommited throws if the operator is missing', () => {
  const r = new DifferenceStreamReader(new DifferenceStreamWriter());
  try {
    r.run(1);
    r.notify(1);
  } catch (_) {
    // ignore
  }

  expect(() => r.notifyCommitted(1)).toThrow(InvariantViolation);
});

test('notifyCommitted does not notify on version mismatch', () => {
  let ran = false;
  const op = {
    run() {},
    notify() {},
    notifyCommitted() {
      ran = true;
    },
    destroy() {},
    messageUpstream() {},
  };
  const r = new DifferenceStreamReader(new DifferenceStreamWriter());
  r.setOperator(op);
  r.run(1);
  r.notify(1);
  expect(ran).toBe(false);
  r.notifyCommitted(2);
  expect(ran).toBe(false);
});

test('drain', () => {
  const r = new DifferenceStreamReader(new DifferenceStreamWriter());

  // draining empty is not an error
  expect(r.drain(1)).toEqual([]);

  // only drains up to version
  const s1 = new Multiset([[1, 1]]);
  const s2 = new Multiset([[2, 1]]);
  const s3 = new Multiset([[3, 1]]);
  r.enqueue([1, s1]);
  r.enqueue([2, s2]);
  r.enqueue([3, s3]);
  expect(r.drain(2)).toEqual([s1, s2]);

  // drain leaves the queue empty if we're draining all versions in it
  expect(r.drain(3)).toEqual([s3]);
  expect(r.isEmpty()).toBe(true);
});
