import {expect, test} from 'vitest';
import {DifferenceStreamReader} from './DifferenceStreamReader.js';
import {Queue} from './Queue.js';
import {NoOp} from './operators/Operator.js';
import {InvariantViolation} from '../../error/InvariantViolation.js';
import {Multiset} from '../Multiset.js';

test('cannot set two operators', () => {
  const r = new DifferenceStreamReader(new Queue());
  r.setOperator(new NoOp());
  expect(() => r.setOperator(new NoOp())).toThrow(InvariantViolation);
});

test('notify runs the operator', () => {
  let ran = false;
  const op = {
    run() {
      ran = true;
    },
    notifyCommitted() {},
  };
  const r = new DifferenceStreamReader(new Queue());
  r.setOperator(op);
  expect(ran).toBe(false);
  r.notify(1);
  expect(ran).toBe(true);
});

test('notifyCommitted passes the message to the operator', () => {
  let ran = false;
  const op = {
    run() {},
    notifyCommitted() {
      ran = true;
    },
  };
  const r = new DifferenceStreamReader(new Queue());
  r.setOperator(op);
  r.notify(1);
  expect(ran).toBe(false);
  r.notifyCommitted(1);
  expect(ran).toBe(true);
});

test('notify throws if the operator is missing', () => {
  const r = new DifferenceStreamReader(new Queue());
  expect(() => r.notify(1)).toThrow(InvariantViolation);
});

test('notifyCommited throws if the operator is missing', () => {
  const r = new DifferenceStreamReader(new Queue());
  try {
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
    notifyCommitted() {
      ran = true;
    },
  };
  const r = new DifferenceStreamReader(new Queue());
  r.setOperator(op);
  r.notify(1);
  expect(ran).toBe(false);
  r.notifyCommitted(2);
  expect(ran).toBe(false);
});

test('drain', () => {
  const q = new Queue();
  const r = new DifferenceStreamReader(q);

  // draining empty is not an error
  expect(r.drain(1)).toEqual([]);

  // only drains up to version
  const s1 = new Multiset([[1, 1]]);
  const s2 = new Multiset([[2, 1]]);
  const s3 = new Multiset([[3, 1]]);
  q.enqueue([1, s1]);
  q.enqueue([2, s2]);
  q.enqueue([3, s3]);
  expect(r.drain(2)).toEqual([s1, s2]);

  // drain leaves the queue empty if we're draining all versions in it
  expect(r.drain(3)).toEqual([s3]);
  expect(q.isEmpty()).toBe(true);
  expect(r.isEmpty()).toBe(true);
});
