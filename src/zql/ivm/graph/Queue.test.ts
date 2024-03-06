import {expect, test} from 'vitest';
import {Queue} from './Queue.js';
import {Multiset} from '../Multiset.js';
import {InvariantViolation} from '../../error/InvariantViolation.js';

test('Rejects data from the past', () => {
  const q = new Queue();
  q.enqueue([1, new Multiset([])]);
  expect(() => q.enqueue([1, new Multiset([])])).toThrow(InvariantViolation);
  expect(() => q.enqueue([0, new Multiset([])])).toThrow(InvariantViolation);
  expect(() => q.enqueue([-1, new Multiset([])])).toThrow(InvariantViolation);
});

test('isEmpty', () => {
  const q = new Queue();
  expect(q.isEmpty()).toBe(true);

  q.enqueue([1, new Multiset([])]);
  expect(q.isEmpty()).toBe(false);

  q.dequeue();
  expect(q.isEmpty()).toBe(true);
});

test('enqueue/dequeue', () => {
  const q = new Queue();
  const s1 = [1, new Multiset([])] as const;
  const s2 = [2, new Multiset([])] as const;

  q.enqueue(s1);
  expect(q.peek()).toBe(s1);
  q.enqueue(s2);
  expect(q.peek()).toBe(s1);

  expect(q.dequeue()).toBe(s1);
  expect(q.peek()).toBe(s2);

  expect(q.dequeue()).toBe(s2);
  expect(q.peek()).toBe(null);

  expect(q.dequeue()).toBe(null);
});
