import {expect, expectTypeOf, test} from 'vitest';
import {filterIter} from './iterables.js';

test('filter', () => {
  const arr = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9];
  expect([...filterIter(arr, x => x % 2 === 0)]).toEqual([0, 2, 4, 6, 8]);
});

test('filter with is function', () => {
  const arr2 = [0, 'a', 1, true, null, 2];
  function asNumber(v: unknown): v is number {
    return typeof v === 'number';
  }
  const filtered = [...filterIter(arr2, asNumber)];
  expect(filtered).toEqual([0, 1, 2]);
  expectTypeOf(filtered).toEqualTypeOf<number[]>();
});

test('filter with index', () => {
  // Keep even indexes
  const arr3 = ['a', 'b', 'c', 'd', 'e', 'f'];
  const filtered2 = [...filterIter(arr3, (_, i) => i % 2 === 0)];
  expect(filtered2).toEqual(['a', 'c', 'e']);
});
