import {expect, test} from 'vitest';
import {Materialite} from '../materialite.js';
import fc from 'fast-check';

const numberComparator = (l: number, r: number) => l - r;

test('add', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newSetSource(numberComparator);

      arr.forEach(x => source.add(x));
      expect([...source.value]).toEqual(arr.sort(numberComparator));
    }),
  );
});

test('delete', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newSetSource(numberComparator);

      arr.forEach(x => source.add(x));
      arr.forEach(x => source.delete(x));
      expect([...source.value]).toEqual([]);
    }),
  );
});

test('on', () => {
  const m = new Materialite();
  const source = m.newSetSource(numberComparator);

  let callCount = 0;
  const dispose = source.on(value => {
    expect(value).toEqual(source.value);
    ++callCount;

    expect([...value]).toEqual([2]);
  });
  m.tx(() => {
    source.add(1);
    source.add(2);
    source.delete(1);
  });

  // only called at the end of a transaction.
  expect(callCount).toBe(1);

  dispose();

  m.tx(() => {
    source.add(3);
  });

  // not notified if the listener is removed
  expect(callCount).toBe(1);

  // TODO: don't notify if the value didn't change?
  // We could track this in the source by checking if add events returned false
});

test('replace', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newSetSource(numberComparator);

      m.tx(() => {
        arr.forEach(x => source.add(x));
      });

      m.tx(() => {
        arr.forEach(x => {
          // We have special affordances for deletes immediately followed by adds
          // As those are really replaces.
          // Check that the source handles this correctly.
          source.delete(x);
          source.add(x);
        });
      });

      expect([...source.value]).toEqual(arr.sort(numberComparator));
    }),
  );
});

// the pending items are not included in the next tx
test('rollback', () => {
  const m = new Materialite();
  const source = m.newSetSource(numberComparator);

  try {
    m.tx(() => {
      source.add(1);
      throw new Error('rollback');
    });
  } catch (e) {
    // ignore
  }

  expect([...source.value]).toEqual([]);

  source.add(2);
  expect([...source.value]).toEqual([2]);
});
