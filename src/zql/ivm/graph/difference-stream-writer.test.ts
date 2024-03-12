import {expect, test} from 'vitest';
import {DifferenceStreamWriter} from './difference-stream-writer.js';

test('notify readers', () => {
  const w = new DifferenceStreamWriter();
  const readers = Array.from({length: 3}).map(() => w.newReader());
  const notifications = readers.map(() => false);

  readers.forEach((r, i) => {
    r.setOperator({
      run() {
        notifications[i] = true;
      },
      notify() {},
      notifyCommitted() {},
    });
  });

  w.notify(1);

  expect(notifications).toEqual(readers.map(() => true));
});

test('notify committed readers', () => {
  const w = new DifferenceStreamWriter();
  const readers = Array.from({length: 3}, () => w.newReader());
  const notifications = readers.map(() => false);

  readers.forEach((r, i) => {
    r.setOperator({
      run() {},
      notify() {},
      notifyCommitted() {
        notifications[i] = true;
      },
    });
  });

  w.notify(1);
  w.notifyCommitted(1);

  expect(notifications).toEqual(readers.map(() => true));
});
