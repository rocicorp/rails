import {expect, test} from 'vitest';
import {DifferenceStreamWriter} from './difference-stream-writer.js';
import {Multiset} from '../multiset.js';
import {DebugOperator} from './operators/debug-operator.js';
import {createPullMessage, createPullResponseMessage} from './message.js';
import {DifferenceStreamReader} from './difference-stream-reader.js';
import {NoOp} from './operators/operator.js';

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
      destroy() {},
      messageUpstream() {},
    });
  });

  w.queueData([1, new Multiset([])]);
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
      destroy() {},
      messageUpstream() {},
    });
  });

  w.queueData([1, new Multiset([])]);
  w.notify(1);
  w.notifyCommitted(1);

  expect(notifications).toEqual(readers.map(() => true));
});

test('replying to a message only notifies along the requesting path', () => {
  /*
  Creates a graph of the shape:

       w
     / | \
    r  r  r
    |  |  |
    d  d  d
    |  |  |
    n  n  n

    w = writer
    r = reader
    d = debug operator
    n = no-op operator
  */
  const w = new DifferenceStreamWriter<number>();
  const readers = Array.from({length: 3}, () => w.newReader());
  const notifications = readers.map(() => false);
  const outputs: DifferenceStreamReader<number>[] = [];

  readers.forEach((r, i) => {
    const outputWriter = new DifferenceStreamWriter<number>();
    const outputReader = outputWriter.newReader();
    outputReader.setOperator(new NoOp(outputReader));
    outputs.push(outputReader);
    new DebugOperator(r, outputWriter, () => (notifications[i] = true));
  });

  const msg = createPullMessage([[], 'asc'], 'select');

  outputs[1].messageUpstream(msg);

  expect(notifications).toEqual([false, false, false]);

  w.queueData([1, new Multiset([]), createPullResponseMessage(msg)]);
  w.notify(1);
  expect(notifications).toEqual([false, true, false]);

  expect(() =>
    w.queueData([2, new Multiset([]), createPullResponseMessage(msg)]),
  ).toThrow('No recipient');
});
