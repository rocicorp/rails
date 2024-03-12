/*
test:
1. It surfaces the value of the last non-delete event received.
*/

import {expect, test} from 'vitest';
import {Materialite} from '../materialite.js';
import fc from 'fast-check';
import {ValueView} from './primitive-view.js';

const eventArray = fc.array(
  fc.oneof(
    fc.record({
      tag: fc.constant('add'),
      value: fc.integer(),
    }),
    fc.record({
      tag: fc.constant('delete'),
      value: fc.integer(),
    }),
  ),
);

test('always surfaces the value of the last non-delete event that was received', () => {
  fc.assert(
    fc.property(eventArray, events => {
      const m = new Materialite();
      const source = m.newSetSource((l: number, r: number) => l - r);
      const view = new ValueView(m, source.stream, 0);

      events.forEach(event => {
        if (event.tag === 'add') {
          source.add(event.value);
        } else {
          source.delete(event.value);
        }
      });

      expect(view.value).toEqual(
        events.toReversed().find(event => event.tag === 'add')?.value || 0,
      );
    }),
  );
});
