import {expect, test} from 'vitest';
import {Materialite} from '../Materialite.js';
import {applySelect, orderingProp} from '../../ast-to-ivm/pipelineBuilder.js';
import {MutableTreeView} from './TreeView.js';
import {ascComparator, descComparator} from '../../query/Statement.js';
import {DifferenceStream} from '../graph/DifferenceStream.js';
import {Primitive} from '@vlcn.io/ds-and-algos/types';
import fc from 'fast-check';

const numberComparator = (l: number, r: number) => l - r;

type Entity = {
  id: string;
  n: number;
};
type Selected = {id: string; [orderingProp]: Primitive[]};
test('asc and descComparator on Entities', () => {
  const m = new Materialite();
  const s = m.newStatelessSource<Entity>();

  const updatedStream = applySelect(
    s.stream,
    ['id'],
    [['n'], 'asc'],
  ) as DifferenceStream<Selected>;

  const view = new MutableTreeView<Selected>(
    m,
    updatedStream,
    ascComparator,
    true,
  );
  const descView = new MutableTreeView<Selected>(
    m,
    applySelect(
      s.stream,
      ['id'],
      [['n'], 'desc'],
    ) as DifferenceStream<Selected>,
    descComparator,
    true,
  );

  const items = [
    {id: 'a', n: 1},
    {id: 'b', n: 1},
    {id: 'c', n: 1},
  ] as const;

  s.add(items[0]);
  s.add(items[1]);
  s.add(items[2]);

  expect(view.value).toEqual([{id: 'a'}, {id: 'b'}, {id: 'c'}]);
  expect(descView.value).toEqual([{id: 'c'}, {id: 'b'}, {id: 'a'}]);
});

test('add & remove', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newStatelessSource<number>();
      const view = new MutableTreeView(
        m,
        source.stream,
        numberComparator,
        true,
      );

      m.tx(() => {
        arr.forEach(x => source.add(x));
      });
      expect(view.value).toEqual(arr.sort(numberComparator));

      m.tx(() => {
        arr.forEach(x => source.delete(x));
      });
      expect(view.value).toEqual([]);
    }),
  );
});

test('replace', () => {
  fc.assert(
    fc.property(fc.uniqueArray(fc.integer()), arr => {
      const m = new Materialite();
      const source = m.newStatelessSource<number>();
      const view = new MutableTreeView(
        m,
        source.stream,
        numberComparator,
        true,
      );

      m.tx(() => {
        arr.forEach(x => source.add(x));
      });
      expect(view.value).toEqual(arr.sort(numberComparator));
      m.tx(() => {
        arr.forEach(x => {
          // We have special affordances for deletes immediately followed by adds
          // As those are really replaces / updates.
          // Check that the source handles this correctly.
          source.delete(x);
          source.add(x);
        });
      });
      expect(view.value).toEqual(arr.sort(numberComparator));

      m.tx(() => {
        arr.forEach(x => source.delete(x));
      });
      expect(view.value).toEqual([]);
    }),
  );
});
