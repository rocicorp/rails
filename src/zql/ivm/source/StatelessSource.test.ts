import {test, expect} from 'vitest';
import {Materialite} from '../Materialite.js';

test('add', () => {
  const m = new Materialite();
  const s = m.newStatelessSource();

  let runs = 0;
  s.stream.effect(() => {
    runs++;
  });
  s.add(1);
  expect(runs).toBe(1);
  s.add(1);
  s.add(2);
  expect(runs).toBe(3);
});

test('remove', () => {
  const m = new Materialite();
  const s = m.newStatelessSource();

  let runs = 0;
  s.stream.effect(() => {
    runs++;
  });
  // A stateless source does not track what it actually contains
  // so it is not an error to remove things that were never added.
  s.delete(2);
  expect(runs).toBe(1);
  s.delete(2);
  s.delete(2);
  expect(runs).toBe(3);
});

test('rollback', () => {
  const m = new Materialite();
  const s = m.newStatelessSource();

  let runs = 0;
  s.stream.effect(() => {
    runs++;
  });
  try {
    m.tx(() => {
      s.add(1);
      s.add(2);
      throw new Error('rollback');
    });
  } catch (_) {
    // ignore
  }
  expect(runs).toBe(0);
});

test('effects are not notified until transaction commit', () => {
  const m = new Materialite();
  const s = m.newStatelessSource();

  let runs = 0;
  s.stream.effect(() => {
    runs++;
  });
  m.tx(() => {
    s.add(2);
    expect(runs).toBe(0);
  });
  expect(runs).toBe(1);
  m.tx(() => {
    s.delete(2);
    expect(runs).toBe(1);
  });
  expect(runs).toBe(2);
});

// We don't have a way to test this at the moment 🤔
// test('reactive graph fully runs on notify', () => {
//   const m = new Materialite();
//   const s = m.newStatelessSource<number>();

//   let mapRun = false;
//   let filterRun = false;
//   let map2Run = false;
//   let filter2Run = false;
//   const mapped = s.stream.map(x => {
//     mapRun = true;
//     return x * 2;
//   });
//   mapped.filter(x => {
//     filterRun = true;
//     return x % 2 === 0;
//   });
//   mapped
//     .map(x => {
//       map2Run = true;
//       return x * 2;
//     })
//     .filter(x => {
//       filter2Run = true;
//       return x % 2 === 0;
//     });

//   m.tx(() => {
//     s.add(1);
//     // hmm... this API shouldn't be exposed to clients.
//     // it lets us advance a transaction without committing it 😅
//     s.stream.notify(1);
//     expect(mapRun).toBe(true);
//     expect(filterRun).toBe(true);
//     expect(map2Run).toBe(true);
//     expect(filter2Run).toBe(true);
//   });
// });
