import {Materialite} from '../ivm/materialite.js';
import {Source} from '../ivm/source/source.js';
import {Entity} from '../../generate.js';

export type Context = {
  materialite: Materialite;
  getSource: <T extends Entity>(
    name: string,
    ordering?: [string[], 'asc' | 'desc'],
  ) => Source<T>;
  destroy: () => void;
};

export function makeTestContext(): Context {
  const materialite = new Materialite();
  const sources = new Map<string, Source<unknown>>();
  const getSource = <T extends Entity>(name: string) => {
    if (!sources.has(name)) {
      sources.set(name, materialite.newStatelessSource<T>());
    }
    return sources.get(name)! as Source<T>;
  };
  return {materialite, getSource, destroy() {}};
}

// const replicacheContexts = new Map<string, Context>();
// export function getReplicacheContext(tx: ReadTransaction): Context {
//   let existing = replicacheContexts.get(tx.clientID);
//   if (!existing) {
//     existing = {
//       materialite: new Materialite(),
//       getSource: (name, _ordering?: [string[], 'asc' | 'desc']) => {
//         throw new Error(`Source not found: ${name}`);
//       },
//       destroy() {
//         replicacheContexts.delete(tx.clientID);
//       },
//     };
//     replicacheContexts.set(tx.clientID, existing);
//   }

//   return existing;
// }
