import {Entity} from '../../../../generate.js';
import {Entry, Multiset} from '../../multiset.js';
import {Version} from '../../types.js';
import {DifferenceStream} from '../difference-stream.js';
import {UnaryOperator} from './unary-operator.js';

/**
 * A dataflow operator that has a single input edge and a single output edge.
 * It collapses multiple updates in the same transaction (version) to a single output.
 * The multiplicity of the output entries is -1, 0, or 1 after the tx is done.
 *
 * The id property iu used to identify the entity.
 */
export class DistinctOperator<T extends Entity> extends UnaryOperator<T, T> {
  // The entries for this version. The string is the ID of the entity.
  readonly #entriesCache = new Map<Version, Map<string, Entry<T>>>();
  // The emitted data for this version. The string is the ID of the entity. The
  // number is the multiplicity outputted so far in this tx.
  readonly #emitted = new Map<Version, Map<string, number>>();

  constructor(input: DifferenceStream<T>, output: DifferenceStream<T>) {
    super(input, output, (version, data) => this.#handleDiff(version, data));
  }

  #handleDiff(version: number, multiset: Multiset<T>): Multiset<T> {
    // Clear data for old versions.
    clearOldVersions(this.#entriesCache, version);
    clearOldVersions(this.#emitted, version);

    // Have we seen the data at this version before?
    let entriesForThisVersion = this.#entriesCache.get(version);
    if (!entriesForThisVersion) {
      // First time we see the data.
      entriesForThisVersion = new Map<string, Entry<T>>();
      this.#entriesCache.set(version, entriesForThisVersion);
    }

    for (const entry of multiset) {
      const {id} = entry[0];
      const existingEntry = entriesForThisVersion.get(id);
      entriesForThisVersion.set(
        id,
        existingEntry ? [entry[0], existingEntry[1] + entry[1]] : entry,
      );
    }

    let emittedMap = this.#emitted.get(version);
    if (!emittedMap) {
      emittedMap = new Map();
      this.#emitted.set(version, emittedMap);
    }

    const newMultiset: Entry<T>[] = [];
    for (const [value, multiplicity] of entriesForThisVersion.values()) {
      const {id} = value;
      const existingMultiplicity = emittedMap.get(id) ?? 0;
      const desiredMultiplicity = Math.sign(multiplicity);
      if (existingMultiplicity !== desiredMultiplicity) {
        newMultiset.push([value, desiredMultiplicity - existingMultiplicity]);
        emittedMap.set(id, desiredMultiplicity);
      }
    }

    return newMultiset;
  }
}

function clearOldVersions(m: Map<Version, unknown>, version: Version) {
  for (const v of m.keys()) {
    if (v < version) {
      m.delete(v);
    } else {
      return;
    }
  }
}
