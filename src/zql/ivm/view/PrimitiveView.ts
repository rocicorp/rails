import {Materialite} from '../Materialite.js';
import {DifferenceStream} from '../graph/DifferenceStream.js';
import {Version} from '../types.js';
import {View} from './View.js';

/**
 * Represents the most recent value from a stream of primitives.
 */
export class ValueView<T> extends View<T, T | null> {
  #data: T | null;

  constructor(
    materialite: Materialite,
    stream: DifferenceStream<T>,
    initial: T | null,
  ) {
    super(materialite, stream);
    this.#data = initial;
  }

  get value() {
    return this.#data;
  }

  protected _run(version: Version) {
    const collections = this._reader.drain(version);
    if (collections.length === 0) {
      return;
    }

    const lastCollection = collections[collections.length - 1]!;
    // const lastValue = lastCollection.entries[lastCollection.entries.length - 1];
    let lastValue = undefined;
    for (const [value, mult] of lastCollection.entries) {
      if (mult > 0) {
        lastValue = value;
      }
    }
    if (lastValue === undefined) {
      return;
    }

    const newData = lastValue as T;
    if (newData !== this.#data) {
      this.#data = newData;
      // this.notify(newData, version);
    }
  }
}
