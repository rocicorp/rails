import {Materialite} from '../materialite.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {Version} from '../types.js';
import {AbstractView} from './abstract-view.js';
import {must} from '../../error/asserts.js';
import {createPullMessage} from '../graph/message.js';

/**
 * Represents the most recent value from a stream of primitives.
 */
export class ValueView<T> extends AbstractView<T, T | null> {
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

  pullHistoricalData(): void {
    this._reader.messageUpstream(createPullMessage(undefined));
  }

  protected _run(version: Version) {
    const collections = this._reader.drain(version);
    if (collections.length === 0) {
      return;
    }

    const lastCollection = must(collections[collections.length - 1])[1];
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
    }
  }
}
