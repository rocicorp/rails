import {Materialite} from '../materialite.js';
import {DifferenceStream, Listener} from '../graph/difference-stream.js';
import {Version} from '../types.js';
import {View} from './view.js';
import {Entry} from '../multiset.js';

export abstract class AbstractView<T extends object, CT> implements View<CT> {
  readonly #stream;
  protected readonly _materialite: Materialite;
  protected readonly _listener: Listener<T>;
  protected _notifiedListenersVersion = -1;
  readonly #listeners: Set<(s: CT, v: Version) => void> = new Set();
  readonly name;
  #hydrated = false;

  abstract get value(): CT;

  /**
   * @param stream The stream of differences that should be materialized into this sink
   * @param comparator How to sort results
   */
  constructor(
    materialite: Materialite,
    stream: DifferenceStream<T>,
    name: string = '',
  ) {
    this.name = name;
    this._materialite = materialite;
    this.#stream = stream;
    this._listener = {
      newData: (v: Version, data: Iterable<Entry<T>>) => {
        this._newData(v, data);
      },
      commit: (v: Version) => {
        this.#hydrated = true;
        this._notifyCommitted(this.value, v);
      },
    };
    this.#stream.addDownstream(this._listener);
  }

  get stream() {
    return this.#stream;
  }

  get hydrated() {
    return this.#hydrated;
  }

  abstract pullHistoricalData(): void;

  protected _notifyCommitted(d: CT, v: Version) {
    if (this._notifiedListenersVersion === v) {
      return;
    }
    this._notifiedListenersVersion = v;
    for (const listener of this.#listeners) {
      listener(d, v);
    }
  }

  on(listener: (s: CT, v: Version) => void) {
    this.#listeners.add(listener);
    return () => {
      this.off(listener);
    };
  }

  /**
   * If there are 0 listeners left after removing the given listener,
   * the view is destroyed.
   *
   * To opt out of this behavior, pass `autoCleanup: false`
   */
  off(listener: (s: CT, v: Version) => void) {
    this.#listeners.delete(listener);
  }

  destroy() {
    this.#listeners.clear();
  }

  protected abstract _newData(v: Version, data: Iterable<Entry<T>>): void;
}
