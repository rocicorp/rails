import {Materialite} from '../materialite.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {Version} from '../types.js';
import {View} from './view.js';
import {assert} from '../../error/asserts.js';

export abstract class AbstractView<T, CT> implements View<CT> {
  readonly #stream;
  protected readonly _materialite: Materialite;
  protected readonly _reader;
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
    this._reader = this.#stream.newReader();
    this._reader.setOperator({
      run: (v: Version) => {
        this._run(v);
      },
      notify(_v: Version) {},
      notifyCommitted: (v: Version) => {
        this.#hydrated = true;
        this._notifyCommitted(this.value, v);
      },
      destroy() {},
      messageUpstream: _ => {
        assert(
          false,
          'Message Upstream should not be called in a view operator',
        );
      },
    });
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
    // TODO: we have to notify our derivations too.
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

  protected abstract _run(v: Version): void;
}
