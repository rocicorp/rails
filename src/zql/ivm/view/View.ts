import {Materialite} from '../materialite.js';
import {DifferenceStream} from '../graph/difference-stream.js';
import {Version} from '../types.js';
import {IView} from './iview.js';

export abstract class View<T, CT> implements IView<CT> {
  readonly #stream;
  protected readonly _materialite: Materialite;
  protected readonly _reader;
  protected _notifiedListenersVersion = -1;
  readonly #listeners: Set<(s: CT, v: Version) => void> = new Set();

  abstract get value(): CT;

  /**
   * @param stream The stream of differences that should be materialized into this sink
   * @param comparator How to sort results
   */
  constructor(
    materialite: Materialite,
    stream: DifferenceStream<T>,
    readonly name: string = '',
  ) {
    this._materialite = materialite;
    this.#stream = stream;
    this._reader = this.#stream.newReader();
    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const self = this;
    this._reader.setOperator({
      run(v: Version) {
        self._run(v);
      },
      notify(_v: Version) {},
      notifyCommitted(v: Version) {
        self._notifyCommitted(self.value, v);
      },
    });
  }

  get stream() {
    return this.#stream;
  }

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
   * @param listener
   */
  off(listener: (s: CT, v: Version) => void) {
    this.#listeners.delete(listener);
  }

  destroy() {
    this.#listeners.clear();
    // TODO: this.#stream.removeReader(this._reader);
  }

  protected abstract _run(v: Version): void;
}
