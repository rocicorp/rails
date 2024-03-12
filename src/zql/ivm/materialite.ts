import {Comparator} from '@vlcn.io/ds-and-algos/types';
import {SourceInternal} from './source/source.js';
import {MutableSetSource} from './source/set-source.js';
import {StatelessSource} from './source/stateless-source.js';
import {Version} from './types.js';
import {must} from '../error/invariant-violation.js';

export type MaterialiteForSourceInternal = {
  readonly materialite: Materialite;
  addDirtySource(source: SourceInternal): void;
};

export class Materialite {
  #version: Version;
  #dirtySources: Set<SourceInternal> = new Set();

  #currentTx: Version | null = null;
  #internal: MaterialiteForSourceInternal;

  constructor() {
    this.#version = 0;
    this.#internal = {
      materialite: this,
      addDirtySource: (source: SourceInternal) => {
        this.#dirtySources.add(source);
        // auto-commit if not in a transaction
        if (this.#currentTx === null) {
          this.#currentTx = this.#version + 1;
          this.#commit();
        }
      },
    };
  }

  newStatelessSource<T>() {
    return new StatelessSource<T>(this.#internal);
  }

  newSetSource<T>(comparator: Comparator<T>) {
    return new MutableSetSource<T>(this.#internal, comparator);
  }

  /**
   * Run the provided lambda in a transaciton.
   * Will be committed when the lambda exits
   * and all incremental computations that depend on modified inputs
   * will be run.
   *
   * An exception to this is in the case of nested transactions.
   * No incremental computation will run until the outermost transaction
   * completes.
   *
   * If the transaction throws, all pending inputs which were queued will be rolled back.
   * If a nested transaction throws, all transactions in the stack are rolled back.
   *
   * In this way, nesting transactions only exists to allow functions to be ignorant
   * of what transactions other functions that they call may create. It would be problematic
   * if creating transactions within transactions failed as it would preclude the use of
   * libraries that use transactions internally.
   */
  tx(fn: () => void) {
    if (this.#currentTx === null) {
      this.#currentTx = this.#version + 1;
    } else {
      // nested transaction
      // just run the function as we're already inside the
      // scope of a transaction that will handle rollback and commit.
      fn();
      return;
    }

    try {
      fn();
      this.#commit();
    } catch (e) {
      this.#rollback();
      throw e;
    } finally {
      this.#dirtySources.clear();
    }
  }

  #rollback() {
    this.#currentTx = null;
    for (const source of this.#dirtySources) {
      source.onRollback();
    }
  }

  #commit() {
    this.#version = must(this.#currentTx);
    this.#currentTx = null;
    for (const source of this.#dirtySources) {
      source.onCommitPhase1(this.#version);
    }
    for (const source of this.#dirtySources) {
      source.onCommitPhase2(this.#version);
    }
    for (const source of this.#dirtySources) {
      source.onCommitted(this.#version);
    }
  }
}
