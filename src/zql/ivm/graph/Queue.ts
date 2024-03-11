import {invariant} from '../../error/invariant-violation.js';
import {Multiset} from '../multiset.js';
import {Version} from '../types.js';

type Node<T> = {
  data: readonly [Version, Multiset<T>];
  next: Node<T> | null;
};

/**
 * Queue between operators in the graph.
 *
 * Methods left out of the current iteration:
 * - prepareForRecompute (for re-fetching data along shared edges of the graph)
 */
export class Queue<T> {
  #lastSeenVersion = -1;
  #head: Node<T> | null = null;
  #tail: Node<T> | null = null;

  enqueue(data: readonly [Version, Multiset<T>]) {
    invariant(
      data[0] > this.#lastSeenVersion,
      'Received stale data along a graph edge.',
    );

    this.#lastSeenVersion = data[0];
    const node = {data, next: null};
    if (this.#head === null) {
      this.#head = node;
    } else {
      this.#tail!.next = node;
    }
    this.#tail = node;
  }

  peek() {
    return this.#head === null ? null : this.#head.data;
  }

  dequeue() {
    if (this.#head === null) {
      return null;
    }
    const ret = this.#head.data;
    this.#head = this.#head.next;
    if (this.#head === null) {
      this.#tail = null;
    }
    return ret;
  }

  isEmpty() {
    return this.#head === null;
  }

  clear() {
    this.#head = null;
    this.#tail = null;
  }
}
