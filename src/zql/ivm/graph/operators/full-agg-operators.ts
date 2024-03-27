import {Multiset} from '../../multiset.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {LinearUnaryOperator} from './linear-unary-operator.js';

export class LinearCountOperator<V> extends LinearUnaryOperator<V, number> {
  #state: number = 0;
  constructor(
    input: DifferenceStreamReader<V>,
    output: DifferenceStreamWriter<number>,
  ) {
    const inner = (collection: Multiset<V>) => {
      for (const e of collection.entries) {
        this.#state += e[1];
      }
      return new Multiset([[this.#state, 1]]);
    };
    super(input, output, inner);
  }
}

type AggregateOut<
  V extends object,
  AggregateResult extends [string, unknown][],
> = V & {
  [K in AggregateResult[number][0]]: AggregateResult[number][1];
};

class FullAggregateOperator<
  V extends object,
  AggregateResult extends [string, unknown][],
> extends LinearUnaryOperator<V, AggregateOut<V, AggregateResult>> {
  #lastOutput: AggregateOut<V, AggregateResult> | undefined;

  constructor(
    input: DifferenceStreamReader<V>,
    output: DifferenceStreamWriter<AggregateOut<V, AggregateResult>>,
    fn: (
      collection: Multiset<V>,
      last: AggregateOut<V, AggregateResult> | V,
    ) => AggregateOut<V, AggregateResult>,
  ) {
    const inner = (
      collection: Multiset<V>,
    ): Multiset<AggregateOut<V, AggregateResult>> => {
      let last: V | AggregateOut<V, AggregateResult>;
      if (this.#lastOutput === undefined) {
        const iter = collection.entries[Symbol.iterator]();
        const first = iter.next().value;
        if (!first) {
          return new Multiset([]);
        }
        last = {
          ...first[0],
        };
      } else {
        last = this.#lastOutput;
      }
      const next = fn(collection, last);

      let ret;
      if (this.#lastOutput !== undefined) {
        ret = new Multiset([
          [this.#lastOutput, -1],
          [next, 1],
        ]);
      } else {
        ret = new Multiset([[next, 1]]);
      }
      this.#lastOutput = next;
      return ret;
    };
    super(input, output, inner);
  }
}

export class FullCountOperator<
  V extends object,
  Alias extends string,
> extends FullAggregateOperator<V, [[Alias, number]]> {
  #count = 0;

  constructor(
    input: DifferenceStreamReader<V>,
    output: DifferenceStreamWriter<AggregateOut<V, [[Alias, number]]>>,
    alias: Alias,
  ) {
    super(
      input,
      output,
      (
        collection: Multiset<V>,
        last: AggregateOut<V, [[Alias, number]]> | V,
      ): AggregateOut<V, [[Alias, number]]> => {
        for (const entry of collection.entries) {
          this.#count += entry[1];
        }
        return {
          ...(last as AggregateOut<V, [[Alias, number]]>),
          [alias]: this.#count,
        };
      },
    );
  }
}

export class FullAvgOperator<
  V extends object,
  Field extends keyof V,
  Alias extends string,
> extends FullAggregateOperator<V, [[Alias, number]]> {
  #numElements = 0;
  #avg = 0;

  constructor(
    input: DifferenceStreamReader<V>,
    output: DifferenceStreamWriter<AggregateOut<V, [[Alias, number]]>>,
    field: Field,
    alias: Alias,
  ) {
    super(
      input,
      output,
      (
        collection: Multiset<V>,
        last: AggregateOut<V, [[Alias, number]]> | V,
      ): AggregateOut<V, [[Alias, number]]> => {
        let numElements = 0;
        let sum = 0;
        for (const entry of collection.entries) {
          numElements += entry[1];
          sum += (entry[0][field] as number) * entry[1];
        }

        if (this.#numElements + numElements === 0) {
          this.#avg = 0;
        } else {
          this.#avg =
            (this.#avg * this.#numElements + sum) /
            (this.#numElements + numElements);
          this.#numElements += numElements;
        }

        return {
          ...(last as AggregateOut<V, [[Alias, number]]>),
          [alias]: this.#avg,
        };
      },
    );
  }
}

export class FullSumOperator<
  V extends object,
  Field extends keyof V,
  Alias extends string,
> extends FullAggregateOperator<V, [[Alias, number]]> {
  #sum = 0;

  constructor(
    input: DifferenceStreamReader<V>,
    output: DifferenceStreamWriter<AggregateOut<V, [[Alias, number]]>>,
    field: Field,
    alias: Alias,
  ) {
    super(
      input,
      output,
      (
        collection: Multiset<V>,
        last: AggregateOut<V, [[Alias, number]]> | V,
      ): AggregateOut<V, [[Alias, number]]> => {
        for (const entry of collection.entries) {
          this.#sum += (entry[0][field] as number) * entry[1];
        }

        return {
          ...(last as AggregateOut<V, [[Alias, number]]>),
          [alias]: this.#sum,
        };
      },
    );
  }
}
