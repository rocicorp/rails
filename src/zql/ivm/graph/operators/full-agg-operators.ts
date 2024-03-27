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
      last: AggregateOut<V, AggregateResult> | undefined,
    ) => AggregateOut<V, AggregateResult>,
  ) {
    const inner = (
      collection: Multiset<V>,
    ): Multiset<AggregateOut<V, AggregateResult>> => {
      const next = fn(collection, this.#lastOutput);

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

export class FullCountOperator<V extends object> extends FullAggregateOperator<
  V,
  AggregateResult extends [string, unknown][]
> extends FullAggregateOperator<V, AggregateOut<V, AggregateResult>>

function makeAggregator() {

}