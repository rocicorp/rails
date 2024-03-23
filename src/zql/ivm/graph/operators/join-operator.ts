import {Primitive} from '../../../ast/ast.js';
import {Multiset} from '../../multiset.js';
import {JoinResult, StrOrNum, Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {BinaryOperator} from './binary-operator.js';
import {Index} from './operator-index.js';

/**
 * Joins two streams.
 *
 * Inputs:
 * - Stream A of changes
 * - Stream B of changes
 * - A function to extract the key to join on from A
 * - A function to extract the key to join on from B
 * - A function to compare the two keys
 *
 * The output is a stream of joined values of the form:
 *
 * ```ts
 * {
 *   table_name_1_or_alias: row_from_t1,
 *   table_name_2_or_alias: row_from_t2,
 * }[]
 * ```
 *
 * From which the `select` operator can extract the desired fields.
 */
export class InnerJoinOperator<
  K extends Primitive,
  AValue,
  BValue,
  AAlias extends string = '',
  BAlias extends string = '',
> extends BinaryOperator<
  AValue,
  BValue,
  // If AValue or BValue are join results
  // then they should be lifted and need no aliasing
  // since they're already aliased
  JoinResult<AValue, BValue, AAlias, BAlias>
> {
  readonly #inputAPending: Index<K, AValue>[] = [];
  readonly #inputBPending: Index<K, BValue>[] = [];

  constructor(
    inputA: DifferenceStreamReader<AValue>,
    aAlias: AAlias | undefined,
    getKeyA: (value: AValue) => K,
    getIdentityA: (value: AValue) => StrOrNum,
    inputB: DifferenceStreamReader<BValue>,
    bAlias: BAlias | undefined,
    getKeyB: (value: BValue) => K,
    getIdentityB: (value: BValue) => StrOrNum,
    output: DifferenceStreamWriter<JoinResult<AValue, BValue, AAlias, BAlias>>,
  ) {
    const indexA = new Index<K, AValue>(getIdentityA);
    const indexB = new Index<K, BValue>(getIdentityB);

    const inner = (version: Version) => {
      for (const entry of this.inputAMessages(version)) {
        const deltaA = new Index<K, AValue>(getIdentityA);
        for (const [value, mult] of entry[1].entries) {
          deltaA.add(getKeyA(value), [value, mult]);
        }
        this.#inputAPending.push(deltaA);
      }

      for (const entry of this.inputBMessages(version)) {
        const deltaB = new Index<K, BValue>(getIdentityB);
        for (const [value, mult] of entry[1].entries) {
          deltaB.add(getKeyB(value), [value, mult]);
        }
        this.#inputBPending.push(deltaB);
      }

      while (this.#inputAPending.length > 0 || this.#inputBPending.length > 0) {
        const result = new Multiset<JoinResult<AValue, BValue, AAlias, BAlias>>(
          [],
        );
        const deltaA = this.#inputAPending.shift();
        const deltaB = this.#inputBPending.shift();

        if (deltaA !== undefined) {
          result.extend(deltaA.join(aAlias, indexB, bAlias, getIdentityB));
          indexA.extend(deltaA);
        }

        if (deltaB !== undefined) {
          result.extend(indexA.join(aAlias, deltaB, bAlias, getIdentityB));
          indexB.extend(deltaB);
        }

        this._output.queueData([version, result.consolidate(x => x.id)]);
        indexA.compact();
        indexB.compact();
      }
    };
    super(inputA, inputB, output, inner);
  }
}

// export
