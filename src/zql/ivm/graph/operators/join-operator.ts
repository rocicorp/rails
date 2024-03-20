import {Primitive} from '../../../ast/ast.js';
import {Multiset} from '../../multiset.js';
import {StrOrNum, Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {BinaryOperator} from './binary-operator.js';
import {Index} from './operator-index.js';

type JoinResult<
  AValue,
  BValue,
  AAlias extends string,
  BAlias extends string,
> = {
  [K in AAlias]: AValue;
} & {
  [K in BAlias]: BValue;
};

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
  AAlias extends string,
  BAlias extends string,
> extends BinaryOperator<
  AValue,
  BValue,
  JoinResult<AValue, BValue, AAlias, BAlias>
> {
  readonly #inputAPending: Index<K, AValue>[] = [];
  readonly #inputBPending: Index<K, BValue>[] = [];

  constructor(
    inputA: DifferenceStreamReader<AValue>,
    aAlias: AAlias,
    getKeyA: (value: AValue) => K,
    getIdentityA: (value: AValue) => StrOrNum,
    inputB: DifferenceStreamReader<BValue>,
    bAlias: BAlias,
    getKeyB: (value: BValue) => K,
    getIdentityB: (value: BValue) => StrOrNum,
    output: DifferenceStreamWriter<JoinResult<AValue, BValue, AAlias, BAlias>>,
  ) {
    const getJoinResultIdentity = makeGetJoinResultIdentity(
      aAlias,
      bAlias,
      getIdentityA,
      getIdentityB,
    );
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
        const result = new Multiset<
          {
            [K in AAlias]: AValue;
          } & {
            [K in BAlias]: BValue;
          }
        >([]);
        const deltaA = this.#inputAPending.shift();
        const deltaB = this.#inputBPending.shift();

        if (deltaA !== undefined) {
          result.extend(deltaA.join(aAlias, indexB, bAlias));
          indexA.extend(deltaA);
        }

        if (deltaB !== undefined) {
          result.extend(indexA.join(aAlias, deltaB, bAlias));
          indexB.extend(deltaB);
        }

        this._output.queueData([
          version,
          result.consolidate(getJoinResultIdentity),
        ]);
        indexA.compact();
        indexB.compact();
      }
    };
    super(inputA, inputB, output, inner);
  }
}

function makeGetJoinResultIdentity<
  AValue,
  BValue,
  AAlias extends string,
  BAlias extends string,
>(
  aAlias: AAlias,
  bAlias: BAlias,
  aIdentity: (v: AValue) => StrOrNum,
  bIdentity: (v: BValue) => StrOrNum,
): (v: JoinResult<AValue, BValue, AAlias, BAlias>) => StrOrNum {
  return v => `${aIdentity(v[aAlias])},${bIdentity(v[bAlias])}`;
}
