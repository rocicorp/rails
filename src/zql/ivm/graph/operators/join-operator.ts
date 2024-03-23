import {Primitive} from '../../../ast/ast.js';
import {Multiset} from '../../multiset.js';
import {JoinResult, StrOrNum, Version} from '../../types.js';
import {DifferenceStreamReader} from '../difference-stream-reader.js';
import {DifferenceStreamWriter} from '../difference-stream-writer.js';
import {BinaryOperator} from './binary-operator.js';
import {Index} from './operator-index.js';

type JoinArgs<
  Key extends Primitive,
  AValue,
  BValue,
  AAlias extends string = '',
  BAlias extends string = '',
> = {
  a: DifferenceStreamReader<AValue>;
  aAs: AAlias | undefined;
  getAJoinKey: (value: AValue) => Key;
  getAPrimaryKey: (value: AValue) => StrOrNum;
  b: DifferenceStreamReader<BValue>;
  bAs: BAlias | undefined;
  getBJoinKey: (value: BValue) => Key;
  getBPrimaryKey: (value: BValue) => StrOrNum;
  output: DifferenceStreamWriter<JoinResult<AValue, BValue, AAlias, BAlias>>;
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

  constructor({
    a,
    aAs,
    getAJoinKey,
    getAPrimaryKey,
    b,
    bAs,
    getBJoinKey,
    getBPrimaryKey,
    output,
  }: JoinArgs<K, AValue, BValue, AAlias, BAlias>) {
    const indexA = new Index<K, AValue>(getAPrimaryKey);
    const indexB = new Index<K, BValue>(getBPrimaryKey);

    const inner = (version: Version) => {
      for (const entry of this.inputAMessages(version)) {
        const deltaA = new Index<K, AValue>(getAPrimaryKey);
        for (const [value, mult] of entry[1].entries) {
          deltaA.add(getAJoinKey(value), [value, mult]);
        }
        this.#inputAPending.push(deltaA);
      }

      for (const entry of this.inputBMessages(version)) {
        const deltaB = new Index<K, BValue>(getBPrimaryKey);
        for (const [value, mult] of entry[1].entries) {
          deltaB.add(getBJoinKey(value), [value, mult]);
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
          result.extend(deltaA.join(aAs, indexB, bAs, getBPrimaryKey));
          indexA.extend(deltaA);
        }

        if (deltaB !== undefined) {
          result.extend(indexA.join(aAs, deltaB, bAs, getBPrimaryKey));
          indexB.extend(deltaB);
        }

        this._output.queueData([version, result.consolidate(x => x.id)]);
        indexA.compact();
        indexB.compact();
      }
    };
    super(a, b, output, inner);
  }
}

// export
