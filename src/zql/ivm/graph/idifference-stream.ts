export type MaterializeOptions = {
  wantInitialData?: boolean;
  limit?: number;
  name?: string;
};

/**
 * A stream of "difference events" or "change events" to the database.
 *
 * These streams are operated on by `Operators` in order to build up
 * a streaming query.
 *
 * Methods left out of the current iteration:
 * - after (for cursoring)
 * - join
 * - reduce (for aggregates)
 * - concat (for union)
 * - negate (for except)
 * - materializeInto (for meterializing the stream)
 */
export interface IDifferenceStream<T> {
  map<O>(f: (value: T) => O): IDifferenceStream<O>;
  filter<S extends T>(f: (x: T) => x is S): IDifferenceStream<S>;
  filter(f: (x: T) => boolean): IDifferenceStream<T>;
  linearCount(): IDifferenceStream<number>;
  effect(f: (i: T, m: number) => void): IDifferenceStream<T>;
}
