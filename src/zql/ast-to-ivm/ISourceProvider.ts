import {DifferenceStream} from '../ivm/graph/DifferenceStream.js';

export interface ISourceStreamProvider {
  get<T>(sourceName: string): DifferenceStream<T>;
}
