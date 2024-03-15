import {ReadonlyJSONValue} from './json.js';

type DiffOperationAdd<Key, Value = ReadonlyJSONValue> = {
  readonly op: 'add';
  readonly key: Key;
  readonly newValue: Value;
};
type DiffOperationDel<Key, Value = ReadonlyJSONValue> = {
  readonly op: 'del';
  readonly key: Key;
  readonly oldValue: Value;
};
type DiffOperationChange<Key, Value = ReadonlyJSONValue> = {
  readonly op: 'change';
  readonly key: Key;
  readonly oldValue: Value;
  readonly newValue: Value;
};
type DiffOperation<Key> =
  | DiffOperationAdd<Key>
  | DiffOperationDel<Key>
  | DiffOperationChange<Key>;
type NoIndexDiff = readonly DiffOperation<string>[];
type WatchNoIndexCallback = (diff: NoIndexDiff) => void;
type WatchIndexCallback = (diff: IndexDiff) => void;
type IndexKey = readonly [secondary: string, primary: string];
type IndexDiff = readonly DiffOperation<IndexKey>[];
type WatchOptions = WatchIndexOptions | WatchNoIndexOptions;
type WatchIndexOptions = WatchNoIndexOptions & {
  indexName: string;
};
type WatchNoIndexOptions = {
  prefix?: string | undefined;
  initialValuesInFirstDiff?: boolean | undefined;
};
type WatchCallbackForOptions<Options extends WatchOptions> =
  Options extends WatchIndexOptions ? WatchIndexCallback : WatchNoIndexCallback;

export type ReplicacheLike = {
  experimentalWatch(callback: WatchNoIndexCallback): () => void;
  experimentalWatch<Options extends WatchOptions>(
    callback: WatchCallbackForOptions<Options>,
    options?: Options,
  ): () => void;
};
