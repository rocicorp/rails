import {Entry} from '../multiset.js';
import {Version} from '../types.js';
import {Reply} from './message.js';

export type QueueEntry<T> =
  | readonly [version: Version, multiset: Iterable<Entry<T>>, reply: Reply]
  | readonly [version: Version, multiset: Iterable<Entry<T>>];
