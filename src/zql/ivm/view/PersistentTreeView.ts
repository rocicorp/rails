import {IView} from './IView.js';

export class PersistentTreeView<TReturn> implements IView<TReturn> {
  asJS(): TReturn {
    // TODO
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    return {} as any;
  }
}
