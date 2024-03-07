import {IStatement, MakeHumanReadable} from './EntityQueryType.js';

export class Statement<TReturn> implements IStatement<TReturn> {
  constructor() {}

  run(): MakeHumanReadable<TReturn> {
    // TODO run the query!
    return {} as TReturn;
  }
}
