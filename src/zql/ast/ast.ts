// Going for a subset of the SQL `SELECT` grammar
// https://www.sqlite.org/lang_select.html

// TODO: the chosen operator needs to constrain the allowed values for the value
// input to the query builder.
export type Operator = '=' | '<' | '>' | '>=' | '<=' | 'IN' | 'LIKE' | 'ILIKE';

export type Primitive = string | number | boolean | null;
// type Ref = `${string}.${string}`;
export type AST = {
  readonly table?: string | undefined;
  readonly alias?: number | undefined;
  readonly select?: string[] | 'count' | undefined;
  // readonly subQueries?: {
  //   readonly alias: string;
  //   readonly query: AST;
  // }[];
  readonly where?: ConditionList | undefined;
  // readonly joins?: {
  //   readonly table: string;
  //   readonly as: string;
  //   readonly on: ConditionList;
  // }[];
  readonly limit?: number | undefined;
  // readonly groupBy?: string[];
  readonly orderBy?: [string[], 'asc' | 'desc'] | undefined;
  // readonly after?: Primitive;
};

type Conjunction = 'AND'; // | 'OR' | 'NOT' | 'EXISTS';
export type ConditionList = (Conjunction | Condition)[];
export type Condition =
  // | ConditionList
  {
    field: string;
    op: Operator;
    value: {
      type: 'literal';
      value: Primitive;
    };
    //  | {
    //   type: 'ref';
    //   value: Ref;
    // } | {
    //   type: 'query';
    //   value: AST;
    // };
  };
