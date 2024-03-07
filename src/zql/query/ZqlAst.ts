// Going for a subset of the SQL `SELECT` grammar
// https://www.sqlite.org/lang_select.html

export type Operator = '=' | '<' | '>' | '>=' | '<=' | 'IN' | 'LIKE' | 'ILIKE';
export type Primitive = string | number | boolean | null | bigint;
type Ref = `${string}.${string}`;
export type AST = {
  readonly table?: string;
  readonly alias?: number;
  readonly select?: string[] | 'count';
  readonly subSelects?: {
    readonly alias: string;
    readonly query: AST;
  }[];
  readonly where?: ConditionList;
  readonly joins?: {
    readonly table: string;
    readonly as: string;
    readonly on: ConditionList;
  }[];
  readonly limit?: number;
  readonly groupBy?: string[];
  readonly orderBy?: [string[], 'asc' | 'desc'];
  readonly after?: Primitive;
};

type Conjunction = 'AND' | 'OR' | 'NOT'; //| 'EXISTS';
export type ConditionList = (Conjunction | Condition)[];
type Condition =
  | ConditionList
  | {
      field: string;
      op: Operator;
      value: {
        type: 'literal' | 'ref' | 'query';
        value: Primitive | Ref | AST;
      };
    };
