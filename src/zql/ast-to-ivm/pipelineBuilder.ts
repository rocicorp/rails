import {Entity} from '../../generate.js';
import {nullthrows} from '../error/InvariantViolation.js';
import {DifferenceStream} from '../ivm/graph/DifferenceStream.js';
import {AST, Condition, ConditionList, Operator} from '../query/ZqlAst.js';

export const orderingProp = Symbol();

export function buildPipeline(
  sourceStreamProvider: <T extends Entity>(
    sourceName: string,
  ) => DifferenceStream<T>,
  ast: AST,
) {
  // filters first
  // maps second
  // order is a param to materialization
  // as well as limit? How does limit work in materialite again?
  let stream = sourceStreamProvider(
    nullthrows(ast.table, 'Table not specified in the AST'),
  );

  if (ast.where) {
    stream = applyWhere(stream, ast.where);
  }

  let ret: DifferenceStream<unknown>;
  if (ast.select) {
    if (ast.select === 'count') {
      ret = stream.linearCount();
    } else {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ret = applySelect(stream, ast.select as any, ast.orderBy as any);
    }
  } else {
    throw new Error('No select clause');
  }

  // Note: the stream is technically attached at this point.
  // We could detach it until the user actually runs (or subscribes to) the statement as a tiny optimization.
  return ret;
}

function applySelect<T extends Entity>(
  stream: DifferenceStream<T>,
  select: (keyof T)[],
  orderBy: [(keyof T)[], 'asc' | 'desc'] | undefined,
) {
  return stream.map(x => {
    const ret: Partial<Record<keyof T, unknown>> = {};
    for (const field of select) {
      ret[field] = x[field];
    }

    const orderingValues: unknown[] = [];
    if (orderBy !== undefined) {
      for (const field of orderBy[0]) {
        orderingValues.push(x[field]);
      }
    } else {
      orderingValues.push(x.id);
    }
    Object.defineProperty(ret, orderingProp, {
      enumerable: false,
      writable: false,
      configurable: false,
      value: orderingValues,
    });

    return ret;
  });
}

function applyWhere<T extends Entity>(
  stream: DifferenceStream<T>,
  where: ConditionList,
) {
  let ret = stream;
  /*
  We'll handle `OR` and parentheticals like so:
  OR: We'll create a new stream for the LHS and RHS of the OR then merge together.
  Parentheticals: We'll create a new stream for the LHS and RHS of the operator involved in combining the parenthetical then merge together.
  
  Example:
  (a = 1 AND b = 2) OR (c = 3 AND d = 4)
  Becomes
        s
       / \
     a=1 c=3
     /     \
     b=2   d=4
      \    /
        OR
         |

  So `ORs` cause a fork (two branches that need to be evaluated) and then that fork is combined.
  */
  for (let i = 0; i < where.length; i++) {
    const condition = where[i];
    if (condition === 'AND') {
      continue;
    }

    ret = applyCondition(ret, condition);
  }

  return ret;
}

function applyCondition<T extends Entity>(
  stream: DifferenceStream<T>,
  condition: Condition,
) {
  const operator = getOperator(condition.op);
  return stream.filter(x =>
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    operator((x as any)[condition.field], condition.value.value),
  );
}

// We're well-typed in the query builder so once we're down here
// we can assume that the operator is valid.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
function getOperator(op: Operator): (l: any, r: any) => boolean {
  switch (op) {
    case '=':
      return (l, r) => l === r;
    case '<':
      return (l, r) => l < r;
    case '>':
      return (l, r) => l > r;
    case '>=':
      return (l, r) => l >= r;
    case '<=':
      return (l, r) => l <= r;
    case 'IN':
      return (l, r) => r.includes(l);
    case 'LIKE':
      return (l, r) => l.includes(r);
    case 'ILIKE':
      return (l, r) => l.toLowerCase().includes(r.toLocaleLowerCase());
    default:
      throw new Error(`Operator ${op} not supported`);
  }
}
