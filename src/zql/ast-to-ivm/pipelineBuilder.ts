import {nullthrows} from '../error/InvariantViolation.js';
import {DifferenceStream} from '../ivm/graph/DifferenceStream.js';
import {AST, Condition, ConditionList, Operator} from '../query/ZqlAst.js';

export function buildPipeline(
  sourceStreamProvider: (sourceName: string) => DifferenceStream<unknown>,
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

  if (ast.select) {
    if (ast.select === 'count') {
      stream = stream.linearCount();
    } else {
      stream = applySelect(stream, ast.select);
    }
  }

  // Note: the stream is technically attached at this point.
  // We could detach it until the user actually runs (or subscribes to) the statement as a tiny optimization.
  return stream;
}

function applySelect(stream: DifferenceStream<unknown>, select: string[]) {
  return stream.map(x => {
    const ret: Record<string, unknown> = {};
    for (const field of select) {
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      ret[field] = (x as any)[field];
    }

    return ret;
  });
}

function applyWhere(stream: DifferenceStream<unknown>, where: ConditionList) {
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

function applyCondition(
  stream: DifferenceStream<unknown>,
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
