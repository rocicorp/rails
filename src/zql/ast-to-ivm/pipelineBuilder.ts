// import {nullthrows} from '../error/InvariantViolation.js';
// import {DifferenceStream} from '../ivm/graph/DifferenceStream.js';
// import {AST} from '../query/ZqlAst.js';

// export function buildPipeline(
//   sourceStreamProvider: (sourceName: string) => DifferenceStream<unknown>,
//   ast: AST,
// ) {
//   // filters first
//   // maps second
//   // order is a param to materialization
//   // as well as limit? How does limit work in materialite again?
//   let stream = sourceStreamProvider(
//     nullthrows(ast.table, 'Table not specified in the AST'),
//   );

//   if (ast.where) {
//     stream = applyWhere(stream, ast.where);
//   }

//   // if order is same as underlying stream, we can skip the sort
//   // and we can apply the limit.
//   // If order is different, we materialize the entire thing and sort it.
//   // Then apply the limit when returning slices of the materialized stream.
// }

// function applyWhere(stream: DifferenceStream<unknown>, where: ConditionList) {
//   for (let i = 0; i < where.length; i++) {
//     const condition = where[i];
//     if (condition === 'AND') {
//       stream = stream.filter(() => true);
//     }
//   }
//   return stream;
// }

/*
x = 1 AND y = 2 OR z = 3 OR ()

1. Or can bifurcate the stream and then re-combine it.
2. ANDing a new ConditionList?
  - 
*/
