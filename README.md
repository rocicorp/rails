# Replicache on Rails

Generates a CRUD-style interface for Replicache and validation from a [Zod schema](https://github.com/colinhacks/zod).

## Install

```bash
npm install --save-dev @rocicorp/rails
```

## Usage

### 1. Define Entities

```ts
// todo.ts

import {z} from 'zod';
import {entitySchema, generate, Update} from 'rails';

// All entities must extend `entitySchema`.
export const todoSchema = entitySchema.extend({
  text: z.string(),
  completed: z.boolean(),
  sort: z.number(),
});

// Export generated interface.
export type Todo = z.infer<typeof todoSchema>;
export type TodoUpdate = Update<Todo>;
export const [createTodo, getTodo, updateTodo, deleteTodo, listTodos] =
  generate('todo', todoSchema);
```

### 2. Define mutators.ts

```ts
// mutators.ts - used on server, too.

import {createTodo, updateTodo, deleteTodo} from './todo';

export const mutators = {
  createTodo,
  updateTodo,
  deleteTodo,
};
export type M = typeof mutators;
```

### 3. Implement app!

```ts
// app.tsx

import {M, mutators} from './mutators';
import {listTodos} from './todo';
import {Replicache} from 'replicache';
import {useSubscribe} from 'replicache-react';

// register mutators with Replicache
const rep = new Replicache({
  //...
  mutators,
});

function ListView() {
  // subscribe to a query
  const todos = useSubscribe(rep, listTodos, []);

  // run a mutator
  const onClick = () => {
    rep.mutate.createTodo({
      id: nanoid(),
      text: 'take out the trash',
      completed: false,
      sort: todos.length,
    });
  };

  ...
}
```

## Validation

Rails validates reads and writes in debug mode only\*. In production mode, validation is skipped for performance.

- Not really, see TODO :).

## Conflict Semantics

- **create**: If the entity already exists, it is overwritten.
- **update**: All keys in an update are applied together atomically. If the entity does not exist a debug message is printed to the console and the update is skipped.
- **delete**: If the entity doesn't exist, the delete is a no-op.

## TODO

- Implement `process.env` and only do validation in dev mode
- Accept a logger as a dependency instead of using `console` directly
- Fix duplicate key allocation in edit function
