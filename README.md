# Replicache on Rails

Generates a CRUD-style interface for Replicache and Reflect, with optional schema validation.

## Install

```bash
npm install --save-dev @rocicorp/rails
```

## Usage

### 1. Define Entities

```ts
// All entities must include at least id:string.
export type Todo = {
  id: string;
  text: string;
  complete: boolean;
  sort: number;
};
```

### 2. Generate Helpers

```ts
import {generate} from '@rocicorp/rails';

export const {
  put: putTodo,
  get: getTodo,
  list: listTodos,
  // ...
} = generate<Todo>('todo');
```

### 3. Build Mutators

The generated functions all have the same signatures as mutators, so they can be used as mutators directly:

```ts
import {putTodo, updateTodo, deleteTodo} from './todo';

export const mutators = {
  putTodo,
  updateTodo,
  deleteTodo,
};
```

You can also compose them to make more advanced mutators:

```ts
import {listTodos, updateTodo} from './todo';

async function markAllComplete(tx: WriteTransaction) {
  const todos = await listTodos(tx);
  for (const t of todos) {
    // Mutators are transactional, so this is safe. The entire function will
    // run atomically and `t.complete` cannot change while it is running.
    if (!t.complete) {
      await updateTodo(todo.id, {complete: true});
    }
  }
}

export const mutators = {
  // ...
  markAllComplete,
};
```

### 4. Build Subscriptions

The generated functions that are read-only (`get`, `has`, `list`, etc) have the correct signature to be used as subscriptions. So you can use them directly:

```ts
// subscribe to a query
const todos = useSubscribe(rep, listTodos, []);
```

But as with mutators, you can also compose them to make more interesting subscriptions:

```ts
async function listIncompleteTodos(tx: WriteTransaction) {
  const todos = await listTodos(tx);
  return todos.filter(t => !t.complete);
}

const incompleteTodos = useSubscribe(rep, listIncompleteTodos, []);
```

## Validation

You can optionally pass `generate` a validation function as a second parameter. For example, to use Zod as your schema validator:

```ts
import * as z from 'zod';
import {generate} from '@rocicorp/rails';

const todoSchema = {
  id: z.string(),
  text: z.string(),
  complete: z.boolean(),
  sort: z.number(),
};

// In this case, the template parameter to generate can be omitted because it
// is inferred from return type of todoSchema.parse().
export const {
  put: putTodo,
  get: getTodo,
  update: updateTodo,
  delete: deleteTodo,
  list: listTodos,
} = generate('todo', todoSchema.parse);
```

## Reference

### `init(tx: WriteTransaction, value: T): Promise<boolean>`

Writes `value` if it is not already present. If `value` is already present, does nothing. Returns `true` if the value was written or false otherwise.

### `put(tx: WriteTransaction, value: T): Promise<void>`

Writes `value`. If not present, creates, otherwise overwrites.

### `update(tx: WriteTransaction, value: Update<T>) => Promise<void>`

Updates `value`. Value can specify any of the fields of `T` and must contain `id`. Fields not included in `value` are left unaffected.

All fields in an update are applied together atomically. If the entity does not exist a debug message is printed to the console and the update is skipped.

### `delete(tx: WriteTransaction, id: string) => Promise<void>`

Delete any existing value or do nothing if none exist.

### `has(tx: ReadTransaction, id: string) => Promise<boolean>`

Return true if specified value exists, false otherwise.

### `get(tx: ReadTransaction, id: string) => Promise<T | undefined>`

Get value by ID, or return undefined if none exists.

### `mustGet(tx: ReadTransaction, id: string) => Promise<T>`

Get value by ID, or throw if none exists.

### `list(tx: ReadTransaction, options?: {startAtID?: string, limit:? number}) => Promise<T[]>`

List values matching criteria.

### `listIDs(tx: ReadTransaction, options?: {startAtID?: string, limit:? number}) => Promise<string[]>`

List ids matching criteria.

### `entries(tx: ReadTransaction, options?: {startAtID?: string, limit:? number}) => Promise<[string, T][]>`

List [id, value] entries matching criteria.

## Upgrade from 0.6

### Pluggable Schema

Rails 0.7 made the schema validator pluggable. Instead of passing an instance of zod, pass the parse function.

Before:

```ts
export const {
  put: putTodo,
  // ...
} = generate<Todo>('todo', todoSchema);
```

Now:

```ts
export const {
  put: putTodo,
  // ...
} = generate<Todo>('todo', todoSchema.parse);
```

### EntitySchema no longer part of Rails

Because the validator is pluggable it no longer makes sense for Rails to provide `entitySchema`. So either define it yourself:

```ts
const entitySchema = z.object({
  id: z.string(),
});
```

... or simply add `id: z.string()` to each of your entity definitions.

### Parse called in Debug mode

In 0.6.0, zod was only used if `process.env.NODE_ENV !== 'production'`. Now that the validator is pluggable, it makes more sense for the app to do this.

Before:

```ts
export const {
  put: putTodo,
  // ...
} = generate<Todo>('todo', todoSchema);
```

Now:

```ts
export const {
  put: putTodo,
  // ...
} = generate<Todo>(
  'todo',
  process.env.NODE_ENV !== 'production' ? todoSchema.parse : undefined,
);
```
