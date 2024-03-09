# ZQL - IVM

The details of IVM are hidden behind ZQL but for those that want to use it
directly, an outline follows.

Interfaces below are introduced from the most abstract to the least.

# Sources

Sources are how we get data into an IVM pipeline. They roughly have the same
interface as a collection:

- add
- addAll
- remove
- removeAll

One additional property provided by sources is a `stream`.

The `stream` property of a source is used to build incremental computations against the source.

As items are added and removed from the source, any computations applied to
`stream` will be incrementally maintained.

## Replicache Integration

Replicache Rails collections are exposed as sources. `add` and `remove` are invoked by `experimentalWatch` for a given collection.

### Source Variants

There are a few variants of sources.

- stateless
- stateful
- ordered
- unordered

## Usage

> Note that one generally wouldn't use a source directly. `Rails` would register sources as users create / generate collections. `ZQL` would consume those sources when queries are run or subscribed to.

```ts
const s = new Source();
s.stream
  .map(...)
  .filter(...)
  ...;
```

See `StatelessSource.test.ts` for more examples.

# Stream

A stream represents a progression of updates made to a source over time. In the code, streams are called "DifferenceStreams" for this reason.

`DifferenceStreams` are what compute operators are attached to --

- join
- reduce (i.e., aggregates)
- filter (where)
- map (select / projection, function application)
- count

## ZQL Integration

A `ZQL` query is compiled into a pipeline of operators that take the difference streams from one or more `Sources` as input.

## Usage

See `DifferenceStream.test.ts`

# Operator, DifferenceStream[Reader/Writer], Multiset

These are low enough down the abstraction ladder than even direct users bypassing ZQL will likely not need to deal with these directly.
