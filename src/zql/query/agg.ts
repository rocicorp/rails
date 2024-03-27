type AggregateBase<Field extends string, Alias extends string> = {
  field: Field;
  alias: Alias;
};

export type Aggregate<Field extends string, Alias extends string> =
  | Min<Field, Alias>
  | Max<Field, Alias>
  | Sum<Field, Alias>
  | Count<Field, Alias>
  | Avg<Field, Alias>
  | AggArray<Field, Alias>;

type Min<Field extends string, Alias extends string> = {
  aggregate: 'min';
} & AggregateBase<Field, Alias>;

type Max<Field extends string, Alias extends string> = {
  aggregate: 'max';
} & AggregateBase<Field, Alias>;

type Sum<Field extends string, Alias extends string> = {
  aggregate: 'sum';
} & AggregateBase<Field, Alias>;

type Avg<Field extends string, Alias extends string> = {
  aggregate: 'avg';
} & AggregateBase<Field, Alias>;

export type Count<Field extends string, Alias extends string> = {
  aggregate: 'count';
} & AggregateBase<Field, Alias>;

export type AggArray<Field extends string, Alias extends string> = {
  aggregate: 'array';
} & AggregateBase<Field, Alias>;

export const agg: {
  min<Field extends string>(field: Field): Min<Field, Field>;
  min<Field extends string, Alias extends string>(
    field: Field,
    alias: Alias,
  ): Min<Field, Alias>;

  max<Field extends string>(field: Field): Max<Field, Field>;
  max<Field extends string, Alias extends string>(
    field: Field,
    alias: Alias,
  ): Max<Field, Alias>;

  sum<Field extends string>(field: Field): Sum<Field, Field>;
  sum<Field extends string, Alias extends string>(
    field: Field,
    alias: Alias,
  ): Sum<Field, Alias>;

  count<Field extends string>(field: Field): Count<Field, Field>;
  count<Field extends string, Alias extends string>(
    field: Field,
    alias: Alias,
  ): Count<Field, Alias>;

  avg<Field extends string>(field: Field): Avg<Field, Field>;
  avg<Field extends string, Alias extends string>(
    field: Field,
    alias: Alias,
  ): Avg<Field, Alias>;

  array<Field extends string>(field: Field): AggArray<Field, Field>;
  array<Field extends string, Alias extends string>(
    field: Field,
    alias: Alias,
  ): AggArray<Field, Alias>;
} = {
  min<Field extends string, Alias extends string>(
    field: Field,
    alias?: Alias | undefined,
  ): Min<Field, Alias> {
    return {
      aggregate: 'min',
      field,
      alias: alias ?? (field as unknown as Alias),
    };
  },

  max<Field extends string, Alias extends string>(
    field: Field,
    alias?: Alias | undefined,
  ): Max<Field, Alias> {
    return {
      aggregate: 'max',
      field,
      alias: alias ?? (field as unknown as Alias),
    };
  },

  sum<Field extends string, Alias extends string>(
    field: Field,
    alias?: Alias | undefined,
  ): Sum<Field, Alias> {
    return {
      aggregate: 'sum',
      field,
      alias: alias ?? (field as unknown as Alias),
    };
  },

  count<Field extends string, Alias extends string>(
    field: Field,
    alias?: Alias | undefined,
  ): Count<Field, Alias> {
    return {
      aggregate: 'count',
      field,
      alias: alias ?? (field as unknown as Alias),
    };
  },

  avg<Field extends string, Alias extends string>(
    field: Field,
    alias?: Alias | undefined,
  ): Avg<Field, Alias> {
    return {
      aggregate: 'avg',
      field,
      alias: alias ?? (field as unknown as Alias),
    };
  },

  array<Field extends string, Alias extends string>(
    field: Field,
    alias?: Alias | undefined,
  ): AggArray<Field, Alias> {
    return {
      aggregate: 'array',
      field,
      alias: alias ?? (field as unknown as Alias),
    };
  },
};
