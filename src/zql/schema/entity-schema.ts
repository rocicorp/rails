export type Relationship<Src extends EntitySchema, Dst extends EntitySchema> = {
  src: Src;
  srcField: keyof Src['fields'];
  dst: Dst;
  dstField: keyof Dst['fields'];
};

export type Relationships = {
  [key: string]: Relationship<EntitySchema, EntitySchema>;
};
export type Fields = {
  id: string;
} & {
  [key: string]: unknown;
};

export interface EntitySchema {
  readonly fields: Fields;
  readonly relationships?: Relationships;
}
