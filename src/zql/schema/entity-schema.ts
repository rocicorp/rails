export type Edge<Src extends EntitySchema, Dst extends EntitySchema> = {
  src: Src;
  srcField: keyof Src['fields'];
  dst: Dst;
  dstField: keyof Dst['fields'];
};

export type Edges = {
  [key: string]: Edge<EntitySchema, EntitySchema>;
};
export type Fields = {
  id: string;
} & {
  [key: string]: unknown;
};

export interface EntitySchema {
  readonly fields: Fields;
  readonly edges?: Edges;
}
