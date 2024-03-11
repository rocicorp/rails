export type Edge<TSrc extends EntitySchema, TDst extends EntitySchema> = {
  src: TSrc;
  srcField: keyof TSrc['fields'];
  dst: TDst;
  dstField: keyof TDst['fields'];
};

export type Edges = {
  [key: string]: Edge<EntitySchema, EntitySchema>;
};
export type Node = {
  id: string;
} & {
  [key: string]: unknown;
};

export interface EntitySchema {
  readonly fields: Node;
  readonly edges?: Edges;
}
