export type Fields = {
  id: string;
} & {
  [key: string]: unknown;
};

export interface EntitySchema {
  readonly fields: Fields;
}
