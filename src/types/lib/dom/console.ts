export type Console = {
  debug(...args: unknown[]): void;
  info(...args: unknown[]): void;
  log(...args: unknown[]): void;
  error(...args: unknown[]): void;
};
