export function tab(tab: number, text: string) {
  const lines = text.split('\n');
  return lines
    .map((line, i) =>
      i !== 0 && i !== lines.length - 1 ? '  '.repeat(tab) + line : line,
    )
    .join('\n');
}
