const intFmt = new Intl.NumberFormat("en-US");

export function int(n: number | undefined): string {
  return n === undefined || Number.isNaN(n) ? "n/a" : intFmt.format(Math.round(n));
}

export function fixed(n: number | undefined, digits = 1): string {
  return n === undefined || Number.isNaN(n)
    ? "n/a"
    : new Intl.NumberFormat("en-US", { minimumFractionDigits: digits, maximumFractionDigits: digits }).format(n);
}

export function bytes(n: number): string {
  if (n >= 1024 * 1024) return `${fixed(n / (1024 * 1024), 1)} MiB`;
  if (n >= 1024) return `${fixed(n / 1024, 1)} KiB`;
  return `${int(n)} B`;
}

export function ms(n: number | undefined): string {
  if (n === undefined) return "n/a";
  if (n >= 1000) return `${fixed(n / 1000, 2)} s`;
  return `${fixed(n, n < 10 ? 1 : 0)} ms`;
}

export function recordSize(n: number): string {
  return n >= 1024 ? `${n / 1024} KB` : `${n} B`;
}
