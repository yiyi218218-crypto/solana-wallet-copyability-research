import { createHash, randomUUID } from "node:crypto";

export function sha256(value: string): string {
  return createHash("sha256").update(value).digest("hex");
}

export function stableJson(value: unknown): string {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableJson).join(",")}]`;
  const object = value as Record<string, unknown>;
  return `{${Object.keys(object)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${stableJson(object[key])}`)
    .join(",")}}`;
}

export function newId(prefix: string): string {
  return `${prefix}_${randomUUID()}`;
}

export function nowIso(): string {
  return new Date().toISOString();
}

export function schemaFingerprint(payload: unknown): string {
  const paths: string[] = [];
  const visit = (value: unknown, path: string): void => {
    if (value === null) {
      paths.push(`${path}:null`);
    } else if (Array.isArray(value)) {
      paths.push(`${path}:array`);
      value.slice(0, 3).forEach((item, index) => visit(item, `${path}[${index}]`));
    } else if (typeof value === "object") {
      paths.push(`${path}:object`);
      for (const [key, child] of Object.entries(value as Record<string, unknown>).sort(([a], [b]) => a.localeCompare(b))) {
        visit(child, `${path}.${key}`);
      }
    } else {
      paths.push(`${path}:${typeof value}`);
    }
  };
  visit(payload, "$");
  return sha256(paths.sort().join("\n"));
}
