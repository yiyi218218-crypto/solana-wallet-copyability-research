import type { Unknown } from "./types.js";

export const UNKNOWN = "UNKNOWN" as const;

export function unknownIfMissing<T>(value: T | null | undefined): Unknown<T> {
  return value === null || value === undefined ? UNKNOWN : value;
}

export function isKnown<T>(value: Unknown<T>): value is T {
  return value !== UNKNOWN;
}

export function finiteNumber(value: unknown): Unknown<number> {
  return typeof value === "number" && Number.isFinite(value) ? value : UNKNOWN;
}

export function booleanOrUnknown(value: unknown): Unknown<boolean> {
  return typeof value === "boolean" ? value : UNKNOWN;
}

export function stringOrUnknown(value: unknown): Unknown<string> {
  return typeof value === "string" && value.length > 0 ? value : UNKNOWN;
}

export function nullableStringOrUnknown(value: unknown): Unknown<string | null> {
  return value === null || (typeof value === "string" && value.length > 0) ? value : UNKNOWN;
}

export function parseEpochSeconds(value: unknown): string | undefined {
  if (typeof value !== "number" || !Number.isFinite(value)) return undefined;
  return new Date(value * 1000).toISOString();
}
