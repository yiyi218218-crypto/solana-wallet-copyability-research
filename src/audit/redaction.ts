/** Removes credentials from persisted provider failure details and diagnostics. */
export function redactSensitiveText(value: string): string {
  return value
    .replace(/(x-api-key|authorization|api[_-]?key|token)=?\s*[:=]?\s*[^\s,&]+/gi, "$1=[REDACTED]")
    .replace(/Bearer\s+[^\s,]+/gi, "Bearer [REDACTED]")
    .replace(/https?:\/\/([^\s?]+)\?([^\s]+)/gi, (_match, host: string, query: string) => `https://${host}?${query.replace(/([A-Za-z0-9_-]*(?:key|token|secret)[A-Za-z0-9_-]*)=[^&\s]+/gi, "$1=[REDACTED]")}`);
}

export function redactUnknownError(error: unknown): string {
  return redactSensitiveText(error instanceof Error ? error.message : String(error));
}
