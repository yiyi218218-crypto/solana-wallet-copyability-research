import { mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { join, relative, sep } from "node:path";
import { sha256 } from "../audit/provenance.js";

const excluded = new Set([".git", "node_modules", "dist", "data", "artifacts"]);

/** Produces a hash inventory when the task directory is not itself a Git worktree. */
export function writeSourceTreeManifest(root = process.cwd(), destination = join(root, "artifacts", "phase-2a-live", "freeze", "SOURCE_TREE_MANIFEST.json")): { destination: string; fileCount: number; manifestHash: string } {
  const files = listFiles(root).map((path) => ({ path: relative(root, path).split(sep).join("/"), sha256: sha256File(path) })).sort((a, b) => a.path.localeCompare(b.path));
  const content = `${JSON.stringify({ format: "SOURCE_TREE_MANIFEST/v1", generatedAt: new Date().toISOString(), root, excluded: [...excluded].sort(), files }, null, 2)}\n`;
  mkdirSync(join(destination, ".."), { recursive: true });
  writeFileSync(destination, content, "utf8");
  return { destination, fileCount: files.length, manifestHash: sha256(content) };
}

function listFiles(directory: string): string[] {
  return readdirSync(directory).flatMap((name) => {
    if (excluded.has(name)) return [];
    const path = join(directory, name);
    return statSync(path).isDirectory() ? listFiles(path) : [path];
  });
}

function sha256File(path: string): string {
  return sha256(readFileSync(path).toString("base64"));
}
