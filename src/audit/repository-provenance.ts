import { execFileSync } from "node:child_process";

export interface RepositoryProvenance {
  commit: string;
  branch: string;
  dirty: boolean;
}

function git(cwd: string, args: string[]): string {
  return execFileSync("git", args, { cwd, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }).trim();
}

/** Returns repository identity for new sessions without rewriting historical records. */
export function repositoryProvenance(cwd = process.cwd()): RepositoryProvenance {
  try {
    const commit = git(cwd, ["rev-parse", "HEAD"]);
    const branch = git(cwd, ["branch", "--show-current"]) || "DETACHED";
    return { commit, branch, dirty: git(cwd, ["status", "--porcelain"]).length > 0 };
  } catch {
    return { commit: "NO_GIT_REPOSITORY", branch: "NO_GIT_REPOSITORY", dirty: false };
  }
}
