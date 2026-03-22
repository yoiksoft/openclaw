import os from "node:os";
import path from "node:path";
import { resolveStateDir } from "../config/paths.js";
import { loadJsonFile, saveJsonFile } from "../infra/json-file.js";

export type BranchRegistryEntry = {
  /** Session key for the subagent that owns this branch. */
  sessionKey: string;
  /** GitHub PR number associated with this branch. */
  prNumber?: number;
  /** Repository identifier (e.g. "owner/repo"). */
  repo?: string;
  /** ISO 8601 timestamp when the entry was created. */
  createdAt: string;
  status: "watching";
};

export type BranchRegistry = Record<string, BranchRegistryEntry>;

/**
 * Resolves the path to the branch registry file.
 * Override via OPENCLAW_BRANCH_REGISTRY_PATH env var.
 * Default: ~/.openclaw/workspace/branches/_registry.json
 */
export function resolveBranchRegistryPath(env: NodeJS.ProcessEnv = process.env): string {
  const override = env.OPENCLAW_BRANCH_REGISTRY_PATH?.trim();
  if (override) {
    return path.resolve(override);
  }
  if (env.VITEST || env.NODE_ENV === "test") {
    return path.join(
      os.tmpdir(),
      "openclaw-test-branch-registry",
      String(process.pid),
      "_registry.json",
    );
  }
  return path.join(resolveStateDir(env), "workspace", "branches", "_registry.json");
}

function loadRegistry(env: NodeJS.ProcessEnv = process.env): BranchRegistry {
  const raw = loadJsonFile(resolveBranchRegistryPath(env));
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return {};
  }
  return raw as BranchRegistry;
}

function saveRegistry(registry: BranchRegistry, env: NodeJS.ProcessEnv = process.env): void {
  saveJsonFile(resolveBranchRegistryPath(env), registry);
}

/** Register a branch → session mapping in the registry. */
export function registerBranch(branch: string, entry: BranchRegistryEntry): void {
  const trimmed = branch.trim();
  if (!trimmed) {
    return;
  }
  const registry = loadRegistry();
  registry[trimmed] = entry;
  saveRegistry(registry);
}

/** Remove a branch from the registry. No-op if the branch is not present. */
export function unregisterBranch(branch: string): void {
  const trimmed = branch.trim();
  if (!trimmed) {
    return;
  }
  const registry = loadRegistry();
  if (!(trimmed in registry)) {
    return;
  }
  delete registry[trimmed];
  saveRegistry(registry);
}

/** Return the registry entry for a branch, or undefined if not found. */
export function getBranchEntry(branch: string): BranchRegistryEntry | undefined {
  const trimmed = branch.trim();
  if (!trimmed) {
    return undefined;
  }
  const registry = loadRegistry();
  return registry[trimmed];
}

/**
 * Return all branches in the registry.
 * When `isSessionAlive` is provided, stale entries (whose session is no longer
 * alive) are pruned on read — removed from the file and excluded from the result.
 */
export function listBranches(isSessionAlive?: (sessionKey: string) => boolean): BranchRegistry {
  const registry = loadRegistry();
  if (!isSessionAlive) {
    return registry;
  }
  const stale: string[] = [];
  for (const [branch, entry] of Object.entries(registry)) {
    if (!isSessionAlive(entry.sessionKey)) {
      stale.push(branch);
    }
  }
  if (stale.length === 0) {
    return registry;
  }
  for (const branch of stale) {
    delete registry[branch];
  }
  saveRegistry(registry);
  return registry;
}
