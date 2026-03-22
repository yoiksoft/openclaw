import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, beforeEach, describe, expect, test } from "vitest";
import {
  getBranchEntry,
  listBranches,
  registerBranch,
  resolveBranchRegistryPath,
  type BranchRegistryEntry,
  unregisterBranch,
} from "./branch-registry.js";

describe("branch-registry", () => {
  let registryPath: string;

  beforeEach(() => {
    registryPath = path.join(
      os.tmpdir(),
      `openclaw-test-registry-${Date.now()}-${Math.random().toString(36).slice(2)}.json`,
    );
    process.env.OPENCLAW_BRANCH_REGISTRY_PATH = registryPath;
  });

  afterEach(() => {
    delete process.env.OPENCLAW_BRANCH_REGISTRY_PATH;
    try {
      fs.unlinkSync(registryPath);
    } catch {
      // ignore — file may not exist
    }
  });

  const exampleEntry: BranchRegistryEntry = {
    sessionKey: "agent:main:subagent:abc123",
    prNumber: 56,
    repo: "yoiksoft/yoik.me",
    createdAt: "2026-03-17T18:04:00Z",
    status: "watching",
  };

  describe("resolveBranchRegistryPath", () => {
    test("returns env var override as resolved path", () => {
      const customPath = "/tmp/custom-registry.json";
      expect(resolveBranchRegistryPath({ OPENCLAW_BRANCH_REGISTRY_PATH: customPath })).toBe(
        path.resolve(customPath),
      );
    });

    test("uses temp path in test environment", () => {
      const p = resolveBranchRegistryPath({ VITEST: "1" });
      expect(p).toContain("openclaw-test-branch-registry");
    });
  });

  describe("registerBranch", () => {
    test("writes entry to registry file", () => {
      registerBranch("feature/ratings-favourites", exampleEntry);
      const raw = JSON.parse(fs.readFileSync(registryPath, "utf8")) as Record<string, unknown>;
      expect(raw["feature/ratings-favourites"]).toEqual(exampleEntry);
    });

    test("overwrites an existing entry for the same branch", () => {
      registerBranch("feature/ratings-favourites", exampleEntry);
      const updated: BranchRegistryEntry = { ...exampleEntry, sessionKey: "session:new" };
      registerBranch("feature/ratings-favourites", updated);
      expect(getBranchEntry("feature/ratings-favourites")).toEqual(updated);
    });

    test("trims whitespace from branch name", () => {
      registerBranch("  feature/spaces  ", exampleEntry);
      expect(getBranchEntry("feature/spaces")).toEqual(exampleEntry);
    });

    test("ignores empty or whitespace-only branch names", () => {
      registerBranch("", exampleEntry);
      registerBranch("   ", exampleEntry);
      // No file written since no valid branch was registered.
      expect(fs.existsSync(registryPath)).toBe(false);
    });
  });

  describe("getBranchEntry", () => {
    test("returns the stored entry", () => {
      registerBranch("feature/ratings-favourites", exampleEntry);
      const entry = getBranchEntry("feature/ratings-favourites");
      expect(entry).toEqual(exampleEntry);
    });

    test("returns undefined for a missing branch", () => {
      expect(getBranchEntry("feature/nonexistent")).toBeUndefined();
    });

    test("returns undefined for empty branch name", () => {
      expect(getBranchEntry("")).toBeUndefined();
    });
  });

  describe("unregisterBranch", () => {
    test("removes the entry from the registry", () => {
      registerBranch("feature/ratings-favourites", exampleEntry);
      unregisterBranch("feature/ratings-favourites");
      expect(getBranchEntry("feature/ratings-favourites")).toBeUndefined();
    });

    test("does not remove other entries", () => {
      registerBranch("feature/a", exampleEntry);
      registerBranch("feature/b", { ...exampleEntry, sessionKey: "session:b" });
      unregisterBranch("feature/a");
      expect(getBranchEntry("feature/b")).toBeDefined();
    });

    test("is a no-op for a missing branch", () => {
      // Should not throw.
      expect(() => unregisterBranch("feature/nonexistent")).not.toThrow();
    });
  });

  describe("listBranches", () => {
    test("returns empty object when registry file does not exist", () => {
      expect(listBranches()).toEqual({});
    });

    test("returns all entries when no checker is provided", () => {
      registerBranch("feature/a", exampleEntry);
      registerBranch("feature/b", { ...exampleEntry, sessionKey: "session:b" });
      const result = listBranches();
      expect(Object.keys(result)).toHaveLength(2);
      expect(result["feature/a"]).toEqual(exampleEntry);
      expect(result["feature/b"]).toBeDefined();
    });

    test("prunes stale entries when isSessionAlive returns false", () => {
      const aliveEntry: BranchRegistryEntry = { ...exampleEntry, sessionKey: "session:alive" };
      const deadEntry: BranchRegistryEntry = { ...exampleEntry, sessionKey: "session:dead" };
      registerBranch("feature/alive", aliveEntry);
      registerBranch("feature/dead", deadEntry);

      const isSessionAlive = (key: string) => key === "session:alive";
      const result = listBranches(isSessionAlive);

      expect(result["feature/alive"]).toBeDefined();
      expect(result["feature/dead"]).toBeUndefined();
    });

    test("persists pruned registry to disk after pruning", () => {
      const aliveEntry: BranchRegistryEntry = { ...exampleEntry, sessionKey: "session:alive" };
      const deadEntry: BranchRegistryEntry = { ...exampleEntry, sessionKey: "session:dead" };
      registerBranch("feature/alive", aliveEntry);
      registerBranch("feature/dead", deadEntry);

      listBranches((key) => key === "session:alive");

      const raw = JSON.parse(fs.readFileSync(registryPath, "utf8")) as Record<string, unknown>;
      expect(raw["feature/dead"]).toBeUndefined();
      expect(raw["feature/alive"]).toBeDefined();
    });

    test("does not write to disk when no entries are pruned", () => {
      registerBranch("feature/alive", { ...exampleEntry, sessionKey: "session:alive" });
      const statBefore = fs.statSync(registryPath);

      // Small delay to ensure mtime would differ if written.
      listBranches(() => true);

      const statAfter = fs.statSync(registryPath);
      expect(statAfter.mtimeMs).toBe(statBefore.mtimeMs);
    });

    test("returns all entries when all sessions are alive", () => {
      registerBranch("feature/a", exampleEntry);
      registerBranch("feature/b", { ...exampleEntry, sessionKey: "session:b" });
      const result = listBranches(() => true);
      expect(Object.keys(result)).toHaveLength(2);
    });

    test("returns empty object when all sessions are dead", () => {
      registerBranch("feature/a", exampleEntry);
      registerBranch("feature/b", { ...exampleEntry, sessionKey: "session:b" });
      const result = listBranches(() => false);
      expect(result).toEqual({});
    });
  });
});
