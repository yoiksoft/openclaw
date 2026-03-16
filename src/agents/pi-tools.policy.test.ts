import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import type { OpenClawConfig } from "../config/config.js";
import {
  filterToolsByPolicy,
  isToolAllowedByPolicyName,
  resolveEffectiveToolPolicy,
  resolveSubagentToolPolicy,
  resolveSubagentToolPolicyForSession,
} from "./pi-tools.policy.js";
import {
  getToolAllowlistForRole,
  resetSessionTeamRolesForTests,
  setSessionTeamRole,
} from "./subagent-team-role-store.js";
import { createStubTool } from "./test-helpers/pi-tool-stubs.js";

describe("pi-tools.policy", () => {
  it("treats * in allow as allow-all", () => {
    const tools = [createStubTool("read"), createStubTool("exec")];
    const filtered = filterToolsByPolicy(tools, { allow: ["*"] });
    expect(filtered.map((tool) => tool.name)).toEqual(["read", "exec"]);
  });

  it("treats * in deny as deny-all", () => {
    const tools = [createStubTool("read"), createStubTool("exec")];
    const filtered = filterToolsByPolicy(tools, { deny: ["*"] });
    expect(filtered).toEqual([]);
  });

  it("supports wildcard allow/deny patterns", () => {
    expect(isToolAllowedByPolicyName("web_fetch", { allow: ["web_*"] })).toBe(true);
    expect(isToolAllowedByPolicyName("web_search", { deny: ["web_*"] })).toBe(false);
  });

  it("keeps apply_patch when exec is allowlisted", () => {
    expect(isToolAllowedByPolicyName("apply_patch", { allow: ["exec"] })).toBe(true);
  });
});

describe("resolveSubagentToolPolicy depth awareness", () => {
  const baseCfg = {
    agents: { defaults: { subagents: { maxSpawnDepth: 2 } } },
  } as unknown as OpenClawConfig;

  const deepCfg = {
    agents: { defaults: { subagents: { maxSpawnDepth: 3 } } },
  } as unknown as OpenClawConfig;

  const leafCfg = {
    agents: { defaults: { subagents: { maxSpawnDepth: 1 } } },
  } as unknown as OpenClawConfig;

  it("applies subagent tools.alsoAllow to re-enable default-denied tools", () => {
    const cfg = {
      agents: { defaults: { subagents: { maxSpawnDepth: 2 } } },
      tools: { subagents: { tools: { alsoAllow: ["sessions_send"] } } },
    } as unknown as OpenClawConfig;
    const policy = resolveSubagentToolPolicy(cfg, 1);
    expect(isToolAllowedByPolicyName("sessions_send", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("cron", policy)).toBe(false);
  });

  it("applies subagent tools.allow to re-enable default-denied tools", () => {
    const cfg = {
      agents: { defaults: { subagents: { maxSpawnDepth: 2 } } },
      tools: { subagents: { tools: { allow: ["sessions_send"] } } },
    } as unknown as OpenClawConfig;
    const policy = resolveSubagentToolPolicy(cfg, 1);
    expect(isToolAllowedByPolicyName("sessions_send", policy)).toBe(true);
  });

  it("merges subagent tools.alsoAllow into tools.allow when both are set", () => {
    const cfg = {
      agents: { defaults: { subagents: { maxSpawnDepth: 2 } } },
      tools: {
        subagents: { tools: { allow: ["sessions_spawn"], alsoAllow: ["sessions_send"] } },
      },
    } as unknown as OpenClawConfig;
    const policy = resolveSubagentToolPolicy(cfg, 1);
    expect(policy.allow).toEqual(["sessions_spawn", "sessions_send"]);
  });

  it("keeps configured deny precedence over allow and alsoAllow", () => {
    const cfg = {
      agents: { defaults: { subagents: { maxSpawnDepth: 2 } } },
      tools: {
        subagents: {
          tools: {
            allow: ["sessions_send"],
            alsoAllow: ["sessions_send"],
            deny: ["sessions_send"],
          },
        },
      },
    } as unknown as OpenClawConfig;
    const policy = resolveSubagentToolPolicy(cfg, 1);
    expect(isToolAllowedByPolicyName("sessions_send", policy)).toBe(false);
  });

  it("does not create a restrictive allowlist when only alsoAllow is configured", () => {
    const cfg = {
      agents: { defaults: { subagents: { maxSpawnDepth: 2 } } },
      tools: { subagents: { tools: { alsoAllow: ["sessions_send"] } } },
    } as unknown as OpenClawConfig;
    const policy = resolveSubagentToolPolicy(cfg, 1);
    expect(policy.allow).toBeUndefined();
    expect(isToolAllowedByPolicyName("subagents", policy)).toBe(true);
  });

  it("depth-1 orchestrator (maxSpawnDepth=2) allows sessions_spawn", () => {
    const policy = resolveSubagentToolPolicy(baseCfg, 1);
    expect(isToolAllowedByPolicyName("sessions_spawn", policy)).toBe(true);
  });

  it("depth-1 orchestrator (maxSpawnDepth=2) allows subagents", () => {
    const policy = resolveSubagentToolPolicy(baseCfg, 1);
    expect(isToolAllowedByPolicyName("subagents", policy)).toBe(true);
  });

  it("depth-1 orchestrator (maxSpawnDepth=2) allows sessions_list", () => {
    const policy = resolveSubagentToolPolicy(baseCfg, 1);
    expect(isToolAllowedByPolicyName("sessions_list", policy)).toBe(true);
  });

  it("depth-1 orchestrator (maxSpawnDepth=2) allows sessions_history", () => {
    const policy = resolveSubagentToolPolicy(baseCfg, 1);
    expect(isToolAllowedByPolicyName("sessions_history", policy)).toBe(true);
  });

  it("depth-1 orchestrator still denies gateway, cron, memory", () => {
    const policy = resolveSubagentToolPolicy(baseCfg, 1);
    expect(isToolAllowedByPolicyName("gateway", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("cron", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("memory_search", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("memory_get", policy)).toBe(false);
  });

  it("depth-2 leaf denies sessions_spawn", () => {
    const policy = resolveSubagentToolPolicy(baseCfg, 2);
    expect(isToolAllowedByPolicyName("sessions_spawn", policy)).toBe(false);
  });

  it("depth-2 orchestrator (maxSpawnDepth=3) allows sessions_spawn", () => {
    const policy = resolveSubagentToolPolicy(deepCfg, 2);
    expect(isToolAllowedByPolicyName("sessions_spawn", policy)).toBe(true);
  });

  it("depth-3 leaf (maxSpawnDepth=3) denies sessions_spawn", () => {
    const policy = resolveSubagentToolPolicy(deepCfg, 3);
    expect(isToolAllowedByPolicyName("sessions_spawn", policy)).toBe(false);
  });

  it("depth-2 leaf denies subagents", () => {
    const policy = resolveSubagentToolPolicy(baseCfg, 2);
    expect(isToolAllowedByPolicyName("subagents", policy)).toBe(false);
  });

  it("depth-2 leaf denies sessions_list and sessions_history", () => {
    const policy = resolveSubagentToolPolicy(baseCfg, 2);
    expect(isToolAllowedByPolicyName("sessions_list", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("sessions_history", policy)).toBe(false);
  });

  it("depth-1 leaf (maxSpawnDepth=1) denies sessions_spawn", () => {
    const policy = resolveSubagentToolPolicy(leafCfg, 1);
    expect(isToolAllowedByPolicyName("sessions_spawn", policy)).toBe(false);
  });

  it("depth-1 leaf (maxSpawnDepth=1) denies sessions_list", () => {
    const policy = resolveSubagentToolPolicy(leafCfg, 1);
    expect(isToolAllowedByPolicyName("sessions_list", policy)).toBe(false);
  });

  it("uses stored leaf role for flat depth-1 session keys", () => {
    const storePath = path.join(
      os.tmpdir(),
      `openclaw-subagent-policy-${Date.now()}-${Math.random().toString(16).slice(2)}.json`,
    );
    fs.mkdirSync(path.dirname(storePath), { recursive: true });
    fs.writeFileSync(
      storePath,
      JSON.stringify(
        {
          "agent:main:subagent:flat-leaf": {
            sessionId: "flat-leaf",
            updatedAt: Date.now(),
            spawnDepth: 1,
            subagentRole: "leaf",
            subagentControlScope: "none",
          },
        },
        null,
        2,
      ),
      "utf-8",
    );
    const cfg = {
      ...baseCfg,
      session: {
        store: storePath,
      },
    } as unknown as OpenClawConfig;

    const policy = resolveSubagentToolPolicyForSession(cfg, "agent:main:subagent:flat-leaf");
    expect(isToolAllowedByPolicyName("sessions_spawn", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("subagents", policy)).toBe(false);
  });

  it("defaults to leaf behavior when no depth is provided", () => {
    const policy = resolveSubagentToolPolicy(baseCfg);
    // Default depth=1, maxSpawnDepth=2 → orchestrator
    expect(isToolAllowedByPolicyName("sessions_spawn", policy)).toBe(true);
  });

  it("defaults to leaf behavior when depth is undefined and maxSpawnDepth is 1", () => {
    const policy = resolveSubagentToolPolicy(leafCfg);
    // Default depth=1, maxSpawnDepth=1 → leaf
    expect(isToolAllowedByPolicyName("sessions_spawn", policy)).toBe(false);
  });
});

describe("resolveEffectiveToolPolicy", () => {
  it("implicitly re-exposes exec and process when tools.exec is configured", () => {
    const cfg = {
      tools: {
        profile: "messaging",
        exec: { host: "sandbox" },
      },
    } as OpenClawConfig;
    const result = resolveEffectiveToolPolicy({ config: cfg });
    expect(result.profileAlsoAllow).toEqual(["exec", "process"]);
  });

  it("implicitly re-exposes read, write, and edit when tools.fs is configured", () => {
    const cfg = {
      tools: {
        profile: "messaging",
        fs: { workspaceOnly: false },
      },
    } as OpenClawConfig;
    const result = resolveEffectiveToolPolicy({ config: cfg });
    expect(result.profileAlsoAllow).toEqual(["read", "write", "edit"]);
  });

  it("merges explicit alsoAllow with implicit tool-section exposure", () => {
    const cfg = {
      tools: {
        profile: "messaging",
        alsoAllow: ["web_search"],
        exec: { host: "sandbox" },
      },
    } as OpenClawConfig;
    const result = resolveEffectiveToolPolicy({ config: cfg });
    expect(result.profileAlsoAllow).toEqual(["web_search", "exec", "process"]);
  });

  it("uses agent tool sections when resolving implicit exposure", () => {
    const cfg = {
      tools: {
        profile: "messaging",
      },
      agents: {
        list: [
          {
            id: "coder",
            tools: {
              fs: { workspaceOnly: true },
            },
          },
        ],
      },
    } as OpenClawConfig;
    const result = resolveEffectiveToolPolicy({ config: cfg, agentId: "coder" });
    expect(result.profileAlsoAllow).toEqual(["read", "write", "edit"]);
  });
});

describe("getToolAllowlistForRole", () => {
  it("returns an allowlist for project manager", () => {
    const allowlist = getToolAllowlistForRole("project manager");
    expect(allowlist).toBeDefined();
    expect(allowlist!.has("read")).toBe(true);
    expect(allowlist!.has("sessions_spawn")).toBe(true);
    expect(allowlist!.has("subagents")).toBe(true);
    expect(allowlist!.has("memory_search")).toBe(true);
    expect(allowlist!.has("exec")).toBe(false);
    expect(allowlist!.has("write")).toBe(false);
  });

  it("returns an allowlist for developer", () => {
    const allowlist = getToolAllowlistForRole("developer");
    expect(allowlist).toBeDefined();
    expect(allowlist!.has("read")).toBe(true);
    expect(allowlist!.has("write")).toBe(true);
    expect(allowlist!.has("exec")).toBe(true);
    expect(allowlist!.has("sessions_spawn")).toBe(false);
    expect(allowlist!.has("browser")).toBe(false);
  });

  it("returns an allowlist for backend lead", () => {
    const allowlist = getToolAllowlistForRole("backend lead");
    expect(allowlist).toBeDefined();
    expect(allowlist!.has("read")).toBe(true);
    expect(allowlist!.has("grep")).toBe(true);
    expect(allowlist!.has("write")).toBe(false);
    expect(allowlist!.has("exec")).toBe(false);
  });

  it("returns an allowlist for frontend lead", () => {
    const allowlist = getToolAllowlistForRole("frontend lead");
    expect(allowlist).toBeDefined();
    expect(allowlist!.has("read")).toBe(true);
    expect(allowlist!.has("find")).toBe(true);
    expect(allowlist!.has("edit")).toBe(false);
  });

  it("returns an allowlist for domain auditor", () => {
    const allowlist = getToolAllowlistForRole("domain auditor");
    expect(allowlist).toBeDefined();
    expect(allowlist!.has("read")).toBe(true);
    expect(allowlist!.has("exec")).toBe(true);
    expect(allowlist!.has("write")).toBe(false);
  });

  it("returns an allowlist for integration auditor", () => {
    const allowlist = getToolAllowlistForRole("integration auditor");
    expect(allowlist).toBeDefined();
    expect(allowlist!.has("read")).toBe(true);
    expect(allowlist!.has("exec")).toBe(true);
    expect(allowlist!.has("write")).toBe(false);
    expect(allowlist!.has("edit")).toBe(false);
  });

  it("returns undefined for unknown roles", () => {
    expect(getToolAllowlistForRole("unknown role")).toBeUndefined();
  });
});

describe("resolveSubagentToolPolicyForSession with team roles", () => {
  const baseCfg = {
    agents: { defaults: { subagents: { maxSpawnDepth: 2 } } },
  } as unknown as OpenClawConfig;

  afterEach(() => {
    resetSessionTeamRolesForTests();
  });

  it("uses team role allowlist for project manager", () => {
    const sessionKey = "agent:main:subagent:pm-test";
    setSessionTeamRole(sessionKey, "project manager");
    const policy = resolveSubagentToolPolicyForSession(baseCfg, sessionKey);
    expect(isToolAllowedByPolicyName("read", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("sessions_spawn", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("subagents", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("memory_search", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("session_status", policy)).toBe(true);
    // PM should NOT have write/exec/edit
    expect(isToolAllowedByPolicyName("write", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("exec", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("edit", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("browser", policy)).toBe(false);
  });

  it("uses team role allowlist for developer", () => {
    const sessionKey = "agent:main:subagent:dev-test";
    setSessionTeamRole(sessionKey, "developer");
    const policy = resolveSubagentToolPolicyForSession(baseCfg, sessionKey);
    expect(isToolAllowedByPolicyName("read", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("write", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("exec", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("image", policy)).toBe(true);
    // Developer should NOT have sessions_spawn/subagents
    expect(isToolAllowedByPolicyName("sessions_spawn", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("subagents", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("browser", policy)).toBe(false);
  });

  it("uses team role allowlist for backend lead", () => {
    const sessionKey = "agent:main:subagent:be-lead-test";
    setSessionTeamRole(sessionKey, "backend lead");
    const policy = resolveSubagentToolPolicyForSession(baseCfg, sessionKey);
    expect(isToolAllowedByPolicyName("read", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("grep", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("find", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("ls", policy)).toBe(true);
    // Lead should NOT write/edit/exec
    expect(isToolAllowedByPolicyName("write", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("edit", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("exec", policy)).toBe(false);
  });

  it("falls back to standard policy when no team role is set", () => {
    const sessionKey = "agent:main:subagent:no-role-test";
    // No setSessionTeamRole call
    const policy = resolveSubagentToolPolicyForSession(baseCfg, sessionKey);
    // Standard subagent policy denies gateway, cron, etc.
    expect(isToolAllowedByPolicyName("gateway", policy)).toBe(false);
    expect(isToolAllowedByPolicyName("cron", policy)).toBe(false);
    // But allows read/write/exec (non-denied tools)
    expect(isToolAllowedByPolicyName("read", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("write", policy)).toBe(true);
  });

  it("PM role overrides default subagent deny for session_status and memory tools", () => {
    const sessionKey = "agent:main:subagent:pm-override-test";
    setSessionTeamRole(sessionKey, "project manager");
    const policy = resolveSubagentToolPolicyForSession(baseCfg, sessionKey);
    // These are normally denied for subagents but PM needs them
    expect(isToolAllowedByPolicyName("session_status", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("memory_search", policy)).toBe(true);
    expect(isToolAllowedByPolicyName("memory_get", policy)).toBe(true);
  });

  it("filters tools correctly with team role allowlist", () => {
    const sessionKey = "agent:main:subagent:filter-test";
    setSessionTeamRole(sessionKey, "backend lead");
    const policy = resolveSubagentToolPolicyForSession(baseCfg, sessionKey);
    const tools = [
      createStubTool("read"),
      createStubTool("write"),
      createStubTool("exec"),
      createStubTool("grep"),
      createStubTool("find"),
      createStubTool("browser"),
    ];
    const filtered = filterToolsByPolicy(tools, policy);
    expect(filtered.map((t) => t.name).toSorted()).toEqual(["find", "grep", "read"]);
  });
});
