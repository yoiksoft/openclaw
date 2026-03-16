/**
 * Lightweight in-memory store that maps child session keys to their
 * team role (e.g. "project manager", "developer"). Used by the bootstrap
 * file filter to apply a stricter allowlist for team-role subagents,
 * and by tool filtering to restrict tools per role.
 *
 * Written by spawn code, read by bootstrap filter and tool policy —
 * kept separate to avoid circular dependencies between workspace.ts
 * and subagent-spawn.ts.
 */
const sessionTeamRoles = new Map<string, string>();

export function setSessionTeamRole(sessionKey: string, role: string): void {
  sessionTeamRoles.set(sessionKey, role);
}

export function getSessionTeamRole(sessionKey: string): string | undefined {
  return sessionTeamRoles.get(sessionKey);
}

export function clearSessionTeamRole(sessionKey: string): void {
  sessionTeamRoles.delete(sessionKey);
}

/** Reset for tests only. */
export function resetSessionTeamRolesForTests(): void {
  sessionTeamRoles.clear();
}

/**
 * Tool allowlists per team role. When a subagent has a team role,
 * only tools in the returned set are permitted. Returns undefined
 * for unknown roles (no filtering applied).
 */
const ROLE_TOOL_ALLOWLISTS: Record<string, ReadonlySet<string>> = {
  "project manager": new Set([
    "read",
    "web_search",
    "web_fetch",
    "sessions_spawn",
    "subagents",
    "sessions_yield",
    "session_status",
    "memory_search",
    "memory_get",
  ]),
  "backend lead": new Set([
    "read",
    "grep",
    "find",
    "ls",
    "web_search",
    "web_fetch",
    "session_status",
  ]),
  "frontend lead": new Set([
    "read",
    "grep",
    "find",
    "ls",
    "web_search",
    "web_fetch",
    "session_status",
  ]),
  developer: new Set([
    "read",
    "write",
    "edit",
    "exec",
    "process",
    "apply_patch",
    "grep",
    "find",
    "ls",
    "web_search",
    "web_fetch",
    "session_status",
    "image",
  ]),
  "domain auditor": new Set([
    "read",
    "exec",
    "process",
    "grep",
    "find",
    "ls",
    "web_search",
    "web_fetch",
    "session_status",
  ]),
  "integration auditor": new Set([
    "read",
    "exec",
    "process",
    "grep",
    "find",
    "ls",
    "web_search",
    "web_fetch",
    "session_status",
  ]),
};

/**
 * Returns the tool allowlist for a given team role, or undefined if
 * the role is not recognized (no filtering should be applied).
 */
export function getToolAllowlistForRole(role: string): ReadonlySet<string> | undefined {
  return ROLE_TOOL_ALLOWLISTS[role];
}
