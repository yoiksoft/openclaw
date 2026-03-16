/**
 * Lightweight in-memory store that maps child session keys to their
 * team role (e.g. "project manager", "developer"). Used by the bootstrap
 * file filter to apply a stricter allowlist for team-role subagents.
 *
 * Written by spawn code, read by bootstrap filter — kept separate to
 * avoid circular dependencies between workspace.ts and subagent-spawn.ts.
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
