import { randomUUID } from "node:crypto";
import {
  initSubagentRegistry,
  isSubagentSessionRunActive,
} from "../../agents/subagent-registry.js";
import type { CliDeps } from "../../cli/deps.js";
import { loadConfig, type OpenClawConfig } from "../../config/config.js";
import { resolveMainSessionKeyFromConfig } from "../../config/sessions.js";
import { runCronIsolatedAgentTurn } from "../../cron/isolated-agent.js";
import type { CronJob } from "../../cron/types.js";
import { requestHeartbeatNow } from "../../infra/heartbeat-wake.js";
import { enqueueSystemEvent } from "../../infra/system-events.js";
import type { createSubsystemLogger } from "../../logging/subsystem.js";
import { isSubagentSessionKey } from "../../routing/session-key.js";
import { registerBranch, unregisterBranch } from "../branch-registry.js";
import {
  normalizeHookDispatchSessionKey,
  type HookAgentDispatchPayload,
  type HooksConfigResolved,
} from "../hooks.js";
import { createHooksRequestHandler, type HookClientIpConfig } from "../server-http.js";

type SubsystemLogger = ReturnType<typeof createSubsystemLogger>;

export function resolveHookClientIpConfig(cfg: OpenClawConfig): HookClientIpConfig {
  return {
    trustedProxies: cfg.gateway?.trustedProxies,
    allowRealIpFallback: cfg.gateway?.allowRealIpFallback === true,
  };
}

export function createGatewayHooksRequestHandler(params: {
  deps: CliDeps;
  getHooksConfig: () => HooksConfigResolved | null;
  getClientIpConfig: () => HookClientIpConfig;
  bindHost: string;
  port: number;
  logHooks: SubsystemLogger;
}) {
  const { deps, getHooksConfig, getClientIpConfig, bindHost, port, logHooks } = params;

  const dispatchWakeHook = (value: { text: string; mode: "now" | "next-heartbeat" }) => {
    const sessionKey = resolveMainSessionKeyFromConfig();
    enqueueSystemEvent(value.text, { sessionKey });
    if (value.mode === "now") {
      requestHeartbeatNow({ reason: "hook:wake" });
    }
  };

  const dispatchAgentHook = (value: HookAgentDispatchPayload) => {
    let sessionKey = normalizeHookDispatchSessionKey({
      sessionKey: value.sessionKey,
      targetAgentId: value.agentId,
    });
    const mainSessionKey = resolveMainSessionKeyFromConfig();

    // Session-aware routing: inject into live subagent sessions directly.
    if (isSubagentSessionKey(sessionKey)) {
      initSubagentRegistry();
      if (isSubagentSessionRunActive(sessionKey)) {
        const dispatchContext = value.dispatch
          ? `\n\n[Dispatch metadata: ${JSON.stringify(value.dispatch)}]`
          : "";
        const eventText = `[Hook: ${value.name}] ${value.message}${dispatchContext}`;
        enqueueSystemEvent(eventText, { sessionKey });
        requestHeartbeatNow({ reason: `hook:${value.name}`, sessionKey });
        return randomUUID();
      }
      // Subagent session is dead — clean up registry entry if a branch was being tracked.
      const deadBranch =
        typeof value.dispatch?.branch === "string" ? value.dispatch.branch.trim() : "";
      if (deadBranch) {
        try {
          unregisterBranch(deadBranch);
        } catch {
          // best-effort cleanup
        }
      }
      // Fall back to main session.
      sessionKey = mainSessionKey;
    }

    const jobId = randomUUID();
    const now = Date.now();
    const job: CronJob = {
      id: jobId,
      agentId: value.agentId,
      name: value.name,
      enabled: true,
      createdAtMs: now,
      updatedAtMs: now,
      schedule: { kind: "at", at: new Date(now).toISOString() },
      sessionTarget: "isolated",
      wakeMode: value.wakeMode,
      payload: {
        kind: "agentTurn",
        message: value.message,
        model: value.model,
        thinking: value.thinking,
        timeoutSeconds: value.timeoutSeconds,
        deliver: value.deliver,
        channel: value.channel,
        to: value.to,
        allowUnsafeExternalContent: value.allowUnsafeExternalContent,
      },
      state: { nextRunAtMs: now },
    };

    const runId = randomUUID();
    void (async () => {
      try {
        const cfg = loadConfig();
        const result = await runCronIsolatedAgentTurn({
          cfg,
          deps,
          job,
          message: value.message,
          sessionKey,
          lane: "cron",
          deliveryContract: "shared",
        });
        // Register branch in the registry when dispatch includes a branch.
        const spawnBranch =
          typeof value.dispatch?.branch === "string" ? value.dispatch.branch.trim() : "";
        if (spawnBranch && result.sessionKey) {
          try {
            registerBranch(spawnBranch, {
              sessionKey: result.sessionKey,
              prNumber:
                typeof value.dispatch?.prNumber === "number"
                  ? Math.floor(value.dispatch.prNumber)
                  : undefined,
              repo:
                typeof value.dispatch?.repo === "string" && value.dispatch.repo.trim()
                  ? value.dispatch.repo.trim()
                  : undefined,
              createdAt: new Date().toISOString(),
              status: "watching",
            });
          } catch {
            // best-effort registry write
          }
        }
        const summary = result.summary?.trim() || result.error?.trim() || result.status;
        const prefix =
          result.status === "ok" ? `Hook ${value.name}` : `Hook ${value.name} (${result.status})`;
        if (!result.delivered) {
          enqueueSystemEvent(`${prefix}: ${summary}`.trim(), {
            sessionKey: mainSessionKey,
          });
          if (value.wakeMode === "now") {
            requestHeartbeatNow({ reason: `hook:${jobId}` });
          }
        }
      } catch (err) {
        logHooks.warn(`hook agent failed: ${String(err)}`);
        enqueueSystemEvent(`Hook ${value.name} (error): ${String(err)}`, {
          sessionKey: mainSessionKey,
        });
        if (value.wakeMode === "now") {
          requestHeartbeatNow({ reason: `hook:${jobId}:error` });
        }
      }
    })();

    return runId;
  };

  return createHooksRequestHandler({
    getHooksConfig,
    bindHost,
    port,
    logHooks,
    getClientIpConfig,
    dispatchAgentHook,
    dispatchWakeHook,
  });
}
