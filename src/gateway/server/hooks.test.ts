import { afterEach, beforeEach, describe, expect, test, vi } from "vitest";

// Mock modules before importing the module under test.
vi.mock("../../routing/session-key.js", async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    isSubagentSessionKey: vi.fn(),
  };
});

vi.mock("../../agents/subagent-registry.js", async (importOriginal) => {
  const actual = await importOriginal();
  return {
    ...actual,
    initSubagentRegistry: vi.fn(),
    isSubagentSessionRunActive: vi.fn(),
  };
});

vi.mock("../../infra/system-events.js", () => ({
  enqueueSystemEvent: vi.fn(),
}));

vi.mock("../../infra/heartbeat-wake.js", () => ({
  requestHeartbeatNow: vi.fn(),
}));

vi.mock("../../config/sessions.js", () => ({
  resolveMainSessionKeyFromConfig: vi.fn(() => "signal:direct:+14168999152"),
}));

vi.mock("../../config/config.js", () => ({
  loadConfig: vi.fn(() => ({})),
}));

vi.mock("../../cron/isolated-agent.js", () => ({
  runCronIsolatedAgentTurn: vi.fn(async () => ({
    status: "ok",
    summary: "done",
    delivered: true,
  })),
}));

import {
  initSubagentRegistry,
  isSubagentSessionRunActive,
} from "../../agents/subagent-registry.js";
import { runCronIsolatedAgentTurn } from "../../cron/isolated-agent.js";
import { requestHeartbeatNow } from "../../infra/heartbeat-wake.js";
import { enqueueSystemEvent } from "../../infra/system-events.js";
// Import after mocks are set up.
import { isSubagentSessionKey } from "../../routing/session-key.js";
import type { HookAgentDispatchPayload } from "../hooks.js";

describe("dispatchAgentHook session-aware routing", () => {
  const mockedIsSubagentSessionKey = vi.mocked(isSubagentSessionKey);
  const mockedIsSubagentSessionRunActive = vi.mocked(isSubagentSessionRunActive);
  const mockedInitSubagentRegistry = vi.mocked(initSubagentRegistry);
  const mockedEnqueueSystemEvent = vi.mocked(enqueueSystemEvent);
  const mockedRequestHeartbeatNow = vi.mocked(requestHeartbeatNow);
  const mockedRunCronIsolatedAgentTurn = vi.mocked(runCronIsolatedAgentTurn);

  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  /**
   * Helper: call dispatchAgentHook by going through `createGatewayHooksRequestHandler`
   * and intercepting the `createHooksRequestHandler` call.
   *
   * Since `dispatchAgentHook` is a closure, we hook into `createHooksRequestHandler`
   * to capture it.
   */
  async function callDispatchAgentHook(
    payload: HookAgentDispatchPayload,
  ): Promise<{ runId: string }> {
    // We need to spy on createHooksRequestHandler to capture the dispatchAgentHook closure.
    const serverHttpModule = await import("../server-http.js");
    let capturedDispatchFn: ((value: HookAgentDispatchPayload) => string) | undefined;

    const originalCreate = serverHttpModule.createHooksRequestHandler;
    vi.spyOn(serverHttpModule, "createHooksRequestHandler").mockImplementation((opts: never) => {
      const typedOpts = opts as { dispatchAgentHook: (value: HookAgentDispatchPayload) => string };
      capturedDispatchFn = typedOpts.dispatchAgentHook;
      return originalCreate(opts);
    });

    const { createGatewayHooksRequestHandler } = await import("./hooks.js");

    createGatewayHooksRequestHandler({
      deps: {} as never,
      getHooksConfig: () => ({
        basePath: "/hooks",
        token: "test-token",
        maxBodyBytes: 256 * 1024,
        mappings: [],
        agentPolicy: {
          defaultAgentId: "main",
          knownAgentIds: new Set(["main"]),
        },
        sessionPolicy: {
          allowRequestSessionKey: true,
        },
      }),
      getClientIpConfig: () => ({}),
      bindHost: "127.0.0.1",
      port: 18789,
      logHooks: { info: vi.fn(), warn: vi.fn(), error: vi.fn(), debug: vi.fn() } as never,
    });

    if (!capturedDispatchFn) {
      throw new Error("Failed to capture dispatchAgentHook from closure");
    }

    const runId = capturedDispatchFn(payload);
    return { runId };
  }

  const basePayload: HookAgentDispatchPayload = {
    message: "PR #42 was merged",
    name: "GitHub",
    agentId: "main",
    wakeMode: "now",
    sessionKey: "subagent:pm-abc123",
    deliver: true,
    channel: "last",
  };

  test("injects system event for alive subagent session", async () => {
    mockedIsSubagentSessionKey.mockReturnValue(true);
    mockedIsSubagentSessionRunActive.mockReturnValue(true);

    const { runId } = await callDispatchAgentHook(basePayload);

    // Should have initialized registry and checked liveness.
    expect(mockedInitSubagentRegistry).toHaveBeenCalled();
    expect(mockedIsSubagentSessionRunActive).toHaveBeenCalledWith("subagent:pm-abc123");

    // Should inject system event with correct session key.
    expect(mockedEnqueueSystemEvent).toHaveBeenCalledWith("[Hook: GitHub] PR #42 was merged", {
      sessionKey: "subagent:pm-abc123",
    });

    // Should wake the specific session.
    expect(mockedRequestHeartbeatNow).toHaveBeenCalledWith({
      reason: "hook:GitHub",
      sessionKey: "subagent:pm-abc123",
    });

    // Should return a UUID.
    expect(runId).toMatch(/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/);

    // Should NOT create an isolated cron turn.
    expect(mockedRunCronIsolatedAgentTurn).not.toHaveBeenCalled();
  });

  test("includes dispatch metadata in system event text", async () => {
    mockedIsSubagentSessionKey.mockReturnValue(true);
    mockedIsSubagentSessionRunActive.mockReturnValue(true);

    const dispatch = { branch: "feature/ratings", registryHit: true, contextDocPath: "/tmp/x.md" };
    await callDispatchAgentHook({ ...basePayload, dispatch });

    const eventText = mockedEnqueueSystemEvent.mock.calls[0]?.[0];
    expect(eventText).toContain("[Hook: GitHub]");
    expect(eventText).toContain("PR #42 was merged");
    expect(eventText).toContain("[Dispatch metadata:");
    expect(eventText).toContain('"branch":"feature/ratings"');
    expect(eventText).toContain('"registryHit":true');
  });

  test("falls back to main session for dead subagent", async () => {
    mockedIsSubagentSessionKey.mockReturnValue(true);
    mockedIsSubagentSessionRunActive.mockReturnValue(false);

    await callDispatchAgentHook(basePayload);

    // Should check liveness.
    expect(mockedInitSubagentRegistry).toHaveBeenCalled();
    expect(mockedIsSubagentSessionRunActive).toHaveBeenCalledWith("subagent:pm-abc123");

    // Should NOT inject a system event for the subagent.
    // Instead it creates an isolated turn (which calls runCronIsolatedAgentTurn).
    expect(mockedRunCronIsolatedAgentTurn).toHaveBeenCalled();

    // The isolated turn should use the main session key (fallback).
    const callArgs = mockedRunCronIsolatedAgentTurn.mock.calls[0]?.[0] as {
      sessionKey: string;
    };
    expect(callArgs.sessionKey).toBe("signal:direct:+14168999152");
  });

  test("uses existing behavior for non-subagent session keys", async () => {
    mockedIsSubagentSessionKey.mockReturnValue(false);

    await callDispatchAgentHook({
      ...basePayload,
      sessionKey: "hook:abc123",
    });

    // Should not check subagent registry.
    expect(mockedInitSubagentRegistry).not.toHaveBeenCalled();
    expect(mockedIsSubagentSessionRunActive).not.toHaveBeenCalled();

    // Should create an isolated cron turn.
    expect(mockedRunCronIsolatedAgentTurn).toHaveBeenCalled();

    // Should use the original session key.
    const callArgs = mockedRunCronIsolatedAgentTurn.mock.calls[0]?.[0] as {
      sessionKey: string;
    };
    expect(callArgs.sessionKey).toBe("hook:abc123");
  });
});
