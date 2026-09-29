import { z } from 'zod';
import { logger } from '../mesh/logger.js';

// ─── MCP Server Definition ───────────────────────────────────────────
export const MCPServerDef = z.object({
  name: z.string(),
  command: z.string(),            // e.g. "npx", "node", "python"
  args: z.array(z.string()).default([]),
  env: z.record(z.string()).default({}),
  transport: z.enum(['stdio', 'sse']).default('stdio'),
  permissions: z.array(z.string()).default(['read', 'write']),
  timeout: z.number().default(30_000),
});
export type MCPServerDef = z.infer<typeof MCPServerDef>;

// ─── MCP Tool Call ───────────────────────────────────────────────────
export const MCPToolCall = z.object({
  server: z.string(),
  method: z.string(),
  params: z.record(z.unknown()).default({}),
  timeout: z.number().optional(),
});
export type MCPToolCall = z.infer<typeof MCPToolCall>;

export const MCPToolResult = z.object({
  success: z.boolean(),
  data: z.unknown().optional(),
  error: z.string().optional(),
  durationMs: z.number(),
  server: z.string(),
  method: z.string(),
});
export type MCPToolResult = z.infer<typeof MCPToolResult>;

/**
 * MCP Integration Layer.
 * Manages MCP server connections and routes tool calls to the appropriate servers.
 *
 * NOTE: In production, this spawns actual MCP server processes via stdio/SSE.
 * For the foundational blueprint, it provides the abstraction and a registry
 * that nodes discover dynamically.
 */
export class MCPLayer {
  private servers = new Map<string, MCPServerDef>();
  private connections = new Map<string, boolean>(); // server -> connected

  /**
   * Register an MCP server. Applies zod defaults for optional fields.
   */
  registerServer(input: z.input<typeof MCPServerDef>): void {
    const server = MCPServerDef.parse(input);
    this.servers.set(server.name, server);
    this.connections.set(server.name, false);
    logger.info({ server: server.name }, 'MCP server registered');
  }

  /**
   * Connect to all registered servers.
   * In production, this spawns the server process and establishes the transport.
   */
  async connectAll(): Promise<void> {
    const tasks = Array.from(this.servers.entries()).map(async ([name, server]) => {
      try {
        // Production: spawn process, connect transport
        // await this.spawnServer(server);
        this.connections.set(name, true);
        logger.info({ server: name }, 'MCP server connected');
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.error({ server: name, error: msg }, 'MCP server connection failed');
        this.connections.set(name, false);
      }
    });
    await Promise.allSettled(tasks);
  }

  /**
   * Execute a tool call via an MCP server.
   * Validates permissions, routes to the correct server, handles timeout.
   */
  async execute(call: MCPToolCall, requiredPermission: string = 'read'): Promise<MCPToolResult> {
    const server = this.servers.get(call.server);
    if (!server) {
      return { success: false, error: `MCP server '${call.server}' not found`, durationMs: 0, ...call };
    }

    if (!this.connections.get(call.server)) {
      return { success: false, error: `MCP server '${call.server}' not connected`, durationMs: 0, ...call };
    }

    if (!server.permissions.includes(requiredPermission)) {
      return {
        success: false,
        error: `MCP server '${call.server}' lacks permission '${requiredPermission}'`,
        durationMs: 0,
        ...call,
      };
    }

    const timeout = call.timeout ?? server.timeout;
    const start = Date.now();

    try {
      // Production: send via transport (stdio/SSE), await response
      // const result = await this.sendToServer(call.server, call.method, call.params, timeout);

      // Placeholder for actual MCP transport
      const result = await this.executeViaTransport(server, call.method, call.params, timeout);

      const durationMs = Date.now() - start;
      logger.info({ server: call.server, method: call.method, durationMs }, 'MCP tool call succeeded');

      return { success: true, data: result, durationMs, server: call.server, method: call.method };
    } catch (err) {
      const durationMs = Date.now() - start;
      const msg = err instanceof Error ? err.message : String(err);
      logger.error({ server: call.server, method: call.method, error: msg, durationMs }, 'MCP tool call failed');

      return { success: false, error: msg, durationMs, server: call.server, method: call.method };
    }
  }

  /**
   * Discover which MCP servers are available for a given capability.
   */
  discoverByCapability(permission: string): MCPServerDef[] {
    return Array.from(this.servers.values()).filter(
      (s) => s.permissions.includes(permission) && this.connections.get(s.name)
    );
  }

  /**
   * Get health status of all MCP servers.
   */
  health(): Array<{ name: string; connected: boolean; permissions: string[] }> {
    return Array.from(this.servers.entries()).map(([name, server]) => ({
      name,
      connected: this.connections.get(name) ?? false,
      permissions: server.permissions,
    }));
  }

  /**
   * Disconnect all servers.
   */
  async disconnectAll(): Promise<void> {
    for (const [name] of this.servers) {
      this.connections.set(name, false);
      logger.info({ server: name }, 'MCP server disconnected');
    }
  }

  // ─── Internal (Transport Layer) ──────────────────────────────────
  private async executeViaTransport(
    server: MCPServerDef,
    method: string,
    params: Record<string, unknown>,
    timeout: number
  ): Promise<unknown> {
    // In production, this would:
    // 1. For stdio: write JSON-RPC message to server.stdin, read from server.stdout
    // 2. For SSE: send HTTP POST to server's SSE endpoint
    //
    // This is a placeholder that should be replaced with actual transport code.

    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        reject(new Error(`MCP call timed out after ${timeout}ms`));
      }, timeout);

      // Simulate async MCP call resolution
      // In production: pipe through stdio or HTTP
      clearTimeout(timer);
      resolve({ status: 'placeholder', message: 'MCP transport not yet connected' });
    });
  }
}

// ─── Default MCP Server Catalog ──────────────────────────────────────
export function createDefaultMCPLayer(): MCPLayer {
  const layer = new MCPLayer();

  layer.registerServer({
    name: 'perplexity-mcp',
    command: 'npx',
    args: ['-y', '@modelcontextprotocol/server-perplexity'],
    permissions: ['search', 'read'],
    timeout: 15_000,
  });

  layer.registerServer({
    name: 'brave-search',
    command: 'npx',
    args: ['-y', '@modelcontextprotocol/server-brave-search'],
    env: { BRAVE_API_KEY: process.env.BRAVE_API_KEY ?? '' },
    permissions: ['search', 'read'],
    timeout: 10_000,
  });

  layer.registerServer({
    name: 'pexels',
    command: 'npx',
    args: ['-y', 'mcp-server-pexels'],
    env: { PEXELS_API_KEY: process.env.PEXELS_API_KEY ?? '' },
    permissions: ['search', 'read', 'download'],
    timeout: 20_000,
  });

  layer.registerServer({
    name: 'elevenlabs-tts',
    command: 'node',
    args: ['mcp/servers/elevenlabs.js'],
    env: { ELEVENLABS_API_KEY: process.env.ELEVENLABS_API_KEY ?? '' },
    permissions: ['write', 'generate'],
    timeout: 60_000,
  });

  layer.registerServer({
    name: 'ffmpeg-assembly',
    command: 'node',
    args: ['mcp/servers/ffmpeg.js'],
    permissions: ['read', 'write', 'transform'],
    timeout: 120_000,
  });

  layer.registerServer({
    name: 'youtube-publisher',
    command: 'node',
    args: ['mcp/servers/youtube.js'],
    env: { YOUTUBE_CREDENTIALS: process.env.YOUTUBE_CREDENTIALS ?? '' },
    permissions: ['read', 'write', 'publish'],
    timeout: 60_000,
  });

  layer.registerServer({
    name: 'instagram-publisher',
    command: 'node',
    args: ['mcp/servers/instagram.js'],
    env: { INSTAGRAM_TOKEN: process.env.INSTAGRAM_TOKEN ?? '' },
    permissions: ['read', 'write', 'publish'],
    timeout: 60_000,
  });

  return layer;
}
