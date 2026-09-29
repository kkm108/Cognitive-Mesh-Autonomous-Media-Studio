import { z } from 'zod';

// ─── Model Definitions ───────────────────────────────────────────────
export const ModelCapability = z.enum([
  'reasoning',
  'creativity',
  'speed',
  'cost_efficiency',
  'long_context',
  'vision',
  'code_generation',
]);
export type ModelCapability = z.infer<typeof ModelCapability>;

export const ModelDef = z.object({
  id: z.string(),
  provider: z.string(),             // openai, anthropic, google, deepseek, local
  tier: z.enum(['frontier', 'reasoning', 'standard', 'fast']),
  capabilities: z.array(ModelCapability),
  costPer1kTokens: z.number(),      // USD
  maxTokens: z.number(),
  avgLatencyMs: z.number(),
  reliability: z.number().min(0).max(1),  // historical success rate
  mcpServers: z.array(z.string()).default([]),  // MCP servers this model can access
});
export type ModelDef = z.infer<typeof ModelDef>;

// ─── Task Classification ─────────────────────────────────────────────
export const TaskProfile = z.object({
  taskType: z.string(),             // e.g. "research.synthesize", "script.dialogue"
  complexityScore: z.number().min(0).max(1),  // 0=simple, 1=complex
  requiredCapabilities: z.array(ModelCapability),
  maxTokensNeeded: z.number().default(4096),
  latencyBudgetMs: z.number().default(30_000),
  costBudgetUsd: z.number().default(0.50),
});
export type TaskProfile = z.infer<typeof TaskProfile>;

// ─── Routing Decision ────────────────────────────────────────────────
export const RoutingDecision = z.object({
  selectedModel: ModelDef,
  score: z.number(),                // 0-1 confidence
  alternatives: z.array(ModelDef).max(3),
  reasoning: z.string(),
  estimatedCostUsd: z.number(),
  estimatedLatencyMs: z.number(),
});
export type RoutingDecision = z.infer<typeof RoutingDecision>;

// ─── Model Registry & Router ─────────────────────────────────────────
export class ModelRouter {
  private models: ModelDef[] = [];
  private usageHistory: Map<string, { successes: number; failures: number; avgLatency: number }> = new Map();

  register(model: ModelDef): void {
    this.models.push(model);
    this.usageHistory.set(model.id, { successes: 0, failures: 0, avgLatency: model.avgLatencyMs });
  }

  /**
   * Route a task to the best-fit model.
   * Scoring: capability_match * reliability * inverse_cost * inverse_latency
   */
  route(profile: TaskProfile): RoutingDecision {
    const scored = this.models.map((m) => {
      const capabilityMatch = this.scoreCapabilities(m, profile.requiredCapabilities);
      const withinBudget = this.checkBudget(m, profile);
      if (!withinBudget) return { model: m, score: 0 };

      const history = this.usageHistory.get(m.id) ?? { successes: 0, failures: 0, avgLatency: m.avgLatencyMs };
      const reliability = history.successes + history.failures === 0
        ? m.reliability
        : history.successes / (history.successes + history.failures);

      const latencyScore = Math.max(0, 1 - (m.avgLatencyMs / profile.latencyBudgetMs));
      const costScore = Math.max(0, 1 - (m.costPer1kTokens / (profile.costBudgetUsd / (profile.maxTokensNeeded / 1000))));

      // Weighted composite
      const score =
        capabilityMatch * 0.35 +
        reliability * 0.25 +
        costScore * 0.20 +
        latencyScore * 0.20;

      return { model: m, score };
    });

    scored.sort((a, b) => b.score - a.score);

    const best = scored[0];
    if (!best || best.score === 0) {
      throw new Error(`No model available for task profile: ${profile.taskType}`);
    }

    return {
      selectedModel: best.model,
      score: best.score,
      alternatives: scored.slice(1, 4).map((s) => s.model),
      reasoning: `Selected ${best.model.id} (score: ${best.score.toFixed(3)}) based on capability match, reliability, and budget constraints.`,
      estimatedCostUsd: (profile.maxTokensNeeded / 1000) * best.model.costPer1kTokens,
      estimatedLatencyMs: best.model.avgLatencyMs,
    };
  }

  /**
   * Record a completed call for adaptive routing.
   */
  recordUsage(modelId: string, success: boolean, latencyMs: number): void {
    const h = this.usageHistory.get(modelId);
    if (!h) return;

    if (success) h.successes++;
    else h.failures++;

    // Exponential moving average
    h.avgLatency = h.avgLatency * 0.8 + latencyMs * 0.2;
  }

  /**
   * Get a model by ID (for direct use).
   */
  getModel(id: string): ModelDef | undefined {
    return this.models.find((m) => m.id === id);
  }

  /**
   * List all registered models.
   */
  listModels(): ModelDef[] {
    return [...this.models];
  }

  // ─── Internal ────────────────────────────────────────────────────
  private scoreCapabilities(model: ModelDef, required: ModelCapability[]): number {
    if (required.length === 0) return 1;
    const matched = required.filter((c) => model.capabilities.includes(c)).length;
    return matched / required.length;
  }

  private checkBudget(model: ModelDef, profile: TaskProfile): boolean {
    const estCost = (profile.maxTokensNeeded / 1000) * model.costPer1kTokens;
    if (estCost > profile.costBudgetUsd) return false;
    if (model.avgLatencyMs > profile.latencyBudgetMs) return false;
    return true;
  }
}

// ─── Default Model Catalog ───────────────────────────────────────────
export function createDefaultRouter(): ModelRouter {
  const router = new ModelRouter();

  // Frontier tier
  router.register({
    id: 'gpt-4o',
    provider: 'openai',
    tier: 'frontier',
    capabilities: ['reasoning', 'creativity', 'code_generation', 'vision', 'long_context'],
    costPer1kTokens: 0.0025,
    maxTokens: 128_000,
    avgLatencyMs: 2500,
    reliability: 0.96,
    mcpServers: ['perplexity-mcp', 'brave-search'],
  });

  router.register({
    id: 'claude-opus-4',
    provider: 'anthropic',
    tier: 'frontier',
    capabilities: ['reasoning', 'creativity', 'code_generation', 'long_context'],
    costPer1kTokens: 0.015,
    maxTokens: 200_000,
    avgLatencyMs: 3500,
    reliability: 0.97,
    mcpServers: ['perplexity-mcp'],
  });

  // Reasoning tier
  router.register({
    id: 'deepseek-r1',
    provider: 'deepseek',
    tier: 'reasoning',
    capabilities: ['reasoning', 'code_generation'],
    costPer1kTokens: 0.00055,
    maxTokens: 64_000,
    avgLatencyMs: 4000,
    reliability: 0.92,
    mcpServers: [],
  });

  // Standard tier
  router.register({
    id: 'gpt-4o-mini',
    provider: 'openai',
    tier: 'standard',
    capabilities: ['creativity', 'speed', 'cost_efficiency'],
    costPer1kTokens: 0.00015,
    maxTokens: 16_000,
    avgLatencyMs: 800,
    reliability: 0.98,
    mcpServers: [],
  });

  router.register({
    id: 'claude-haiku',
    provider: 'anthropic',
    tier: 'standard',
    capabilities: ['speed', 'cost_efficiency', 'creativity'],
    costPer1kTokens: 0.00025,
    maxTokens: 200_000,
    avgLatencyMs: 600,
    reliability: 0.99,
    mcpServers: [],
  });

  // Fast tier
  router.register({
    id: 'gemini-flash',
    provider: 'google',
    tier: 'fast',
    capabilities: ['speed', 'cost_efficiency'],
    costPer1kTokens: 0.000075,
    maxTokens: 1_000_000,
    avgLatencyMs: 400,
    reliability: 0.97,
    mcpServers: [],
  });

  return router;
}
