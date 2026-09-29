import { MeshEvent, EventType, createEvent } from '../mesh/event-schema.js';
import { EventBus } from '../mesh/event-bus.js';
import { AgentNode } from '../mesh/agent-node.js';
import { ModelRouter, TaskProfile } from '../router/model-router.js';
import { MCPLayer } from '../mcp/mcp-layer.js';
import { PipelineStore } from '../mesh/pipeline-store.js';
import { logger } from '../mesh/logger.js';

/**
 * Research Agent Node.
 * Receives trending topics, synthesizes research from MCP search servers,
 * and emits research.complete with structured intelligence.
 *
 * Self-healing: if primary MCP search fails, falls back to secondary.
 * Dynamic routing: uses ModelRouter to pick the best model for synthesis.
 */
export class ResearchNode extends AgentNode {
  private router: ModelRouter;
  private mcp: MCPLayer;
  private pipelines: PipelineStore;
  private subIds: string[] = [];

  constructor(bus: EventBus, router: ModelRouter, mcp: MCPLayer, pipelines: PipelineStore) {
    super(bus, {
      id: 'research-node',
      name: 'research',
      capabilities: ['trend_analysis', 'web_search', 'synthesis', 'competitor_analysis'],
      maxConcurrency: 5,
      healthCheckIntervalMs: 30_000,
    });
    this.router = router;
    this.mcp = mcp;
    this.pipelines = pipelines;
  }

  protected async registerHandlers(): Promise<void> {
    // React to new topics being injected
    this.subIds.push(
      this.bus.subscribe(this.config.id, EventType.TOPIC_INJECTED, (e) => this.handleTopic(e))
    );

    // React to trend detection events
    this.subIds.push(
      this.bus.subscribe(this.config.id, EventType.TREND_DETECTED, (e) => this.handleTopic(e))
    );

    // Handle retry requests
    this.subIds.push(
      this.bus.subscribe(this.config.id, 'research.retry', (e) => this.handleRetry(e))
    );
  }

  protected async deregisterHandlers(): Promise<void> {
    this.subIds.forEach((id) => this.bus.unsubscribe(id));
    this.subIds = [];
  }

  // ─── Handlers ──────────────────────────────────────────────────────
  private async handleTopic(event: MeshEvent): Promise<void> {
    const payload = event.payload as { topic: string; pipelineId?: string };
    const pipelineId = payload.pipelineId ?? event.metadata.correlationId;
    const topic = payload.topic;
    if (!topic || !pipelineId) {
      logger.warn({ eventId: event.id }, 'ResearchNode: missing topic or pipelineId');
      return;
    }
    logger.info({ pipelineId, topic }, 'ResearchNode: processing topic');

    const result = await this.executeTask(
      () => this.conductResearch(topic, pipelineId),
      pipelineId,
      (err) => this.emitFailed(pipelineId, EventType.RESEARCH_FAILED, err.message, event.metadata.retryCount)
    );

    if (result) {
      this.emitComplete(pipelineId, EventType.RESEARCH_COMPLETE, {
        topic,
        researchData: result,
        completedAt: new Date().toISOString(),
      });
    }
  }

  private async handleRetry(event: MeshEvent): Promise<void> {
    const payload = event.payload as { topic: string; pipelineId?: string };
    const pipelineId = payload.pipelineId ?? event.metadata.correlationId;
    const topic = payload.topic;
    logger.warn({ pipelineId, topic, retry: event.metadata.retryCount }, 'ResearchNode: retry attempt');

    const result = await this.executeTask(
      () => this.conductResearchReduced(topic, pipelineId),
      pipelineId,
      (err) => this.emitFailed(pipelineId, EventType.RESEARCH_FAILED, err.message, event.metadata.retryCount)
    );

    if (result) {
      this.emitComplete(pipelineId, EventType.RESEARCH_COMPLETE, {
        topic,
        researchData: result,
        completedAt: new Date().toISOString(),
      });
    }
  }

  // ─── Core Research Logic ──────────────────────────────────────────
  private async conductResearch(
    topic: string,
    pipelineId: string
  ): Promise<Record<string, unknown>> {
    this.emitProgress(pipelineId, { stage: 'search', message: 'Querying search sources' });

    // Step 1: Multi-source search via MCP
    const searchData = await this.multiSourceSearch(topic);

    this.emitProgress(pipelineId, { stage: 'synthesis', message: 'Synthesizing findings' });

    // Step 2: Select best model for synthesis (high-reasoning task)
    const synthesisProfile: TaskProfile = {
      taskType: 'research.synthesize',
      complexityScore: 0.8,
      requiredCapabilities: ['reasoning', 'long_context'],
      maxTokensNeeded: 8000,
      latencyBudgetMs: 15_000,
      costBudgetUsd: 0.10,
    };

    const routing = this.router.route(synthesisProfile);

    // Step 3: Synthesize via routed model (would call model API here)
    const synthesis = await this.synthesizeWithModel(routing.selectedModel.id, topic, searchData);

    return {
      topic,
      sources: searchData.sources,
      keyInsights: synthesis.keyInsights,
      angle: synthesis.angle,
      competitorAngles: synthesis.competitorAngles,
      targetDemographic: synthesis.targetDemographic,
      modelUsed: routing.selectedModel.id,
      routingScore: routing.score,
      estimatedCost: routing.estimatedCostUsd,
    };
  }

  /**
   * Self-healing fallback: reduced search scope with single source.
   */
  private async conductResearchReduced(
    topic: string,
    pipelineId: string
  ): Promise<Record<string, unknown>> {
    this.emitProgress(pipelineId, { stage: 'reduced_search', message: 'Fallback: single-source search' });

    const searchData = await this.singleSourceSearch(topic);

    const synthesisProfile: TaskProfile = {
      taskType: 'research.synthesize.minimal',
      complexityScore: 0.5,
      requiredCapabilities: ['reasoning'],
      maxTokensNeeded: 4000,
      latencyBudgetMs: 10_000,
      costBudgetUsd: 0.05,
    };

    const routing = this.router.route(synthesisProfile);

    return {
      topic,
      sources: searchData.sources,
      keyInsights: searchData.keyInsights,
      angle: 'general_interest',
      competitorAngles: [],
      targetDemographic: 'general',
      modelUsed: routing.selectedModel.id,
      reducedMode: true,
    };
  }

  // ─── MCP Search Calls ─────────────────────────────────────────────
  private async multiSourceSearch(topic: string): Promise<{ sources: string[]; keyInsights: string[] }> {
    const sources: string[] = [];
    const keyInsights: string[] = [];

    // Try multiple MCP servers in parallel (failover)
    const searchServers = ['brave-search', 'perplexity-mcp'];
    const searchPromises = searchServers.map((server) =>
      this.mcp.execute(
        { server, method: 'search', params: { query: topic, count: 10 } },
        'search'
      ).catch((err) => {
        logger.warn({ server, error: err.message }, 'Search server failed, continuing with others');
        return null;
      })
    );

    const results = await Promise.allSettled(searchPromises);

    for (const r of results) {
      if (r.status === 'fulfilled' && r.value?.success) {
        const data = r.value.data as { results?: Array<{ title: string; snippet: string }> };
        if (data?.results) {
          for (const item of data.results) {
            sources.push(item.title);
            keyInsights.push(item.snippet);
          }
        }
      }
    }

    return { sources: [...new Set(sources)].slice(0, 15), keyInsights: [...new Set(keyInsights)].slice(0, 20) };
  }

  private async singleSourceSearch(topic: string): Promise<{ sources: string[]; keyInsights: string[] }> {
    const result = await this.mcp.execute(
      { server: 'brave-search', method: 'search', params: { query: topic, count: 5 } },
      'search'
    );

    if (result.success) {
      const data = result.data as { results?: Array<{ title: string; snippet: string }> };
      return {
        sources: data?.results?.map((r) => r.title) ?? [],
        keyInsights: data?.results?.map((r) => r.snippet) ?? [],
      };
    }

    return { sources: [], keyInsights: [] };
  }

  private async synthesizeWithModel(
    modelId: string,
    topic: string,
    searchData: { sources: string[]; keyInsights: string[] }
  ): Promise<{
    keyInsights: string[];
    angle: string;
    competitorAngles: string[];
    targetDemographic: string;
  }> {
    // In production, this calls the LLM via API with a synthesis prompt.
    // The model is selected dynamically by the router.
    logger.info({ modelId, topic }, 'Synthesizing research with model');

    // Placeholder: actual implementation calls the routed model
    return {
      keyInsights: searchData.keyInsights.slice(0, 10),
      angle: `Exploring the trending topic: ${topic}`,
      competitorAngles: [],
      targetDemographic: 'general_audience_18_34',
    };
  }
}
