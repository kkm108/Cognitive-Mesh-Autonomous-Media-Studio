import { MeshEvent, EventType } from '../mesh/event-schema.js';
import { EventBus } from '../mesh/event-bus.js';
import { AgentNode } from '../mesh/agent-node.js';
import { ModelRouter } from '../router/model-router.js';
import { MCPLayer } from '../mcp/mcp-layer.js';
import { PipelineStore } from '../mesh/pipeline-store.js';
import { logger } from '../mesh/logger.js';

/**
 * TEMPLATE: Extensible Node — Trend Forecasting Node (example).
 *
 * This file demonstrates the extensibility contract.
 * Copy it, change the event subscriptions and logic, and hot-swap via:
 *
 *   await mesh.registerNode(new TrendForecastingNode(bus, router, mcp, pipelines));
 *
 * No changes to EventBus, PipelineStore, Supervisor, or other nodes required.
 *
 * Contract to implement:
 *  1. Extend AgentNode
 *  2. Implement registerHandlers() — subscribe to whatever events you care about
 *  3. Implement deregisterHandlers() — unsubscribe on shutdown
 *  4. Use this.emitProgress / this.emitComplete / this.emitFailed (pipelineId, eventType, data)
 *  5. Use this.executeTask() for concurrency + health tracking
 */
export class TrendForecastingNode extends AgentNode {
  private router: ModelRouter;
  private mcp: MCPLayer;
  private pipelines: PipelineStore;
  private subIds: string[] = [];

  constructor(bus: EventBus, router: ModelRouter, mcp: MCPLayer, pipelines: PipelineStore) {
    super(bus, {
      id: 'trend-forecasting-node',
      name: 'trend',
      capabilities: ['trend_prediction', 'forecasting', 'audience_modeling'],
      maxConcurrency: 2,
      healthCheckIntervalMs: 30_000,
    });
    this.router = router;
    this.mcp = mcp;
    this.pipelines = pipelines;
  }

  protected async registerHandlers(): Promise<void> {
    // Example: listen for research completion and enrich with forecast
    this.subIds.push(
      this.bus.subscribe(this.config.id, EventType.RESEARCH_COMPLETE, (e) => this.handleResearchComplete(e))
    );

    // Could also listen to trend.detected, publish.complete, etc.
  }

  protected async deregisterHandlers(): Promise<void> {
    this.subIds.forEach((id) => this.bus.unsubscribe(id));
    this.subIds = [];
  }

  private async handleResearchComplete(event: MeshEvent): Promise<void> {
    const payload = event.payload as { topic: string; pipelineId?: string };
    const pipelineId = payload.pipelineId ?? event.metadata.correlationId;

    logger.info({ pipelineId }, 'TrendForecastingNode: enriching with forecast');

    const result = await this.executeTask(
      () => this.forecast(payload.topic, pipelineId),
      pipelineId,
      (err) => this.emitFailed(pipelineId, 'trend.failed', err.message)
    );

    if (result) {
      this.emitComplete(pipelineId, 'trend.forecast.complete', {
        forecast: result,
        pipelineId,
      });
    }
  }

  private async forecast(topic: string, pipelineId: string): Promise<Record<string, unknown>> {
    this.emitProgress(pipelineId, { stage: 'forecasting', topic });

    // Use router to pick a model for forecasting (high-reasoning task)
    const decision = this.router.route({
      taskType: 'trend.forecast',
      complexityScore: 0.75,
      requiredCapabilities: ['reasoning'],
      maxTokensNeeded: 4000,
      latencyBudgetMs: 10_000,
      costBudgetUsd: 0.05,
    });

    logger.info({ model: decision.selectedModel.id, topic }, 'Forecasting with routed model');

    // Placeholder — in production, call the model + MCP trend data
    return {
      topic,
      predictedVirality: 0.82,
      bestPublishWindow: '2026-08-29T14:00:00Z',
      suggestedHashtags: ['#trending', `#${topic.replace(/\s+/g, '')}`],
      modelUsed: decision.selectedModel.id,
    };
  }
}

/**
 * TEMPLATE: Engagement Analytics Node (second example).
 * Listens for publish.complete and emits engagement predictions.
 * Same contract — zero infra changes.
 */
export class EngagementAnalyticsNode extends AgentNode {
  private subIds: string[] = [];

  constructor(bus: EventBus) {
    super(bus, {
      id: 'engagement-analytics-node',
      name: 'analytics',
      capabilities: ['engagement_prediction', 'a_b_testing'],
      maxConcurrency: 5,
      healthCheckIntervalMs: 30_000,
    });
  }

  protected async registerHandlers(): Promise<void> {
    this.subIds.push(
      this.bus.subscribe(this.config.id, EventType.PUBLISH_COMPLETE, (e) => this.handlePublish(e))
    );
  }

  protected async deregisterHandlers(): Promise<void> {
    this.subIds.forEach((id) => this.bus.unsubscribe(id));
    this.subIds = [];
  }

  private async handlePublish(event: MeshEvent): Promise<void> {
    const pipelineId = event.metadata.correlationId;
    await this.executeTask(async () => {
      this.emitProgress(pipelineId, { stage: 'analyzing', pipelineId });
      // placeholder analytics
      this.emitComplete(pipelineId, 'analytics.engagement.complete', {
        pipelineId,
        predictedViews: 12500,
        predictedCtr: 0.041,
      });
    }, pipelineId);
  }
}
