import { MeshEvent, PipelineContext, EventType, createEvent } from './event-schema.js';
import { logger } from './logger.js';

/**
 * PipelineStore tracks the state of each content pipeline.
 * Stores pipeline context, handles state transitions, and provides
 * traceability from seed topic to published output.
 *
 * Backed by a Map (swappable for Redis/Postgres in production).
 */
export class PipelineStore {
  private pipelines = new Map<string, PipelineContext>();

  /**
   * Create a new pipeline context from a seed topic.
   */
  create(seedTopic: string): PipelineContext {
    const ctx: PipelineContext = {
      id: crypto.randomUUID(),
      seedTopic,
      status: 'active',
      failureLog: [],
    };
    this.pipelines.set(ctx.id, ctx);
    logger.info({ pipelineId: ctx.id, topic: seedTopic }, 'Pipeline created');
    return ctx;
  }

  /**
   * Get a pipeline by ID.
   */
  get(id: string): PipelineContext | undefined {
    return this.pipelines.get(id);
  }

  /**
   * Update a pipeline's state based on a mesh event.
   * This is the central state machine for the content pipeline.
   */
  transition(event: MeshEvent): PipelineContext | undefined {
    const ctx = this.pipelines.get(event.metadata.correlationId);
    if (!ctx) {
      logger.warn({ eventId: event.id, correlationId: event.metadata.correlationId }, 'Pipeline not found for event');
      return undefined;
    }

    switch (event.type) {
      case EventType.RESEARCH_COMPLETE:
        ctx.researchData = event.payload;
        ctx.status = 'active';
        break;

      case EventType.SCRIPT_FINALIZED:
        ctx.scriptData = event.payload;
        ctx.status = 'active';
        break;

      case EventType.VISUAL_COMPLETE:
        ctx.visualData = event.payload;
        ctx.status = 'active';
        break;

      case EventType.ASSEMBLY_COMPLETE:
        ctx.assemblyData = event.payload;
        ctx.status = 'active';
        break;

      case EventType.PUBLISH_COMPLETE:
        ctx.publishData = event.payload;
        ctx.status = 'completed';
        break;

      case EventType.RESEARCH_FAILED:
      case EventType.SCRIPT_FAILED:
      case EventType.VISUAL_FAILED:
      case EventType.ASSEMBLY_FAILED:
      case EventType.PUBLISH_FAILED: {
        const nodeId = event.source;
        const error = (event.payload as { error?: string }).error ?? 'Unknown error';
        ctx.failureLog.push({
          nodeId,
          error,
          timestamp: event.createdAt,
          recoveryAttempt: event.metadata.retryCount,
        });

        if (event.metadata.retryCount >= event.metadata.maxRetries) {
          ctx.status = 'failed';
          logger.error(
            { pipelineId: ctx.id, nodeId, error, retries: event.metadata.retryCount },
            'Pipeline node exhausted retries'
          );
        }
        break;
      }
    }

    this.pipelines.set(ctx.id, ctx);
    return ctx;
  }

  /**
   * List all pipelines with optional status filter.
   */
  list(status?: PipelineContext['status']): PipelineContext[] {
    const all = Array.from(this.pipelines.values());
    if (status) return all.filter((p) => p.status === status);
    return all;
  }

  /**
   * Remove completed or failed pipelines (garbage collection).
   */
  gc(): number {
    let count = 0;
    for (const [id, ctx] of this.pipelines) {
      if (ctx.status === 'completed' || ctx.status === 'failed') {
        this.pipelines.delete(id);
        count++;
      }
    }
    return count;
  }

  health(): { total: number; active: number; failed: number; completed: number } {
    const all = Array.from(this.pipelines.values());
    return {
      total: all.length,
      active: all.filter((p) => p.status === 'active').length,
      failed: all.filter((p) => p.status === 'failed').length,
      completed: all.filter((p) => p.status === 'completed').length,
    };
  }
}
