import { EventBus } from './event-bus.js';
import { EventType, MeshEvent, createEvent } from './event-schema.js';
import { PipelineStore } from './pipeline-store.js';
import { AgentNode } from './agent-node.js';
import { logger } from './logger.js';

interface SupervisorConfig {
  healthTimeoutMs: number;      // consider node dead after this silence
  checkIntervalMs: number;      // how often to sweep
  maxRetries: number;
  taskSplitThreshold: number;   // failure count before splitting task
}

interface NodeRecord {
  node: AgentNode;
  lastHeartbeat: number;
  failureCount: number;
  restartCount: number;
  status: 'alive' | 'suspect' | 'dead';
}

/**
 * MeshSupervisor — self-healing control plane (NOT an orchestrator).
 *
 * It does NOT route tasks. Tasks flow via PubSub (event bus).
 * The supervisor only observes and heals:
 *  - watches health heartbeats; if a node goes silent, restarts it
 *  - watches failure events; retries with backoff, or splits task into parallel subtasks
 *  - maintains dead-letter renegotiation
 *
 * New nodes can be hot-swapped: `registerNode()` at runtime, no core changes.
 */
export class MeshSupervisor {
  private bus: EventBus;
  private pipelines: PipelineStore;
  private nodes = new Map<string, NodeRecord>();
  private config: SupervisorConfig;
  private sweepTimer: ReturnType<typeof setInterval> | null = null;
  private subIds: string[] = [];

  constructor(bus: EventBus, pipelines: PipelineStore, config?: Partial<SupervisorConfig>) {
    this.bus = bus;
    this.pipelines = pipelines;
    this.config = {
      healthTimeoutMs: config?.healthTimeoutMs ?? 90_000,
      checkIntervalMs: config?.checkIntervalMs ?? 15_000,
      maxRetries: config?.maxRetries ?? 3,
      taskSplitThreshold: config?.taskSplitThreshold ?? 2,
    };
  }

  // ─── Lifecycle ───────────────────────────────────────────────────
  async start(): Promise<void> {
    logger.info('Supervisor starting — self-healing enabled');

    // Watch health heartbeats
    this.subIds.push(
      this.bus.subscribe('supervisor', EventType.MESH_HEALTH, (e) => this.onHealth(e))
    );

    // Watch failures to renegotiate
    this.subIds.push(
      this.bus.subscribe('supervisor', '*.failed', (e) => this.onFailure(e))
    );

    // Watch dead-letters via pipeline store transitions
    this.sweepTimer = setInterval(() => this.sweep(), this.config.checkIntervalMs);

    logger.info({ config: this.config }, 'Supervisor started');
  }

  async stop(): Promise<void> {
    if (this.sweepTimer) clearInterval(this.sweepTimer);
    this.subIds.forEach((id) => this.bus.unsubscribe(id));
    logger.info('Supervisor stopped');
  }

  // ─── Node Registry (extensibility) ───────────────────────────────
  /**
   * Hot-swap: inject a new node at runtime. No core infra change needed.
   * Example: supervisor.registerNode(new TrendForecastingNode(bus, router, mcp, pipelines))
   */
  registerNode(node: AgentNode): void {
    const id = node.getConfig().id;
    this.nodes.set(id, {
      node,
      lastHeartbeat: Date.now(),
      failureCount: 0,
      restartCount: 0,
      status: 'alive',
    });
    logger.info({ nodeId: id }, 'Supervisor: node registered (hot-swapped)');
  }

  deregisterNode(nodeId: string): void {
    this.nodes.delete(nodeId);
    logger.info({ nodeId }, 'Supervisor: node deregistered');
  }

  // ─── Health Handling ─────────────────────────────────────────────
  private onHealth(event: MeshEvent): void {
    const nodeId = event.source;
    const record = this.nodes.get(nodeId);
    if (!record) {
      // Unknown node — auto-register as observed (supports external nodes)
      logger.info({ nodeId }, 'Supervisor: discovered new node via heartbeat');
      return;
    }
    record.lastHeartbeat = Date.now();
    record.status = 'alive';
    record.failureCount = 0;
  }

  private async sweep(): Promise<void> {
    const now = Date.now();
    for (const [id, record] of this.nodes) {
      const silence = now - record.lastHeartbeat;

      if (silence > this.config.healthTimeoutMs && record.status !== 'dead') {
        record.status = silence > this.config.healthTimeoutMs * 1.5 ? 'dead' : 'suspect';
        logger.warn({ nodeId: id, silenceMs: silence, status: record.status }, 'Supervisor: node health degraded');

        if (record.status === 'dead') {
          await this.healNode(id, record);
        }
      }
    }

    // Also sweep dead pipelines for retry
    this.retryDeadPipelines();
  }

  private async healNode(nodeId: string, record: NodeRecord): Promise<void> {
    record.restartCount++;

    if (record.restartCount > this.config.maxRetries) {
      logger.error({ nodeId, restarts: record.restartCount }, 'Supervisor: node exceeded restart limit — isolating');
      record.status = 'dead';
      // Emit alert; in production, page on-call or scale redundant node
      await this.bus.publish(createEvent('mesh.node.isolated', 'supervisor', {
        nodeId,
        reason: 'restart_limit_exceeded',
        restarts: record.restartCount,
      }));
      return;
    }

    logger.info({ nodeId, attempt: record.restartCount }, 'Supervisor: restarting dead node');

    try {
      await record.node.stop();
      await new Promise((r) => setTimeout(r, 2000)); // cool-down
      await record.node.start();
      record.status = 'alive';
      record.lastHeartbeat = Date.now();
      logger.info({ nodeId }, 'Supervisor: node healed');
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      logger.error({ nodeId, error: msg }, 'Supervisor: node restart failed');
      // Exponential backoff before next attempt — handled by next sweep
    }
  }

  // ─── Failure Renegotiation ───────────────────────────────────────
  private async onFailure(event: MeshEvent): Promise<void> {
    const pipelineId = event.metadata.correlationId ?? (event.payload as { pipelineId?: string }).pipelineId;
    if (!pipelineId) return;

    const pipeline = this.pipelines.get(pipelineId);
    const retryCount = event.metadata.retryCount ?? 0;

    logger.warn({ pipelineId, type: event.type, retryCount }, 'Supervisor: failure detected, renegotiating');

    // Strategy 1: simple retry with backoff (first failure)
    if (retryCount < this.config.taskSplitThreshold) {
      const backoffMs = Math.pow(2, retryCount) * 1000;
      logger.info({ pipelineId, backoffMs }, 'Supervisor: scheduling retry with backoff');

      setTimeout(async () => {
        const retryEvent = createEvent(
          event.type.replace('.failed', '.retry'),
          'supervisor',
          { ...event.payload as object, pipelineId, originalError: (event.payload as { error?: string }).error },
          { priority: 7 }
        );
        // Preserve correlation
        (retryEvent.metadata as unknown as Record<string, unknown>).correlationId = pipelineId;
        (retryEvent.metadata as unknown as Record<string, unknown>).causationId = event.id;
        (retryEvent.metadata as unknown as Record<string, unknown>).retryCount = retryCount + 1;
        await this.bus.publish(retryEvent);
      }, backoffMs);
      return;
    }

    // Strategy 2: task decomposition — split into parallel simpler operations
    if (retryCount >= this.config.taskSplitThreshold) {
      logger.info({ pipelineId }, 'Supervisor: splitting failed task into parallel subtasks');
      await this.decomposeTask(event, pipelineId);
    }
  }

  /**
   * Break a failed task into smaller parallel operations.
   * Example: if visual generation failed on a 60s video, split into 3x20s segments.
   */
  private async decomposeTask(failedEvent: MeshEvent, pipelineId: string): Promise<void> {
    const taskType = failedEvent.type.replace('.failed', '');

    // Emit parallel subtask requests
    const subtaskCount = 3;
    for (let i = 0; i < subtaskCount; i++) {
      const subtask = createEvent(
        `${taskType}.request`,
        'supervisor',
        {
          ...failedEvent.payload as object,
          pipelineId,
          subtaskIndex: i,
          subtaskTotal: subtaskCount,
          decomposed: true,
          originalError: (failedEvent.payload as { error?: string }).error,
        },
        { priority: 6 }
      );
      (subtask.metadata as unknown as Record<string, unknown>).correlationId = pipelineId;
      (subtask.metadata as unknown as Record<string, unknown>).causationId = failedEvent.id;
      await this.bus.publish(subtask);
    }

    logger.info({ pipelineId, taskType, subtaskCount }, 'Supervisor: decomposed task emitted');
  }

  private retryDeadPipelines(): void {
    const dead = this.bus.getDeadLetters();
    for (let i = 0; i < dead.length; i++) {
      const dl = dead[i];
      if (dl.attempts < this.config.maxRetries) {
        logger.info({ eventId: dl.event.id, attempts: dl.attempts }, 'Supervisor: retrying dead-letter');
        this.bus.retryDeadLetter(i);
      }
    }
  }

  // ─── Observability ───────────────────────────────────────────────
  health(): { nodes: number; alive: number; suspect: number; dead: number; deadLetters: number } {
    const records = Array.from(this.nodes.values());
    return {
      nodes: records.length,
      alive: records.filter((r) => r.status === 'alive').length,
      suspect: records.filter((r) => r.status === 'suspect').length,
      dead: records.filter((r) => r.status === 'dead').length,
      deadLetters: this.bus.getDeadLetters().length,
    };
  }
}
