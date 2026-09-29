import { MeshEvent, EventType, createEvent, PipelineContext } from './event-schema.js';
import { EventBus } from './event-bus.js';
import { logger } from './logger.js';

interface NodeConfig {
  id: string;
  name: string;
  capabilities: string[];
  maxConcurrency: number;
  healthCheckIntervalMs: number;
}

interface NodeHealth {
  nodeId: string;
  isAlive: boolean;
  lastHeartbeat: string;
  tasksRunning: number;
  tasksCompleted: number;
  tasksFailed: number;
  uptimeMs: number;
}

/**
 * Base class for all mesh nodes.
 * Provides lifecycle management, health reporting, and event handling.
 */
export abstract class AgentNode {
  protected bus: EventBus;
  protected config: NodeConfig;
  protected health: NodeHealth;
  protected startTime: number;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;

  constructor(bus: EventBus, config: NodeConfig) {
    this.bus = bus;
    this.config = config;
    this.startTime = Date.now();
    this.health = {
      nodeId: config.id,
      isAlive: true,
      lastHeartbeat: new Date().toISOString(),
      tasksRunning: 0,
      tasksCompleted: 0,
      tasksFailed: 0,
      uptimeMs: 0,
    };
  }

  /**
   * Start the node: register subscriptions, begin heartbeat.
   */
  async start(): Promise<void> {
    logger.info({ nodeId: this.config.id, name: this.config.name }, 'Node starting');

    // Register event handlers
    await this.registerHandlers();

    // Start heartbeat
    this.heartbeatTimer = setInterval(() => {
      this.health.uptimeMs = Date.now() - this.startTime;
      this.health.lastHeartbeat = new Date().toISOString();
      this.bus.publish(createEvent(EventType.MESH_HEALTH, this.config.id, {
        health: { ...this.health },
      }, { topics: ['health'] }));
    }, this.config.healthCheckIntervalMs);

    this.health.isAlive = true;
    logger.info({ nodeId: this.config.id }, 'Node started');
  }

  /**
   * Gracefully shut down the node.
   */
  async stop(): Promise<void> {
    logger.info({ nodeId: this.config.id }, 'Node shutting down');
    this.health.isAlive = false;
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    await this.deregisterHandlers();
    logger.info({ nodeId: this.config.id }, 'Node stopped');
  }

  /**
   * Emit a progress event. Does NOT affect health counters.
   */
  protected emitProgress(pipelineId: string, data: Record<string, unknown>): void {
    const eventType = `${this.config.name.toLowerCase()}.progress` as EventType;
    this.bus.publish(createEvent(eventType, this.config.id, { ...data, pipelineId }, {
      correlationId: pipelineId,
      topics: ['progress', this.config.name.toLowerCase()],
    }));
  }

  /**
   * Emit a completion event. Health counters are managed by executeTask,
   * so this only publishes.
   */
  protected emitComplete(pipelineId: string, eventType: string, data: Record<string, unknown>): void {
    this.bus.publish(createEvent(eventType, this.config.id, { ...data, pipelineId }, {
      correlationId: pipelineId,
      priority: 8,
      topics: ['complete', this.config.name.toLowerCase()],
    }));
  }

  /**
   * Emit a failure event. Health counters are managed by executeTask.
   */
  protected emitFailed(pipelineId: string, eventType: string, error: string, retryCount = 0): void {
    this.bus.publish(createEvent(eventType, this.config.id, { error, pipelineId }, {
      correlationId: pipelineId,
      priority: 9,
      retryCount,
      topics: ['failed', this.config.name.toLowerCase()],
    }));
  }

  /**
   * Subclasses must implement: register their event handlers.
   */
  protected abstract registerHandlers(): Promise<void>;

  /**
   * Subclasses must implement: deregister their event handlers.
   */
  protected abstract deregisterHandlers(): Promise<void>;

  /**
   * Execute a task with concurrency control and failure handling.
   * Single source of truth for health counters — emit methods do NOT touch counters.
   */
  protected async executeTask<T>(
    fn: () => Promise<T>,
    pipelineId: string,
    errorHandler?: (err: Error) => void
  ): Promise<T | null> {
    if (this.health.tasksRunning >= this.config.maxConcurrency) {
      logger.warn({ nodeId: this.config.id }, 'Concurrency limit reached, queuing task');
      await new Promise((r) => setTimeout(r, 1000));
      return this.executeTask(fn, pipelineId, errorHandler);
    }

    this.health.tasksRunning++;
    try {
      const result = await fn();
      this.health.tasksCompleted++;
      return result;
    } catch (err) {
      this.health.tasksFailed++;
      const msg = err instanceof Error ? err.message : String(err);
      logger.error({ nodeId: this.config.id, pipelineId, error: msg }, 'Task execution failed');
      errorHandler?.(err instanceof Error ? err : new Error(msg));
      return null;
    } finally {
      this.health.tasksRunning--;
    }
  }

  getHealth(): NodeHealth {
    return { ...this.health };
  }

  getConfig(): NodeConfig {
    return { ...this.config };
  }
}
