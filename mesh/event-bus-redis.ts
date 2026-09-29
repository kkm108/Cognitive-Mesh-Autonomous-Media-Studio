import { EventBus } from './event-bus.js';
import { MeshEvent } from './event-schema.js';
import { Persistence } from './persistence.js';
import { logger } from './logger.js';

/**
 * Redis-backed EventBus wrapper.
 * Extends in-memory EventBus with optional persistence + horizontal pub/sub.
 *
 * If Persistence is Redis-backed, every publish is also appended to Redis streams.
 * Subscriptions remain in-process (Node EventEmitter behavior) but the event log
 * is durable. For true cross-process PubSub, swap the internal emitter for Redis PubSub.
 *
 * Usage:
 *   const persistence = await createPersistence(); // memory or redis
 *   const bus = new PersistentEventBus(persistence);
 */
export class PersistentEventBus extends EventBus {
  constructor(private persistence: Persistence, opts: { maxLogSize?: number } = {}) {
    super(opts);
  }

  override async publish(event: MeshEvent): Promise<void> {
    await super.publish(event);
    // Durable log (best-effort — do not fail the pipeline if Redis is down)
    this.persistence.appendEvent(event).catch((err) => {
      logger.warn({ err: String(err), eventId: event.id }, 'Persistence appendEvent failed');
    });
  }

  async replayFromPersistence(
    correlationId: string,
    callback: (event: MeshEvent) => void | Promise<void>
  ): Promise<void> {
    const events = await this.persistence.getEventsByCorrelation(correlationId);
    for (const e of events) await callback(e);
  }
}
