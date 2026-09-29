import { EventEmitter } from 'node:events';
import { MeshEvent, createEvent } from './event-schema.js';
import { logger } from './logger.js';

type Subscription = {
  id: string;
  pattern: string | RegExp;
  callback: (event: MeshEvent) => void | Promise<void>;
  filter?: (event: MeshEvent) => boolean;
};

type DeadLetter = {
  event: MeshEvent;
  error: string;
  attempts: number;
  deadLetteredAt: string;
};

export class EventBus {
  private emitter = new EventEmitter();
  private subscriptions = new Map<string, Subscription>();
  private deadLetters: DeadLetter[] = [];
  private eventLog: MeshEvent[] = [];
  private maxLogSize: number;

  constructor(opts: { maxLogSize?: number } = {}) {
    this.maxLogSize = opts.maxLogSize ?? 10_000;
    this.emitter.setMaxListeners(200);
  }

  /**
   * Publish an event to the bus. All matching subscribers are notified.
   */
  async publish(event: MeshEvent): Promise<void> {
    // Log the event
    this.eventLog.push(event);
    if (this.eventLog.length > this.maxLogSize) {
      this.eventLog = this.eventLog.slice(-this.maxLogSize);
    }

    logger.info({ eventId: event.id, type: event.type, source: event.source }, 'Event published');

    // Route to all matching subscribers
    // Note: target is for directed delivery (nodeId). If set, only that node's subscriptions receive it.
    // Otherwise, pattern-match all.
    const matched = Array.from(this.subscriptions.values()).filter((sub) => {
      if (event.target && event.target !== sub.id) return false;

      if (typeof sub.pattern === 'string') {
        return this.matchPattern(sub.pattern, event.type);
      }
      return sub.pattern.test(event.type);
    });

    const notifications = matched.map(async (sub) => {
      if (sub.filter && !sub.filter(event)) return;
      try {
        await sub.callback(event);
      } catch (err) {
        logger.error({ subscriptionId: sub.id, eventId: event.id, err }, 'Subscriber callback failed');
        this.handleDeadLetter(event, err);
      }
    });

    await Promise.allSettled(notifications);
  }

  /**
   * Subscribe to events matching a pattern.
   * Pattern supports wildcards: "research.*" matches "research.complete"
   */
  subscribe(
    id: string,
    pattern: string | RegExp,
    callback: (event: MeshEvent) => void | Promise<void>,
    filter?: (event: MeshEvent) => boolean
  ): string {
    const subId = `${id}:${crypto.randomUUID()}`;
    this.subscriptions.set(subId, { id, pattern, callback, filter });
    logger.debug({ subscriptionId: subId, pattern: String(pattern) }, 'Subscription registered');
    return subId;
  }

  /**
   * Subscribe to a single event type (exact match).
   */
  on(
    eventType: string,
    callback: (event: MeshEvent) => void | Promise<void>
  ): string {
    return this.subscribe(eventType, eventType, callback);
  }

  /**
   * Remove a subscription.
   */
  unsubscribe(subId: string): boolean {
    const removed = this.subscriptions.delete(subId);
    if (removed) logger.debug({ subscriptionId: subId }, 'Subscription removed');
    return removed;
  }

  /**
   * Replay events matching a pattern (for late-joining nodes).
   */
  replay(pattern: string, callback: (event: MeshEvent) => void | Promise<void>): void {
    const re = new RegExp(`^${pattern.replace(/\*/g, '.*')}$`);
    const matching = this.eventLog.filter((e) => re.test(e.type));
    matching.forEach((e) => callback(e));
  }

  /**
   * Get events by correlation ID (trace a pipeline).
   */
  getCorrelated(correlationId: string): MeshEvent[] {
    return this.eventLog.filter((e) => e.metadata.correlationId === correlationId);
  }

  /**
   * Get dead-lettered events for inspection.
   */
  getDeadLetters(): DeadLetter[] {
    return [...this.deadLetters];
  }

  /**
   * Retry a dead-lettered event.
   */
  async retryDeadLetter(index: number): Promise<boolean> {
    const dl = this.deadLetters[index];
    if (!dl) return false;

    dl.event.metadata.retryCount++;
    if (dl.event.metadata.retryCount >= dl.event.metadata.maxRetries) {
      logger.warn({ eventId: dl.event.id }, 'Max retries exceeded, not re-publishing');
      return false;
    }

    this.deadLetters.splice(index, 1);
    await this.publish(dl.event);
    return true;
  }

  /**
   * Emit a health snapshot.
   */
  health(): {
    subscriptions: number;
    eventLogSize: number;
    deadLetters: number;
  } {
    return {
      subscriptions: this.subscriptions.size,
      eventLogSize: this.eventLog.length,
      deadLetters: this.deadLetters.length,
    };
  }

  // ─── Internal ────────────────────────────────────────────────────
  private matchPattern(pattern: string, eventType: string): boolean {
    if (pattern === '*') return true;
    if (!pattern.includes('*')) return pattern === eventType;

    const re = new RegExp(`^${pattern.replace(/\./g, '\\.').replace(/\*/g, '.*')}$`);
    return re.test(eventType);
  }

  private handleDeadLetter(event: MeshEvent, error: unknown): void {
    const msg = error instanceof Error ? error.message : String(error);
    logger.warn({ eventId: event.id, error: msg, retryCount: event.metadata.retryCount }, 'Dead-lettering event');

    this.deadLetters.push({
      event,
      error: msg,
      attempts: event.metadata.retryCount + 1,
      deadLetteredAt: new Date().toISOString(),
    });
  }
}
