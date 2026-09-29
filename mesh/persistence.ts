import { MeshEvent } from './event-schema.js';
import { logger } from './logger.js';

/**
 * Persistence Abstraction — swappable backing store.
 * In-memory by default, Redis when MESH_REDIS_URL is set.
 *
 * This module provides:
 *  - Pipeline persistence (create/get/set/list)
 *  - Event log persistence (append/getCorrelated)
 *  - Health check
 *
 * Nodes and EventBus remain agnostic — they call these interfaces.
 */

export interface PipelineRecord {
  id: string;
  seedTopic: string;
  researchData?: Record<string, unknown>;
  scriptData?: Record<string, unknown>;
  visualData?: Record<string, unknown>;
  assemblyData?: Record<string, unknown>;
  publishData?: Record<string, unknown>;
  status: string;
  failureLog: unknown[];
  updatedAt: string;
}

export interface Persistence {
  // Pipelines
  savePipeline(record: PipelineRecord): Promise<void>;
  getPipeline(id: string): Promise<PipelineRecord | null>;
  listPipelines(): Promise<PipelineRecord[]>;
  deletePipeline(id: string): Promise<void>;

  // Events
  appendEvent(event: MeshEvent): Promise<void>;
  getEventsByCorrelation(correlationId: string): Promise<MeshEvent[]>;
  getEventLog(limit: number): Promise<MeshEvent[]>;

  // Lifecycle
  health(): Promise<{ connected: boolean; backend: string }>;
  disconnect(): Promise<void>;
}

// ─── In-Memory (default) ───────────────────────────────────────────
export class InMemoryPersistence implements Persistence {
  private pipelines = new Map<string, PipelineRecord>();
  private events: MeshEvent[] = [];

  async savePipeline(r: PipelineRecord): Promise<void> {
    this.pipelines.set(r.id, { ...r, updatedAt: new Date().toISOString() });
  }
  async getPipeline(id: string): Promise<PipelineRecord | null> {
    return this.pipelines.get(id) ?? null;
  }
  async listPipelines(): Promise<PipelineRecord[]> {
    return Array.from(this.pipelines.values());
  }
  async deletePipeline(id: string): Promise<void> {
    this.pipelines.delete(id);
  }
  async appendEvent(e: MeshEvent): Promise<void> {
    this.events.push(e);
  }
  async getEventsByCorrelation(cid: string): Promise<MeshEvent[]> {
    return this.events.filter((e) => e.metadata.correlationId === cid);
  }
  async getEventLog(limit: number): Promise<MeshEvent[]> {
    return this.events.slice(-limit);
  }
  async health(): Promise<{ connected: boolean; backend: string }> {
    return { connected: true, backend: 'memory' };
  }
  async disconnect(): Promise<void> {}
}

// ─── Redis (optional) ──────────────────────────────────────────────
export class RedisPersistence implements Persistence {
  private client: any; // ioredis Redis instance (lazy import)
  private connected = false;

  constructor(private url: string) {}

  async init(): Promise<void> {
    try {
      const { Redis } = await import('ioredis');
      this.client = new Redis(this.url, {
        maxRetriesPerRequest: 2,
        lazyConnect: true,
        enableReadyCheck: true,
      });
      await this.client.connect();
      this.connected = true;
      logger.info({ url: this.url.replace(/:\/\/.*@/, '://***@') }, 'Redis persistence connected');
    } catch (err) {
      logger.warn({ err: String(err) }, 'Redis connect failed — falling back to memory');
      this.connected = false;
      throw err;
    }
  }

  async savePipeline(r: PipelineRecord): Promise<void> {
    if (!this.connected) return;
    await this.client.hset('mesh:pipelines', r.id, JSON.stringify(r));
    await this.client.hset('mesh:pipelines:meta', r.id, new Date().toISOString());
  }

  async getPipeline(id: string): Promise<PipelineRecord | null> {
    if (!this.connected) return null;
    const raw = await this.client.hget('mesh:pipelines', id);
    return raw ? JSON.parse(raw) : null;
  }

  async listPipelines(): Promise<PipelineRecord[]> {
    if (!this.connected) return [];
    const all = await this.client.hgetall('mesh:pipelines');
    return Object.values(all as Record<string, string>).map((v: string) => JSON.parse(v));
  }

  async deletePipeline(id: string): Promise<void> {
    if (!this.connected) return;
    await this.client.hdel('mesh:pipelines', id);
    await this.client.hdel('mesh:pipelines:meta', id);
  }

  async appendEvent(e: MeshEvent): Promise<void> {
    if (!this.connected) return;
    // Stream per correlation + global log
    await this.client.xadd('mesh:events', '*', 'data', JSON.stringify(e));
    await this.client.xadd(`mesh:events:${e.metadata.correlationId}`, '*', 'data', JSON.stringify(e));
    // Keep trim
    await this.client.xtrim('mesh:events', 'MAXLEN', '~', 20000);
  }

  async getEventsByCorrelation(cid: string): Promise<MeshEvent[]> {
    if (!this.connected) return [];
    const entries = await this.client.xrange(`mesh:events:${cid}`, '-', '+');
    return (entries as Array<[string, string[]]>).map(([, fields]) => {
      const idx = fields.indexOf('data');
      return JSON.parse(fields[idx + 1]);
    });
  }

  async getEventLog(limit: number): Promise<MeshEvent[]> {
    if (!this.connected) return [];
    const entries = await this.client.xrevrange('mesh:events', '+', '-', 'COUNT', limit);
    return (entries as Array<[string, string[]]>).map(([, fields]) => {
      const idx = fields.indexOf('data');
      return JSON.parse(fields[idx + 1]);
    }).reverse();
  }

  async health(): Promise<{ connected: boolean; backend: string }> {
    if (!this.connected) return { connected: false, backend: 'redis' };
    try {
      await this.client.ping();
      return { connected: true, backend: 'redis' };
    } catch {
      return { connected: false, backend: 'redis' };
    }
  }

  async disconnect(): Promise<void> {
    if (this.client) await this.client.quit().catch(() => {});
    this.connected = false;
  }
}

// ─── Factory ───────────────────────────────────────────────────────
export async function createPersistence(): Promise<Persistence> {
  const url = process.env.MESH_REDIS_URL ?? process.env.REDIS_URL;
  if (url) {
    const redis = new RedisPersistence(url);
    try {
      await redis.init();
      const h = await redis.health();
      if (h.connected) return redis;
    } catch {
      // fall through to memory
    }
    await redis.disconnect().catch(() => {});
    logger.warn('Redis unavailable — using in-memory persistence');
  }
  return new InMemoryPersistence();
}
