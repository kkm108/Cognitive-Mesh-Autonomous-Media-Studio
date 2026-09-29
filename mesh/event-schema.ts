import { z } from 'zod';

// ─── Event Severity & Routing ────────────────────────────────────────
export const EventSeverity = z.enum(['info', 'warning', 'error', 'critical']);
export type EventSeverity = z.infer<typeof EventSeverity>;

export const ModelTier = z.enum([
  'frontier',     // GPT-4o, Claude Opus, Gemini Ultra
  'reasoning',    // DeepSeek-R1, Claude Sonnet
  'standard',     // GPT-4o-mini, Claude Haiku
  'fast',         // Gemini Flash, smaller models
]);
export type ModelTier = z.infer<typeof ModelTier>;

// ─── Core Event Schema ───────────────────────────────────────────────
export const MeshEvent = z.object({
  // Identity
  id: z.string().min(1),                    // nanoid-generated
  type: z.string().min(1),                  // e.g. "research.complete"
  version: z.literal('1.0').default('1.0'),

  // Routing
  source: z.string().min(1),                // node identifier that emitted this
  target: z.string().optional(),            // null = broadcast, string = directed
  priority: z.number().int().min(0).max(10).default(5),

  // Temporal
  createdAt: z.string().datetime(),         // ISO 8601
  deadlineAt: z.string().datetime().optional(),
  completedAt: z.string().datetime().optional(),

  // Payload
  payload: z.record(z.unknown()),           // arbitrary structured data
  metadata: z.object({
    correlationId: z.string(),              // chains related events
    causationId: z.string(),                // the event that caused this one
    retryCount: z.number().int().min(0).default(0),
    maxRetries: z.number().int().min(0).default(3),
    requiredModelTier: ModelTier.optional(),
    severity: EventSeverity.default('info'),
  }),

  // Routing hints
  topics: z.array(z.string()).default([]),  // for pub/sub filtering
});
export type MeshEvent = z.infer<typeof MeshEvent>;

// ─── Event Type Catalog ──────────────────────────────────────────────
export const EventType = {
  // System
  MESH_HEALTH: 'mesh.health',
  MESH_SHUTDOWN: 'mesh.shutdown',

  // Trigger
  TOPIC_INJECTED: 'topic.injected',
  TREND_DETECTED: 'trend.detected',

  // Research
  RESEARCH_REQUEST: 'research.request',
  RESEARCH_PROGRESS: 'research.progress',
  RESEARCH_COMPLETE: 'research.complete',
  RESEARCH_FAILED: 'research.failed',

  // Script
  SCRIPT_REQUEST: 'script.request',
  SCRIPT_PROGRESS: 'script.progress',
  SCRIPT_FINALIZED: 'script.finalized',
  SCRIPT_FAILED: 'script.failed',

  // Visual
  VISUAL_REQUEST: 'visual.request',
  VISUAL_PROGRESS: 'visual.progress',
  VISUAL_COMPLETE: 'visual.complete',
  VISUAL_FAILED: 'visual.failed',

  // Assembly
  ASSEMBLY_REQUEST: 'assembly.request',
  ASSEMBLY_PROGRESS: 'assembly.progress',
  ASSEMBLY_COMPLETE: 'assembly.complete',
  ASSEMBLY_FAILED: 'assembly.failed',

  // Publish
  PUBLISH_REQUEST: 'publish.request',
  PUBLISH_COMPLETE: 'publish.complete',
  PUBLISH_FAILED: 'publish.failed',

  // Pipeline completion
  PIPELINE_COMPLETE: 'pipeline.complete',
} as const;
export type EventType = typeof EventType[keyof typeof EventType];

// ─── Event Factory ───────────────────────────────────────────────────
export function createEvent(
  type: string,
  source: string,
  payload: Record<string, unknown>,
  opts: Partial<Pick<MeshEvent, 'target' | 'priority' | 'topics' | 'deadlineAt'>> & {
    correlationId?: string;
    causationId?: string;
    retryCount?: number;
  } = {}
): MeshEvent {
  const now = new Date().toISOString();
  return {
    id: crypto.randomUUID(),
    type,
    version: '1.0',
    source,
    target: opts.target ?? undefined,
    priority: opts.priority ?? 5,
    createdAt: now,
    deadlineAt: opts.deadlineAt ?? undefined,
    payload,
    metadata: {
      correlationId: opts.correlationId ?? opts.target ?? crypto.randomUUID(),
      causationId: opts.causationId ?? '',
      retryCount: opts.retryCount ?? 0,
      maxRetries: 3,
      severity: 'info',
    },
    topics: opts.topics ?? [],
  };
}

// ─── Pipeline Context (correlation chain) ────────────────────────────
export const PipelineContext = z.object({
  id: z.string(),
  seedTopic: z.string(),
  researchData: z.record(z.unknown()).optional(),
  scriptData: z.record(z.unknown()).optional(),
  visualData: z.record(z.unknown()).optional(),
  assemblyData: z.record(z.unknown()).optional(),
  publishData: z.record(z.unknown()).optional(),
  status: z.enum(['pending', 'active', 'failed', 'completed']).default('active'),
  failureLog: z.array(z.object({
    nodeId: z.string(),
    error: z.string(),
    timestamp: z.string(),
    recoveryAttempt: z.number(),
  })).default([]),
});
export type PipelineContext = z.infer<typeof PipelineContext>;
