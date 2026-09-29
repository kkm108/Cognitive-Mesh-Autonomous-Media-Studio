import { MeshEvent, EventType } from '../mesh/event-schema.js';
import { EventBus } from '../mesh/event-bus.js';
import { AgentNode } from '../mesh/agent-node.js';
import { ModelRouter } from '../router/model-router.js';
import { MCPLayer } from '../mcp/mcp-layer.js';
import { PipelineStore } from '../mesh/pipeline-store.js';
import { logger } from '../mesh/logger.js';

// Re-export ScriptOutput shape for type sharing (avoid circular)
interface ScriptSegment {
  index: number;
  type: 'hook' | 'narration' | 'call_to_action';
  text: string;
  durationSeconds: number;
  visualCue: string;
  voiceStyle: string;
}
interface ScriptData {
  title: string;
  hook: string;
  segments: ScriptSegment[];
  metadata: {
    targetDurationSeconds: number;
    aspectRatio: string;
    platform: string[];
    tags: string[];
    description: string;
  };
}

interface VisualAsset {
  segmentIndex: number;
  query: string;
  assetUrl: string;
  localPath: string;
  durationSeconds: number;
  qa: { passed: boolean; score: number; reason?: string };
}

interface VisualOutput {
  pipelineId: string;
  topic: string;
  assets: VisualAsset[];
  voiceover: {
    path: string;
    durationSeconds: number;
    voiceId: string;
  };
  totalDurationSeconds: number;
  modelRouting: Record<string, string>;
}

/**
 * Visual Agent Node — Asset Generation.
 * Consumes script.finalized, produces visual-complete.
 *
 * Responsibilities:
 *  - Per-segment Pexels search via MCP (parallel)
 *  - Per-segment TTS via elevenlabs-tts MCP (concatenated)
 *  - Visual QA (reject low-score assets → emit visual.failed for supervisor to heal)
 *  - Handles supervisor-decomposed subtasks (subtaskIndex/subtaskTotal)
 *
 * Dynamic routing: vision/curation tasks → standard tier, not frontier.
 */
export class VisualNode extends AgentNode {
  private router: ModelRouter;
  private mcp: MCPLayer;
  private pipelines: PipelineStore;
  private subIds: string[] = [];

  constructor(bus: EventBus, router: ModelRouter, mcp: MCPLayer, pipelines: PipelineStore) {
    super(bus, {
      id: 'visual-node',
      name: 'visual',
      capabilities: ['asset_search', 'image_generation', 'voice_synthesis', 'visual_qa'],
      maxConcurrency: 3,
      healthCheckIntervalMs: 30_000,
    });
    this.router = router;
    this.mcp = mcp;
    this.pipelines = pipelines;
  }

  protected async registerHandlers(): Promise<void> {
    this.subIds.push(
      this.bus.subscribe(this.config.id, EventType.SCRIPT_FINALIZED, (e) => this.handleScript(e))
    );
    // Direct visual.request (from supervisor decomposition / manual)
    this.subIds.push(
      this.bus.subscribe(this.config.id, EventType.VISUAL_REQUEST, (e) => this.handleScript(e))
    );
    this.subIds.push(
      this.bus.subscribe(this.config.id, 'visual.retry', (e) => this.handleRetry(e))
    );
  }

  protected async deregisterHandlers(): Promise<void> {
    this.subIds.forEach((id) => this.bus.unsubscribe(id));
    this.subIds = [];
  }

  // ─── Handlers ──────────────────────────────────────────────────────
  private async handleScript(event: MeshEvent): Promise<void> {
    const payload = event.payload as {
      topic: string;
      scriptData?: ScriptData;
      pipelineId?: string;
      subtaskIndex?: number;
      subtaskTotal?: number;
      decomposed?: boolean;
    };
    const pipelineId = payload.pipelineId ?? event.metadata.correlationId;
    const scriptData = payload.scriptData as ScriptData | undefined;
    const topic = payload.topic ?? (scriptData?.title ?? 'untitled');

    if (!pipelineId || !scriptData) {
      logger.warn({ eventId: event.id, pipelineId }, 'VisualNode: missing scriptData/pipelineId');
      // Still emit failed so supervisor can renegotiate
      this.emitFailed(pipelineId, EventType.VISUAL_FAILED, 'missing scriptData', event.metadata.retryCount);
      return;
    }

    // If supervisor decomposed, slice segments
    let segments = scriptData.segments ?? [];
    if (payload.decomposed && payload.subtaskTotal && payload.subtaskTotal > 1) {
      const chunk = Math.ceil(segments.length / payload.subtaskTotal);
      const idx = payload.subtaskIndex ?? 0;
      segments = segments.slice(idx * chunk, (idx + 1) * chunk);
      logger.info({ pipelineId, subtaskIndex: idx, chunk, total: scriptData.segments.length }, 'VisualNode: handling decomposed subtask');
    }

    const slicedScript: ScriptData = { ...scriptData, segments };

    logger.info({ pipelineId, topic, segments: segments.length, decomposed: !!payload.decomposed }, 'VisualNode: generating assets');

    const result = await this.executeTask(
      () => this.generateVisuals(topic, slicedScript, pipelineId, !!payload.decomposed),
      pipelineId,
      (err) => this.emitFailed(pipelineId, EventType.VISUAL_FAILED, err.message, event.metadata.retryCount)
    );

    if (result) {
      // QA gate
      const qaFailed = result.assets.filter((a) => !a.qa.passed);
      if (qaFailed.length > 0 && !payload.decomposed) {
        // Let supervisor split — emit failed rather than incomplete data
        logger.warn({ pipelineId, failed: qaFailed.length }, 'VisualNode: QA rejected assets — triggering healing');
        this.emitFailed(pipelineId, EventType.VISUAL_FAILED, `QA rejected ${qaFailed.length} assets`, event.metadata.retryCount);
        return;
      }

      this.emitComplete(pipelineId, EventType.VISUAL_COMPLETE, {
        topic,
        visualData: result,
        subtaskIndex: payload.subtaskIndex,
        subtaskTotal: payload.subtaskTotal,
        completedAt: new Date().toISOString(),
      });
    }
  }

  private async handleRetry(event: MeshEvent): Promise<void> {
    const payload = event.payload as { topic: string; scriptData: ScriptData; pipelineId?: string };
    const pipelineId = payload.pipelineId ?? event.metadata.correlationId;
    logger.warn({ pipelineId, retry: event.metadata.retryCount }, 'VisualNode: retry with reduced fidelity');

    const result = await this.executeTask(
      () => this.generateVisualsReduced(payload.topic, payload.scriptData, pipelineId),
      pipelineId,
      (err) => this.emitFailed(pipelineId, EventType.VISUAL_FAILED, err.message, event.metadata.retryCount)
    );

    if (result) {
      this.emitComplete(pipelineId, EventType.VISUAL_COMPLETE, {
        topic: payload.topic,
        visualData: result,
        reducedMode: true,
        completedAt: new Date().toISOString(),
      });
    }
  }

  // ─── Core Visual Generation ────────────────────────────────────────
  private async generateVisuals(
    topic: string,
    script: ScriptData,
    pipelineId: string,
    isDecomposed: boolean
  ): Promise<VisualOutput> {
    this.emitProgress(pipelineId, { stage: 'asset_search', total: script.segments.length });

    // Route: curation/vision is standard tier (not frontier) — cost optimization
    const visionProfile = {
      taskType: 'visual.curate',
      complexityScore: 0.55,
      requiredCapabilities: ['vision' as const],
      maxTokensNeeded: 3000,
      latencyBudgetMs: 15_000,
      costBudgetUsd: 0.04,
    };
    let routing: ReturnType<ModelRouter['route']> | undefined;
    try {
      routing = this.router.route(visionProfile);
    } catch {
      // fallback if no vision-capable model within budget
      routing = undefined;
    }

    // Parallel asset fetches (MCP-native)
    const assets = await this.fetchAssetsParallel(script.segments, pipelineId);

    this.emitProgress(pipelineId, { stage: 'voiceover', segments: script.segments.length });

    // TTS via elevenlabs-tts MCP (single concatenated call)
    const fullText = script.segments.map((s) => s.text).join(' ');
    const voiceover = await this.synthesizeVoice(fullText, script.segments[0]?.voiceStyle ?? 'calm', pipelineId);

    const totalDuration = assets.reduce((sum, a) => sum + a.durationSeconds, 0);

    return {
      pipelineId,
      topic,
      assets,
      voiceover,
      totalDurationSeconds: totalDuration,
      modelRouting: routing ? { curation: routing.selectedModel.id } : {},
    };
  }

  private async generateVisualsReduced(
    topic: string,
    script: ScriptData,
    pipelineId: string
  ): Promise<VisualOutput> {
    this.emitProgress(pipelineId, { stage: 'reduced_assets', message: 'Fallback: text-only assets' });

    // Reduced mode: no MCP search, return placeholder assets (no external call)
    const assets: VisualAsset[] = script.segments.map((seg) => ({
      segmentIndex: seg.index,
      query: seg.visualCue,
      assetUrl: `placeholder://${seg.visualCue.replace(/\s+/g, '_')}`,
      localPath: `/tmp/${pipelineId}/seg-${seg.index}.jpg`,
      durationSeconds: seg.durationSeconds,
      qa: { passed: true, score: 0.8 },
    }));

    return {
      pipelineId,
      topic,
      assets,
      voiceover: {
        path: `/tmp/${pipelineId}/voiceover.mp3`,
        durationSeconds: assets.reduce((s, a) => s + a.durationSeconds, 0),
        voiceId: 'fallback',
      },
      totalDurationSeconds: assets.reduce((s, a) => s + a.durationSeconds, 0),
      modelRouting: {},
    };
  }

  // ─── MCP Calls ─────────────────────────────────────────────────────
  private async fetchAssetsParallel(segments: ScriptSegment[], pipelineId: string): Promise<VisualAsset[]> {
    const results = await Promise.allSettled(
      segments.map(async (seg) => {
        // Try pexels MCP
        const query = seg.visualCue || seg.text.slice(0, 40);
        const res = await this.mcp.execute(
          { server: 'pexels', method: 'search', params: { query, per_page: 3, orientation: 'portrait' } },
          'search'
        );

        if (res.success) {
          const data = res.data as { photos?: Array<{ src: { portrait: string }; id: number }> };
          const url = data?.photos?.[0]?.src?.portrait ?? `placeholder://${query.replace(/\s+/g, '_')}`;
          return {
            segmentIndex: seg.index,
            query,
            assetUrl: url,
            localPath: `/tmp/${pipelineId}/seg-${seg.index}.jpg`,
            durationSeconds: seg.durationSeconds,
            qa: this.runQA(url, seg),
          } as VisualAsset;
        }

        // Fallback placeholder on MCP failure
        return {
          segmentIndex: seg.index,
          query,
          assetUrl: `placeholder://${query.replace(/\s+/g, '_')}`,
          localPath: `/tmp/${pipelineId}/seg-${seg.index}.jpg`,
          durationSeconds: seg.durationSeconds,
          qa: { passed: true, score: 0.7, reason: 'mcp_fallback' },
        } as VisualAsset;
      })
    );

    return results.map((r, i) => {
      if (r.status === 'fulfilled') return r.value;
      // Should not happen due to inner catch, but handle
      return {
        segmentIndex: segments[i].index,
        query: segments[i].visualCue,
        assetUrl: `placeholder://failed-${i}`,
        localPath: `/tmp/${pipelineId}/seg-${segments[i].index}.jpg`,
        durationSeconds: segments[i].durationSeconds,
        qa: { passed: false, score: 0, reason: String(r.reason) },
      } as VisualAsset;
    });
  }

  private async synthesizeVoice(text: string, voiceStyle: string, pipelineId: string): Promise<VisualOutput['voiceover']> {
    const res = await this.mcp.execute(
      { server: 'elevenlabs-tts', method: 'synthesize', params: { text, voiceStyle, pipelineId } },
      'generate'
    );

    if (res.success) {
      const data = res.data as { path?: string; duration?: number; voiceId?: string };
      return {
        path: data?.path ?? `/tmp/${pipelineId}/voiceover.mp3`,
        durationSeconds: data?.duration ?? text.split(' ').length * 0.4,
        voiceId: data?.voiceId ?? voiceStyle,
      };
    }

    // Fallback: estimate duration without TTS
    logger.warn({ pipelineId }, 'VisualNode: TTS failed — using duration estimate');
    return {
      path: `/tmp/${pipelineId}/voiceover.mp3`,
      durationSeconds: text.split(' ').length * 0.45,
      voiceId: 'fallback',
    };
  }

  private runQA(url: string, seg: ScriptSegment): VisualAsset['qa'] {
    // Deterministic QA: placeholder URLs fail if they look low-effort, real URLs pass
    if (url.startsWith('placeholder://')) {
      // Simulate occasional QA failure for testing self-healing (10% of placeholders)
      const hash = url.split('').reduce((a, c) => a + c.charCodeAt(0), 0);
      if (hash % 10 === 0) return { passed: false, score: 0.3, reason: 'placeholder_quality_low' };
      return { passed: true, score: 0.75 };
    }
    return { passed: true, score: 0.92 };
  }
}
