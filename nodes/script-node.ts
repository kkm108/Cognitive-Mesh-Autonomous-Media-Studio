import { MeshEvent, EventType, createEvent } from '../mesh/event-schema.js';
import { EventBus } from '../mesh/event-bus.js';
import { AgentNode } from '../mesh/agent-node.js';
import { ModelRouter, TaskProfile } from '../router/model-router.js';
import { MCPLayer } from '../mcp/mcp-layer.js';
import { PipelineStore } from '../mesh/pipeline-store.js';
import { logger } from '../mesh/logger.js';

interface ScriptOutput {
  title: string;
  hook: string;
  segments: Array<{
    index: number;
    type: 'hook' | 'narration' | 'call_to_action';
    text: string;
    durationSeconds: number;
    visualCue: string;
    voiceStyle: string;
  }>;
  metadata: {
    targetDurationSeconds: number;
    aspectRatio: '9:16' | '16:9' | '1:1';
    platform: string[];
    tags: string[];
    description: string;
  };
}

/**
 * Script Agent Node.
 * Receives research data, generates a faceless video script with segments,
 * visual cues, and voice direction. Emits script.finalized.
 *
 * Dynamic routing: routes scriptwriting to creativity-tier models,
 * metadata formatting to fast-tier models.
 * Self-healing: if script generation fails, retries with simplified prompt.
 */
export class ScriptNode extends AgentNode {
  private router: ModelRouter;
  private mcp: MCPLayer;
  private pipelines: PipelineStore;
  private subIds: string[] = [];

  constructor(bus: EventBus, router: ModelRouter, mcp: MCPLayer, pipelines: PipelineStore) {
    super(bus, {
      id: 'script-node',
      name: 'script',
      capabilities: ['scriptwriting', 'dialogue', 'storyboarding', 'metadata_generation'],
      maxConcurrency: 3,
      healthCheckIntervalMs: 30_000,
    });
    this.router = router;
    this.mcp = mcp;
    this.pipelines = pipelines;
  }

  protected async registerHandlers(): Promise<void> {
    // React to research completion — this is the main trigger
    this.subIds.push(
      this.bus.subscribe(this.config.id, EventType.RESEARCH_COMPLETE, (e) => this.handleResearchComplete(e))
    );

    // Handle retry requests
    this.subIds.push(
      this.bus.subscribe(this.config.id, 'script.retry', (e) => this.handleRetry(e))
    );
  }

  protected async deregisterHandlers(): Promise<void> {
    this.subIds.forEach((id) => this.bus.unsubscribe(id));
    this.subIds = [];
  }

  // ─── Handlers ──────────────────────────────────────────────────────
  private async handleResearchComplete(event: MeshEvent): Promise<void> {
    const payload = event.payload as { topic: string; researchData?: Record<string, unknown>; pipelineId?: string };
    const pipelineId = payload.pipelineId ?? event.metadata.correlationId;
    const topic = payload.topic;
    const researchData = payload.researchData ?? payload as unknown as Record<string, unknown>;

    if (!pipelineId || !topic) {
      logger.warn({ eventId: event.id }, 'ScriptNode: missing pipelineId/topic');
      return;
    }

    logger.info({ pipelineId, topic }, 'ScriptNode: generating script from research');

    const result = await this.executeTask(
      () => this.generateScript(topic, researchData, pipelineId),
      pipelineId,
      (err) => this.emitFailed(pipelineId, EventType.SCRIPT_FAILED, err.message, event.metadata.retryCount)
    );

    if (result) {
      const metadata = await this.generateMetadata(topic, result, pipelineId);

      const finalScript: ScriptOutput = {
        ...result,
        metadata: { ...result.metadata, ...metadata },
      };

      // EventType is script.finalized per catalog
      this.emitComplete(pipelineId, EventType.SCRIPT_FINALIZED, {
        topic,
        scriptData: finalScript,
        completedAt: new Date().toISOString(),
      });
    }
  }

  private async handleRetry(event: MeshEvent): Promise<void> {
    const payload = event.payload as { topic: string; researchData?: Record<string, unknown>; pipelineId?: string };
    const pipelineId = payload.pipelineId ?? event.metadata.correlationId;
    const topic = payload.topic;
    const researchData = payload.researchData ?? {};

    logger.warn({ pipelineId, topic, retry: event.metadata.retryCount }, 'ScriptNode: retry with reduced scope');

    const result = await this.executeTask(
      () => this.generateScriptMinimal(topic, researchData, pipelineId),
      pipelineId,
      (err) => this.emitFailed(pipelineId, EventType.SCRIPT_FAILED, err.message, event.metadata.retryCount)
    );

    if (result) {
      this.emitComplete(pipelineId, EventType.SCRIPT_FINALIZED, {
        topic,
        scriptData: result,
        completedAt: new Date().toISOString(),
      });
    }
  }

  // ─── Core Script Generation ───────────────────────────────────────
  private async generateScript(
    topic: string,
    researchData: Record<string, unknown>,
    pipelineId: string
  ): Promise<ScriptOutput> {
    this.emitProgress(pipelineId, { stage: 'outline', message: 'Generating script outline' });

    // Route scriptwriting to a creativity-focused model
    const scriptProfile: TaskProfile = {
      taskType: 'script.dialogue',
      complexityScore: 0.85,
      requiredCapabilities: ['creativity', 'long_context'],
      maxTokensNeeded: 6000,
      latencyBudgetMs: 20_000,
      costBudgetUsd: 0.08,
    };

    const routing = this.router.route(scriptProfile);

    this.emitProgress(pipelineId, {
      stage: 'writing',
      message: `Generating script with ${routing.selectedModel.id}`,
      modelUsed: routing.selectedModel.id,
    });

    // Build the script prompt from research data
    const scriptPrompt = this.buildScriptPrompt(topic, researchData);

    // Generate script via routed model (placeholder for actual API call)
    const script = await this.callModelForScript(routing.selectedModel.id, scriptPrompt);

    this.emitProgress(pipelineId, { stage: 'segments', message: 'Structuring segments' });

    // Parse and structure into segments
    const structured = this.structureScript(script, topic);

    return structured;
  }

  /**
   * Self-healing: minimal script generation with reduced segment count.
   */
  private async generateScriptMinimal(
    topic: string,
    researchData: Record<string, unknown>,
    pipelineId: string
  ): Promise<ScriptOutput> {
    this.emitProgress(pipelineId, { stage: 'minimal_script', message: 'Generating minimal script' });

    const minimalProfile: TaskProfile = {
      taskType: 'script.dialogue.minimal',
      complexityScore: 0.4,
      requiredCapabilities: ['creativity'],
      maxTokensNeeded: 2000,
      latencyBudgetMs: 10_000,
      costBudgetUsd: 0.03,
    };

    const routing = this.router.route(minimalProfile);

    return {
      title: `${topic} - Quick Facts`,
      hook: `Did you know about ${topic}? Here's what you need to know.`,
      segments: [
        { index: 0, type: 'hook', text: `Did you know about ${topic}?`, durationSeconds: 5, visualCue: 'text_overlay_bold', voiceStyle: 'energetic' },
        { index: 1, type: 'narration', text: `Let's break down the key points.`, durationSeconds: 15, visualCue: 'b_roll_stock', voiceStyle: 'calm' },
        { index: 2, type: 'call_to_action', text: 'Follow for more insights.', durationSeconds: 5, visualCue: 'subscribe_animation', voiceStyle: 'enthusiastic' },
      ],
      metadata: {
        targetDurationSeconds: 25,
        aspectRatio: '9:16',
        platform: ['youtube_shorts', 'instagram_reels'],
        tags: [topic.toLowerCase().replace(/\s+/g, '_')],
        description: `Quick facts about ${topic}`,
      },
    };
  }

  // ─── Script Prompt Construction ───────────────────────────────────
  private buildScriptPrompt(topic: string, research: Record<string, unknown>): string {
    const insights = (research.keyInsights as string[]) ?? [];
    const angle = (research.angle as string) ?? topic;

    return `
You are a professional scriptwriter for faceless short-form video content.

TOPIC: ${topic}
ANGLE: ${angle}
KEY INSIGHTS:
${insights.map((i: string, idx: number) => `${idx + 1}. ${i}`).join('\n')}

REQUIREMENTS:
- Target duration: 30-60 seconds
- Aspect ratio: 9:16 (vertical)
- Platform: YouTube Shorts + Instagram Reels
- Style: Fast-paced, hook-driven, no presenter on camera
- Include visual cues for each segment
- Voice direction (tone/style) per segment

OUTPUT FORMAT:
Title: [compelling title]
Hook: [opening line that grabs attention in 2 seconds]

Segment 1 (Type: Hook, Duration: Xs):
Text: [spoken text]
Visual: [visual description]
Voice: [tone direction]

[... more segments ...]

Segment N (Type: Call to Action, Duration: Xs):
Text: [closing text]
Visual: [visual description]
Voice: [tone direction]

Generate the script now:
    `.trim();
  }

  // ─── Model Calls ──────────────────────────────────────────────────
  private async callModelForScript(modelId: string, prompt: string): Promise<string> {
    // In production, this calls the LLM API via the routed model.
    logger.info({ modelId, promptLength: prompt.length }, 'Calling model for script generation');

    // Placeholder for actual API call
    return `Title: The Truth About This Topic
Hook: You won't believe what's happening right now...

Segment 1 (Type: Hook, Duration: 5s):
Text: You won't believe what's happening right now
Visual: Dramatic zoom into relevant image
Voice: Energetic, urgent

Segment 2 (Type: Narration, Duration: 20s):
Text: Here's what you need to know. The key facts are staggering.
Visual: Animated text overlays with b-roll footage
Voice: Calm, informative

Segment 3 (Type: Call to Action, Duration: 5s):
Text: Follow for more daily insights
Visual: Subscribe button animation
Voice: Warm, inviting`;
  }

  /**
   * Generate platform metadata using a fast/cost-efficient model.
   */
  private async generateMetadata(
    topic: string,
    script: ScriptOutput,
    pipelineId: string
  ): Promise<Partial<ScriptOutput['metadata']>> {
    const metaProfile: TaskProfile = {
      taskType: 'metadata.format',
      complexityScore: 0.2,
      requiredCapabilities: ['cost_efficiency'],
      maxTokensNeeded: 1000,
      latencyBudgetMs: 5_000,
      costBudgetUsd: 0.01,
    };

    const routing = this.router.route(metaProfile);

    this.emitProgress(pipelineId, {
      stage: 'metadata',
      message: `Generating metadata with ${routing.selectedModel.id}`,
    });

    // Placeholder for actual model call
    return {
      tags: [
        topic.toLowerCase().replace(/\s+/g, '_'),
        'shorts',
        'viral',
        'trending',
        'facts',
      ],
      description: `${script.title}\n\n${script.hook}\n\n#${topic.replace(/\s+/g, '').toLowerCase()} #shorts #viral #trending`,
    };
  }

  // ─── Script Structuring ───────────────────────────────────────────
  private structureScript(rawScript: string, topic: string): ScriptOutput {
    // Parse raw script text into structured segments
    // In production, this uses an LLM to parse the raw output into structured JSON
    const segments = this.parseSegments(rawScript);

    return {
      title: this.extractTitle(rawScript) ?? `${topic} - Everything You Need to Know`,
      hook: this.extractHook(rawScript) ?? `Let's talk about ${topic}`,
      segments,
      metadata: {
        targetDurationSeconds: segments.reduce((sum, s) => sum + s.durationSeconds, 0),
        aspectRatio: '9:16',
        platform: ['youtube_shorts', 'instagram_reels'],
        tags: [],
        description: '',
      },
    };
  }

  private parseSegments(raw: string): ScriptOutput['segments'] {
    const segments: ScriptOutput['segments'] = [];
    const segmentRegex = /Segment\s+(\d+)\s*\(Type:\s*(\w+),\s*Duration:\s*(\d+)s?\):/gi;
    let match;
    let lastIndex = 0;

    while ((match = segmentRegex.exec(raw)) !== null) {
      const afterMatch = raw.slice(match.index + match[0].length);
      const nextSegment = afterMatch.search(/Segment\s+\d+\s*\(/i);
      const block = nextSegment === -1 ? afterMatch : afterMatch.slice(0, nextSegment);

      const textMatch = block.match(/Text:\s*(.+)/i);
      const visualMatch = block.match(/Visual:\s*(.+)/i);
      const voiceMatch = block.match(/Voice:\s*(.+)/i);

      segments.push({
        index: parseInt(match[1]) - 1,
        type: match[2].toLowerCase() as ScriptOutput['segments'][0]['type'],
        text: textMatch?.[1]?.trim() ?? '',
        durationSeconds: parseInt(match[3]),
        visualCue: visualMatch?.[1]?.trim() ?? 'default',
        voiceStyle: voiceMatch?.[1]?.trim() ?? 'neutral',
      });
    }

    return segments;
  }

  private extractTitle(raw: string): string | null {
    const match = raw.match(/Title:\s*(.+)/i);
    return match?.[1]?.trim() ?? null;
  }

  private extractHook(raw: string): string | null {
    const match = raw.match(/Hook:\s*(.+)/i);
    return match?.[1]?.trim() ?? null;
  }
}
