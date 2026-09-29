import { EventBus } from './event-bus.js';
import { PersistentEventBus } from './event-bus-redis.js';
import { PipelineStore } from './pipeline-store.js';
import { MeshSupervisor } from './supervisor.js';
import { createPersistence } from './persistence.js';
import { createDefaultRouter } from '../router/model-router.js';
import { createDefaultMCPLayer } from '../mcp/mcp-layer.js';
import { ResearchNode } from '../nodes/research-node.js';
import { ScriptNode } from '../nodes/script-node.js';
import { VisualNode } from '../nodes/visual-node.js';
import { AssemblyNode } from '../nodes/assembly-node.js';
import { PublishNode } from '../nodes/publish-node.js';
import { EventType, createEvent } from './event-schema.js';
import { logger } from './logger.js';
import type { AgentNode } from './agent-node.js';

// ─── Mesh Container ────────────────────────────────────────────────
export interface Mesh {
  bus: EventBus;
  pipelines: PipelineStore;
  supervisor: MeshSupervisor;
  nodes: AgentNode[];
  injectTopic: (topic: string) => Promise<string>; // returns pipelineId
  registerNode: (node: AgentNode) => Promise<void>;
  health: () => Record<string, unknown>;
  shutdown: () => Promise<void>;
}

/**
 * Bootstrap the Cognitive Mesh.
 *
 * Zero-orchestrator: nodes discover work via PubSub.
 * Supervisor only heals — it never routes tasks.
 */
export async function bootstrap(): Promise<Mesh> {
  logger.info('Cognitive Mesh — genesis bootstrap');

  // Persistence (Redis if MESH_REDIS_URL set, else in-memory)
  const persistence = await createPersistence();
  const pHealth = await persistence.health();
  logger.info({ backend: pHealth.backend, connected: pHealth.connected }, 'Persistence ready');

  // Core infrastructure — PersistentEventBus wraps EventBus with durable log
  const bus: EventBus = new PersistentEventBus(persistence as any, { maxLogSize: 20_000 });
  const pipelines = new PipelineStore();
  const router = createDefaultRouter();
  const mcp = createDefaultMCPLayer();

  // Connect MCP servers (placeholder transport — wiring in)
  await mcp.connectAll().catch((err) => {
    logger.warn({ err: String(err) }, 'MCP connectAll failed — continuing in degraded mode');
  });

  // Supervisor (self-healing control plane)
  const supervisor = new MeshSupervisor(bus, pipelines, {
    healthTimeoutMs: 90_000,
    checkIntervalMs: 15_000,
    maxRetries: 3,
    taskSplitThreshold: 2,
  });

  // Full mesh — 5 core nodes (Research → Script → Visual → Assembly → Publish)
  const researchNode = new ResearchNode(bus, router, mcp, pipelines);
  const scriptNode = new ScriptNode(bus, router, mcp, pipelines);
  const visualNode = new VisualNode(bus, router, mcp, pipelines);
  const assemblyNode = new AssemblyNode(bus, mcp, pipelines);
  const publishNode = new PublishNode(bus, mcp, pipelines, router);

  const nodes: AgentNode[] = [researchNode, scriptNode, visualNode, assemblyNode, publishNode];

  // Wire pipeline store to observe all events (state transitions)
  bus.subscribe('pipeline-store', '*', (event) => {
    pipelines.transition(event);
  });

  // Start supervisor first, then nodes
  await supervisor.start();

  for (const node of nodes) {
    supervisor.registerNode(node);
    await node.start();
  }

  // Replay protection: if a pipeline was injected before nodes were ready,
  // script node would miss research.complete. The pipeline-store subscription above catches all,
  // and nodes use bus.subscribe with wildcard replay if needed. Supervisor handles re-emit on failure.

  logger.info(
    {
      nodes: nodes.map((n) => n.getConfig().id),
      mcpServers: mcp.health(),
      models: router.listModels().map((m) => m.id),
    },
    'Cognitive Mesh — online'
  );

  // ─── Public API ────────────────────────────────────────────────

  /**
   * Inject a seed topic into the mesh.
   * Creates a pipeline context and emits topic.injected — from there, autonomy takes over.
   */
  async function injectTopic(topic: string): Promise<string> {
    const pipeline = pipelines.create(topic);

    const event = createEvent(EventType.TOPIC_INJECTED, 'bootstrap', {
      topic,
      pipelineId: pipeline.id,
    }, {
      correlationId: pipeline.id,
      topics: ['trigger', 'topic'],
      priority: 9,
    });

    await bus.publish(event);
    logger.info({ pipelineId: pipeline.id, topic }, 'Topic injected — mesh is now autonomous');

    return pipeline.id;
  }

  /**
   * Hot-swap: inject a new node without altering core infrastructure.
   *
   * Example — adding a TrendForecastingNode at runtime:
   *   const trendNode = new TrendForecastingNode(bus, router, mcp, pipelines);
   *   await mesh.registerNode(trendNode);
   *
   * The node self-registers its PubSub subscriptions and is immediately live.
   */
  async function registerNode(node: AgentNode): Promise<void> {
    supervisor.registerNode(node);
    await node.start();
    nodes.push(node);
    logger.info({ nodeId: node.getConfig().id }, 'Node hot-swapped into mesh');
  }

  function health(): Record<string, unknown> {
    return {
      bus: bus.health(),
      pipelines: pipelines.health(),
      supervisor: supervisor.health(),
      nodes: nodes.map((n) => ({ id: n.getConfig().id, health: n.getHealth() })),
      mcp: mcp.health(),
    };
  }

  async function shutdown(): Promise<void> {
    logger.info('Cognitive Mesh — shutdown initiated');
    for (const node of nodes) {
      await node.stop().catch((e) => logger.error({ err: String(e) }, 'Node stop failed'));
    }
    await supervisor.stop();
    await mcp.disconnectAll();
    await persistence.disconnect().catch(() => {});
    logger.info('Cognitive Mesh — shutdown complete');
  }

  // Graceful shutdown on signals
  const handleSignal = async (signal: string) => {
    logger.info({ signal }, 'Received shutdown signal');
    await shutdown();
    process.exit(0);
  };
  process.on('SIGINT', () => handleSignal('SIGINT'));
  process.on('SIGTERM', () => handleSignal('SIGTERM'));

  return { bus, pipelines, supervisor, nodes, injectTopic, registerNode, health, shutdown };
}

// ─── CLI entrypoint ────────────────────────────────────────────────
if (import.meta.url === `file://${process.argv[1]?.replace(/\\/g, '/')}`) {
  const mesh = await bootstrap();

  // Demo: inject a seed topic if provided via CLI
  const seedTopic = process.argv[2];
  if (seedTopic) {
    const pipelineId = await mesh.injectTopic(seedTopic);
    logger.info({ pipelineId }, 'Demo pipeline created — watch the mesh');

    // Print health every 10s
    setInterval(() => {
      console.log('\n— mesh health —');
      console.log(JSON.stringify(mesh.health(), null, 2));
    }, 10_000);
  } else {
    console.log(`
Cognitive Mesh online.
Inject a topic:
  node --loader ts-node/esm mesh/bootstrap.ts "why cats dream"

Or programmatically:
  import { bootstrap } from './mesh/bootstrap.js';
  const mesh = await bootstrap();
  const id = await mesh.injectTopic("trending topic here");
  console.log("Pipeline:", id);

Hot-swap a new node:
  import { TrendForecastingNode } from './nodes/trend-forecasting-node.js';
  await mesh.registerNode(new TrendForecastingNode(mesh.bus, router, mcp, mesh.pipelines));
`);
  }
}
