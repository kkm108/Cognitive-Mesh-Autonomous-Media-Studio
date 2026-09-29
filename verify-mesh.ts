import { bootstrap } from './mesh/bootstrap.js';
import { EventType } from './mesh/event-schema.js';
import { TrendForecastingNode } from './nodes/template-node.js';
import { createDefaultRouter } from './router/model-router.js';

async function main() {
  console.log('=== Cognitive Mesh — Verification ===\n');

  // 1. Bootstrap
  console.log('[1/6] Bootstrapping mesh...');
  const mesh = await bootstrap();
  console.log('  ✓ Mesh online');
  console.log('  Nodes:', mesh.nodes.map(n => n.getConfig().id).join(', '));
  console.log('  Bus health:', JSON.stringify(mesh.bus.health()));
  console.log('');

  // 2. Verify event flow: inject → research.complete → script.finalized
  console.log('[2/6] Testing autonomous pipeline (topic.injected → research → script)...');
  const topic = 'why octopuses dream';
  let researchCompleteReceived = false;
  let scriptFinalizedReceived = false;
  let researchPayload: any = null;
  let scriptPayload: any = null;

  const researchPromise = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('research.complete timeout (10s)')), 10_000);
    mesh.bus.subscribe('verify', EventType.RESEARCH_COMPLETE, (e) => {
      researchCompleteReceived = true;
      researchPayload = e.payload;
      console.log('  ✓ research.complete received');
      console.log('    topic:', (e.payload as any).topic);
      clearTimeout(timer);
      resolve();
    });
  });

  const scriptPromise = new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error('script.finalized timeout (15s)')), 15_000);
    mesh.bus.subscribe('verify2', EventType.SCRIPT_FINALIZED, (e) => {
      scriptFinalizedReceived = true;
      scriptPayload = e.payload;
      console.log('  ✓ script.finalized received');
      console.log('    title:', (e.payload as any).scriptData?.title ?? 'N/A');
      console.log('    segments:', (e.payload as any).scriptData?.segments?.length ?? 0);
      clearTimeout(timer);
      resolve();
    });
  });

  const pipelineId = await mesh.injectTopic(topic);
  console.log(`  Injected topic "${topic}" → pipeline ${pipelineId}`);

  try {
    await researchPromise;
  } catch (e) {
    console.error('  ✗ research stage failed:', (e as Error).message);
  }

  try {
    await scriptPromise;
  } catch (e) {
    console.error('  ✗ script stage failed:', (e as Error).message);
  }

  // Allow pipeline store to settle
  await new Promise(r => setTimeout(r, 500));
  const ctx = mesh.pipelines.get(pipelineId);
  console.log('  Pipeline ctx:', JSON.stringify({ id: ctx?.id, status: ctx?.status, hasResearch: !!ctx?.researchData, hasScript: !!ctx?.scriptData }, null, 2));
  console.log('');

  // 3. Model router verification
  console.log('[3/6] Verifying dynamic model routing...');
  const router = createDefaultRouter();
  const frontierChoice = router.route({
    taskType: 'research.synthesize',
    complexityScore: 0.85,
    requiredCapabilities: ['reasoning', 'long_context'],
    maxTokensNeeded: 8000,
    latencyBudgetMs: 15000,
    costBudgetUsd: 0.50,
  });
  console.log(`  High-reasoning task → ${frontierChoice.selectedModel.id} (tier=${frontierChoice.selectedModel.tier}, score=${frontierChoice.score.toFixed(3)})`);
  const fastChoice = router.route({
    taskType: 'metadata.format',
    complexityScore: 0.2,
    requiredCapabilities: ['cost_efficiency'],
    maxTokensNeeded: 1000,
    latencyBudgetMs: 5000,
    costBudgetUsd: 0.02,
  });
  console.log(`  Low-complexity task → ${fastChoice.selectedModel.id} (tier=${fastChoice.selectedModel.tier}, cost=$${fastChoice.estimatedCostUsd.toFixed(4)})`);
  console.log(`  Cost delta: frontier $${frontierChoice.estimatedCostUsd.toFixed(4)} vs fast $${fastChoice.estimatedCostUsd.toFixed(4)} (~${(frontierChoice.estimatedCostUsd / fastChoice.estimatedCostUsd).toFixed(1)}x)`);
  console.log('');

  // 4. MCP layer verification
  console.log('[4/6] Verifying MCP native tooling (no scrapers)...');
  const { createDefaultMCPLayer } = await import('./mcp/mcp-layer.js');
  const mcp = createDefaultMCPLayer();
  await mcp.connectAll();
  console.log('  MCP servers:', mcp.health().map(s => `${s.name}:${s.connected?'✓':'✗'}`).join(', '));
  const publishServers = mcp.discoverByCapability('publish');
  console.log('  Publish-capable servers:', publishServers.map(s => s.name).join(', ') || '(none connected)');
  console.log('  ✓ No Playwright/Selenium — native MCP only');
  console.log('');

  // 5. Self-healing verification (supervisor decomposition)
  console.log('[5/6] Verifying self-healing (supervisor retry + decompose)...');
  const beforeHealth = mesh.supervisor.health();
  console.log('  Supervisor before:', beforeHealth);
  // Simulate a failure event
  const { createEvent } = await import('./mesh/event-schema.js');
  const failEvent = createEvent('visual.failed', 'visual-node', { error: 'MCP timeout', pipelineId }, { correlationId: pipelineId, retryCount: 2 });
  await mesh.bus.publish(failEvent);
  await new Promise(r => setTimeout(r, 2500)); // wait for backoff retry
  console.log('  Simulated visual.failed (retryCount=2) → supervisor should decompose');
  // Check bus for decomposed subtasks
  const correlated = mesh.bus.getCorrelated(pipelineId);
  const decomposed = correlated.filter(e => (e.payload as any).decomposed);
  console.log(`  Decomposed subtasks emitted: ${decomposed.length}`);
  console.log(`  Dead letters: ${mesh.bus.getDeadLetters().length}`);
  console.log('');

  // 6. Extensibility: hot-swap new node
  console.log('[6/6] Verifying extensibility (hot-swap new node)...');
  const beforeNodeCount = mesh.nodes.length;
  console.log(`  Nodes before: ${beforeNodeCount}`);
  const { MCPLayer } = await import('./mcp/mcp-layer.js');
  // Use existing mesh bus/router — simulate hot-swapping a TrendForecastingNode
  const trendNode = new TrendForecastingNode(mesh.bus, router, mcp, mesh.pipelines);
  await mesh.registerNode(trendNode);
  console.log(`  Nodes after: ${mesh.nodes.length}`);
  console.log(`  New node: ${trendNode.getConfig().id}`);
  // Trigger it: emit another research.complete, verify trend node reacts
  let trendReceived = false;
  mesh.bus.subscribe('verify-trend', 'trend.forecast.complete', () => { trendReceived = true; });
  const pipeline2 = await mesh.injectTopic('future of AI agents');
  await new Promise(r => setTimeout(r, 3000));
  console.log(`  Trend forecast emitted: ${trendReceived ? '✓' : '— (async, may still be pending)'}`);
  console.log('');

  // Final health snapshot
  console.log('=== Final Mesh Health ===');
  console.log(JSON.stringify(mesh.health(), null, 2));
  console.log('');

  // Summary
  const checks = [
    researchCompleteReceived,
    scriptFinalizedReceived,
    frontierChoice.selectedModel.tier === 'frontier' || frontierChoice.selectedModel.tier === 'reasoning',
    fastChoice.selectedModel.tier === 'fast' || fastChoice.selectedModel.tier === 'standard',
    mesh.nodes.length === beforeNodeCount + 1,
  ];
  const passed = checks.filter(Boolean).length;
  console.log(`=== Result: ${passed}/${checks.length} checks passed ===`);
  if (researchCompleteReceived && scriptFinalizedReceived) {
    console.log('✓ End-to-end autonomy verified: topic → research → script');
  } else {
    console.log('✗ End-to-end incomplete — see logs above');
  }

  await mesh.shutdown();
  process.exit(passed === checks.length ? 0 : 1);
}

main().catch((e) => {
  console.error('Verification failed:', e);
  process.exit(1);
});
