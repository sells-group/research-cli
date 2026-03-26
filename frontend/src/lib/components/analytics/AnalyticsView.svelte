<script lang="ts">
  import { onMount } from 'svelte';
  import { api } from '$lib/api';
  import * as Card from '$lib/components/ui/card';
  import LoadingSpinner from '$lib/components/shared/LoadingSpinner.svelte';
  import ErrorAlert from '$lib/components/shared/ErrorAlert.svelte';
  import SyncTrendChart from './SyncTrendChart.svelte';
  import IdentifierCoverageChart from './IdentifierCoverageChart.svelte';
  import XrefHeatmap from './XrefHeatmap.svelte';
  import CostTrendChart from './CostTrendChart.svelte';
  import type { SyncTrend, IdentifierCoverage, XrefCoverage, EnrichmentStats, CostBreakdown } from '$lib/types';

  let syncTrends = $state<SyncTrend[]>([]);
  let idCoverage = $state<IdentifierCoverage[]>([]);
  let xrefCoverage = $state<XrefCoverage[]>([]);
  let enrichmentStats = $state<EnrichmentStats | null>(null);
  let costBreakdown = $state<CostBreakdown[]>([]);
  let loading = $state(true);
  let error = $state('');

  async function loadData() {
    loading = true;
    error = '';
    try {
      const [st, ic, xc, es, cb] = await Promise.all([
        api.syncTrends().catch(() => ({ trends: [] })),
        api.identifierCoverage().catch(() => ({ coverage: [] })),
        api.xrefCoverage().catch(() => ({ coverage: [] })),
        api.enrichmentStats().catch(() => null),
        api.costBreakdown().catch(() => ({ breakdown: [] })),
      ]);
      syncTrends = st.trends ?? [];
      idCoverage = ic.coverage ?? [];
      xrefCoverage = xc.coverage ?? [];
      enrichmentStats = es;
      costBreakdown = cb.breakdown ?? [];
    } catch (e: any) {
      error = e.message;
    } finally {
      loading = false;
    }
  }

  onMount(loadData);
</script>

<div class="flex-1 overflow-auto p-6 space-y-6">
  {#if loading}
    <LoadingSpinner label="Loading analytics..." />
  {:else if error}
    <ErrorAlert message={error} onRetry={loadData} />
  {:else}
    <!-- Enrichment Summary -->
    {#if enrichmentStats}
      <div class="grid grid-cols-4 gap-4">
        <Card.Root class="p-4">
          <p class="text-xs text-muted-foreground">Total Runs</p>
          <p class="text-2xl font-bold">{enrichmentStats.total_runs.toLocaleString()}</p>
        </Card.Root>
        <Card.Root class="p-4">
          <p class="text-xs text-muted-foreground">Avg Score</p>
          <p class="text-2xl font-bold">{(enrichmentStats.avg_score * 100).toFixed(0)}%</p>
        </Card.Root>
        <Card.Root class="p-4">
          <p class="text-xs text-muted-foreground">Identifier Types</p>
          <p class="text-2xl font-bold">{idCoverage.length}</p>
        </Card.Root>
        <Card.Root class="p-4">
          <p class="text-xs text-muted-foreground">Cross-Ref Pairs</p>
          <p class="text-2xl font-bold">{xrefCoverage.length}</p>
        </Card.Root>
      </div>
    {/if}

    <div class="grid grid-cols-2 gap-6">
      <!-- Sync Trends -->
      <Card.Root class="p-4">
        <h3 class="text-sm font-semibold mb-3">Sync Trends (rows/day)</h3>
        <SyncTrendChart data={syncTrends} />
      </Card.Root>

      <!-- Cost Breakdown -->
      <Card.Root class="p-4">
        <h3 class="text-sm font-semibold mb-3">API Cost by Tier</h3>
        <CostTrendChart data={costBreakdown} />
      </Card.Root>

      <!-- Identifier Coverage -->
      <Card.Root class="p-4">
        <h3 class="text-sm font-semibold mb-3">Identifier Coverage</h3>
        <IdentifierCoverageChart data={idCoverage} />
      </Card.Root>

      <!-- XRef Heatmap -->
      <Card.Root class="p-4">
        <h3 class="text-sm font-semibold mb-3">Cross-Reference Coverage</h3>
        <XrefHeatmap data={xrefCoverage} />
      </Card.Root>
    </div>
  {/if}
</div>
