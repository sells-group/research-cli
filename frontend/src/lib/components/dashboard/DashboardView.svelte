<script lang="ts">
  import { onMount } from 'svelte';
  import { api } from '$lib/api';
  import MetricCard from '$lib/components/shared/MetricCard.svelte';
  import FedsyncStatusGrid from './FedsyncStatusGrid.svelte';
  import RecentRunsList from './RecentRunsList.svelte';
  import LoadingSpinner from '$lib/components/shared/LoadingSpinner.svelte';
  import ErrorAlert from '$lib/components/shared/ErrorAlert.svelte';
  import type { MetricsSnapshot, Run, DatasetStatus } from '$lib/types';
  import Activity from 'lucide-svelte/icons/activity';
  import CheckCircle from 'lucide-svelte/icons/check-circle-2';
  import XCircle from 'lucide-svelte/icons/x-circle';
  import Clock from 'lucide-svelte/icons/clock';

  let metrics = $state<MetricsSnapshot | null>(null);
  let recentRuns = $state<Run[]>([]);
  let datasetStatuses = $state<DatasetStatus[]>([]);
  let loading = $state(true);
  let error = $state('');

  async function loadData() {
    loading = true;
    error = '';
    try {
      const [m, r, d] = await Promise.all([
        api.metrics().catch(() => null),
        api.listRuns({ limit: '10', sort: 'created_at', order: 'desc' }).catch(() => ({ runs: [], total: 0 })),
        api.fedsyncStatuses().catch(() => ({ datasets: [] })),
      ]);
      metrics = m;
      recentRuns = r.runs ?? [];
      datasetStatuses = d.datasets ?? [];
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
    <LoadingSpinner label="Loading dashboard..." />
  {:else if error}
    <ErrorAlert message={error} onRetry={loadData} />
  {:else}
    <!-- Metric Cards -->
    <div class="grid grid-cols-4 gap-4">
      <MetricCard label="Queued" value={metrics?.pipeline_queued ?? 0} icon={Clock} />
      <MetricCard label="Total Runs" value={metrics?.pipeline_total ?? 0} icon={Activity} />
      <MetricCard label="Complete" value={metrics?.pipeline_complete ?? 0} icon={CheckCircle} />
      <MetricCard label="Failed" value={metrics?.pipeline_failed ?? 0} icon={XCircle} />
    </div>

    <div class="grid grid-cols-2 gap-6">
      <!-- Fedsync Status Grid -->
      <div>
        <h2 class="text-sm font-semibold mb-3">Federal Dataset Status</h2>
        <FedsyncStatusGrid datasets={datasetStatuses} />
      </div>

      <!-- Recent Runs -->
      <div>
        <h2 class="text-sm font-semibold mb-3">Recent Enrichment Runs</h2>
        <RecentRunsList runs={recentRuns} />
      </div>
    </div>

    <!-- Cost Summary -->
    {#if metrics}
      <div class="grid grid-cols-3 gap-4">
        <MetricCard label="Avg Score" value={(metrics.pipeline_avg_score * 100).toFixed(0) + '%'} />
        <MetricCard label="Pipeline Cost" value={'$' + metrics.pipeline_cost_usd.toFixed(2)} />
        <MetricCard label="DLQ Depth" value={metrics.dlq_depth} />
      </div>
    {/if}
  {/if}
</div>
