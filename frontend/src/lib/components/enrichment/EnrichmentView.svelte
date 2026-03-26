<script lang="ts">
  import { onMount } from 'svelte';
  import { api } from '$lib/api';
  import { runs, selectedRun, runFilter, runsTotal, runsLoading } from '$lib/stores/enrichment';
  import RunList from './RunList.svelte';
  import RunDetail from './RunDetail.svelte';
  import EmptyState from '$lib/components/shared/EmptyState.svelte';
  import ErrorAlert from '$lib/components/shared/ErrorAlert.svelte';

  let error = $state('');
  let page = $state(1);
  const perPage = 50;

  async function loadRuns() {
    runsLoading.set(true);
    error = '';
    try {
      const filter = $runFilter;
      const params: Record<string, string> = {
        limit: String(perPage),
        offset: String((page - 1) * perPage),
        sort: filter.sortBy,
        order: filter.sortOrder,
      };
      if (filter.status !== 'all') params.status = filter.status;
      if (filter.search) params.company_url = filter.search;

      const result = await api.listRuns(params);
      runs.set(result.runs ?? []);
      runsTotal.set(result.total);
    } catch (e: any) {
      error = e.message;
    } finally {
      runsLoading.set(false);
    }
  }

  onMount(loadRuns);

  $effect(() => {
    // Re-load when filter changes
    const _ = $runFilter;
    loadRuns();
  });
</script>

<div class="flex-1 flex overflow-hidden">
  <!-- Left: Run List -->
  <div class="w-[400px] border-r border-border flex flex-col">
    {#if error}
      <ErrorAlert message={error} onRetry={loadRuns} />
    {:else}
      <RunList
        runs={$runs}
        total={$runsTotal}
        loading={$runsLoading}
        selected={$selectedRun}
        {page}
        {perPage}
        onSelect={(run) => selectedRun.set(run)}
        onPageChange={(p) => { page = p; loadRuns(); }}
      />
    {/if}
  </div>

  <!-- Right: Run Detail -->
  <div class="flex-1 overflow-auto">
    {#if $selectedRun}
      <RunDetail run={$selectedRun} />
    {:else}
      <EmptyState title="Select a run" description="Click on an enrichment run to view details" />
    {/if}
  </div>
</div>
