<script lang="ts">
  import StatusBadge from '$lib/components/shared/StatusBadge.svelte';
  import { activeView } from '$lib/stores/view';
  import { selectedRun } from '$lib/stores/enrichment';
  import type { Run } from '$lib/types';

  interface Props {
    runs: Run[];
  }

  let { runs }: Props = $props();

  function viewRun(run: Run) {
    selectedRun.set(run);
    activeView.set('enrichment');
  }

  function formatTime(ts: string): string {
    const d = new Date(ts);
    return d.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  }
</script>

<div class="space-y-1">
  {#each runs as run}
    <button
      class="w-full text-left px-3 py-2 rounded-md hover:bg-accent/50 transition-colors flex items-center gap-3"
      onclick={() => viewRun(run)}
    >
      <StatusBadge status={run.status} size="sm" />
      <span class="text-xs truncate flex-1">{run.company?.name ?? run.company?.url ?? 'Unknown'}</span>
      {#if run.result?.score != null}
        <span class="text-xs text-muted-foreground tabular-nums">{(run.result.score * 100).toFixed(0)}%</span>
      {/if}
      <span class="text-xs text-muted-foreground tabular-nums">{formatTime(run.created_at)}</span>
    </button>
  {:else}
    <p class="text-xs text-muted-foreground px-3 py-4">No recent runs</p>
  {/each}
</div>
