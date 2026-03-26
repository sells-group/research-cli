<script lang="ts">
  import type { XrefCoverage } from '$lib/types';
  import EmptyState from '$lib/components/shared/EmptyState.svelte';

  interface Props {
    data: XrefCoverage[];
  }

  let { data }: Props = $props();

  const maxCount = $derived(Math.max(...data.map(d => d.count), 1));

  function opacity(count: number): number {
    return 0.1 + 0.9 * (count / maxCount);
  }
</script>

{#if data.length === 0}
  <EmptyState title="No cross-reference data" description="Cross-reference coverage will appear after entity matching runs" />
{:else}
  <div class="space-y-1 max-h-64 overflow-auto">
    {#each data.sort((a, b) => b.count - a.count) as item}
      <div class="flex items-center gap-2 text-xs">
        <span class="w-24 truncate text-right">{item.source_a}</span>
        <div
          class="flex-1 h-6 rounded flex items-center justify-center"
          style="background: oklch(0.488 0.243 264.376 / {opacity(item.count)})"
        >
          <span class="text-[10px] font-medium">{item.count.toLocaleString()}</span>
        </div>
        <span class="w-24 truncate">{item.source_b}</span>
      </div>
    {/each}
  </div>
{/if}
