<script lang="ts">
  import type { CostBreakdown } from '$lib/types';
  import EmptyState from '$lib/components/shared/EmptyState.svelte';

  interface Props {
    data: CostBreakdown[];
  }

  let { data }: Props = $props();

  const grouped = $derived(() => {
    const byDate = new Map<string, number>();
    for (const d of data) {
      byDate.set(d.date, (byDate.get(d.date) ?? 0) + d.cost);
    }
    return Array.from(byDate.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .slice(-30);
  });

  const maxVal = $derived(Math.max(...(grouped().map(([, v]) => v)), 0.01));
  const totalCost = $derived(data.reduce((s, d) => s + d.cost, 0));
</script>

{#if data.length === 0}
  <EmptyState title="No cost data" description="Cost trends will appear after enrichment runs" />
{:else}
  <p class="text-xs text-muted-foreground mb-2">Total: ${totalCost.toFixed(2)}</p>
  <div class="h-48 flex items-end gap-0.5">
    {#each grouped() as [date, cost]}
      <div class="flex-1 flex flex-col items-center">
        <div
          class="w-full bg-chart-5 rounded-t opacity-80 hover:opacity-100 transition-opacity min-h-[2px]"
          style="height: {(cost / maxVal) * 100}%"
          title="{date}: ${cost.toFixed(3)}"
        ></div>
      </div>
    {/each}
  </div>
  <div class="flex justify-between mt-1">
    <span class="text-[10px] text-muted-foreground">{grouped()[0]?.[0] ?? ''}</span>
    <span class="text-[10px] text-muted-foreground">{grouped()[grouped().length - 1]?.[0] ?? ''}</span>
  </div>
{/if}
