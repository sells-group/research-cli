<script lang="ts">
  import type { SyncTrend } from '$lib/types';
  import EmptyState from '$lib/components/shared/EmptyState.svelte';

  interface Props {
    data: SyncTrend[];
  }

  let { data }: Props = $props();

  const grouped = $derived(() => {
    const byDate = new Map<string, number>();
    for (const d of data) {
      byDate.set(d.date, (byDate.get(d.date) ?? 0) + d.rows_synced);
    }
    return Array.from(byDate.entries())
      .sort(([a], [b]) => a.localeCompare(b))
      .slice(-30);
  });

  const maxVal = $derived(Math.max(...(grouped().map(([, v]) => v)), 1));
</script>

{#if data.length === 0}
  <EmptyState title="No sync data" description="Sync trends will appear after datasets are synced" />
{:else}
  <div class="h-48 flex items-end gap-0.5">
    {#each grouped() as [date, count]}
      <div class="flex-1 flex flex-col items-center gap-1">
        <div
          class="w-full bg-chart-1 rounded-t opacity-80 hover:opacity-100 transition-opacity min-h-[2px]"
          style="height: {(count / maxVal) * 100}%"
          title="{date}: {count.toLocaleString()} rows"
        ></div>
      </div>
    {/each}
  </div>
  <div class="flex justify-between mt-1">
    <span class="text-[10px] text-muted-foreground">{grouped()[0]?.[0] ?? ''}</span>
    <span class="text-[10px] text-muted-foreground">{grouped()[grouped().length - 1]?.[0] ?? ''}</span>
  </div>
{/if}
