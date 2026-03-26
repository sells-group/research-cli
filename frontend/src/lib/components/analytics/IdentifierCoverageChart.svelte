<script lang="ts">
  import type { IdentifierCoverage } from '$lib/types';
  import EmptyState from '$lib/components/shared/EmptyState.svelte';

  interface Props {
    data: IdentifierCoverage[];
  }

  let { data }: Props = $props();

  const colors = ['bg-blue-500', 'bg-purple-500', 'bg-green-500', 'bg-orange-500', 'bg-cyan-500', 'bg-amber-500', 'bg-indigo-500', 'bg-rose-500'];
</script>

{#if data.length === 0}
  <EmptyState title="No identifier data" description="Identifier coverage will appear when companies have identifiers" />
{:else}
  <div class="space-y-2">
    {#each data.sort((a, b) => b.percentage - a.percentage) as item, i}
      <div class="flex items-center gap-3">
        <span class="text-xs w-16 uppercase font-medium">{item.system}</span>
        <div class="flex-1 h-5 bg-muted rounded-full overflow-hidden">
          <div
            class="h-full rounded-full {colors[i % colors.length]} opacity-80"
            style="width: {item.percentage}%"
          ></div>
        </div>
        <span class="text-xs text-muted-foreground tabular-nums w-20 text-right">
          {item.count.toLocaleString()} ({item.percentage.toFixed(0)}%)
        </span>
      </div>
    {/each}
  </div>
{/if}
