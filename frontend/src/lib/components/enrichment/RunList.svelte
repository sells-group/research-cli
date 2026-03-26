<script lang="ts">
  import StatusBadge from '$lib/components/shared/StatusBadge.svelte';
  import { Input } from '$lib/components/ui/input';
  import { Button } from '$lib/components/ui/button';
  import { runFilter } from '$lib/stores/enrichment';
  import { Skeleton } from '$lib/components/ui/skeleton';
  import ChevronLeft from 'lucide-svelte/icons/chevron-left';
  import ChevronRight from 'lucide-svelte/icons/chevron-right';
  import Search from 'lucide-svelte/icons/search';
  import type { Run } from '$lib/types';

  interface Props {
    runs: Run[];
    total: number;
    loading: boolean;
    selected: Run | null;
    page: number;
    perPage: number;
    onSelect: (run: Run) => void;
    onPageChange: (page: number) => void;
  }

  let { runs, total, loading, selected, page, perPage, onSelect, onPageChange }: Props = $props();

  let searchTimer: ReturnType<typeof setTimeout>;
  const totalPages = $derived(Math.ceil(total / perPage));

  function handleSearch(value: string) {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      runFilter.update(f => ({ ...f, search: value }));
    }, 300);
  }

  function formatDate(ts: string): string {
    return new Date(ts).toLocaleDateString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' });
  }
</script>

<div class="flex flex-col h-full">
  <!-- Filters -->
  <div class="p-3 space-y-2 border-b border-border">
    <div class="relative">
      <Search class="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground" />
      <Input
        type="text"
        placeholder="Search by company URL..."
        class="h-8 text-xs pl-8"
        oninput={(e: Event) => handleSearch((e.target as HTMLInputElement).value)}
      />
    </div>
    <div class="flex gap-2">
      <select
        class="h-7 text-xs bg-background border border-input rounded-md px-2 flex-1"
        onchange={(e: Event) => runFilter.update(f => ({ ...f, status: (e.target as HTMLSelectElement).value as any }))}
      >
        <option value="all">All statuses</option>
        <option value="queued">Queued</option>
        <option value="crawling">Crawling</option>
        <option value="extracting">Extracting</option>
        <option value="complete">Complete</option>
        <option value="failed">Failed</option>
      </select>
      <select
        class="h-7 text-xs bg-background border border-input rounded-md px-2 flex-1"
        onchange={(e: Event) => runFilter.update(f => ({ ...f, sortBy: (e.target as HTMLSelectElement).value as any }))}
      >
        <option value="created_at">Date</option>
        <option value="score">Score</option>
        <option value="cost">Cost</option>
      </select>
    </div>
  </div>

  <!-- Run items -->
  <div class="flex-1 overflow-auto">
    {#if loading}
      {#each Array(8) as _}
        <div class="px-3 py-2 border-b border-border">
          <Skeleton class="h-4 w-3/4 mb-1" />
          <Skeleton class="h-3 w-1/2" />
        </div>
      {/each}
    {:else}
      {#each runs as run}
        <button
          class="w-full text-left px-3 py-2.5 border-b border-border hover:bg-accent/50 transition-colors {selected?.id === run.id ? 'bg-accent' : ''}"
          onclick={() => onSelect(run)}
        >
          <div class="flex items-center gap-2">
            <StatusBadge status={run.status} size="sm" />
            <span class="text-xs font-medium truncate flex-1">{run.company?.name ?? run.company?.url ?? 'Unknown'}</span>
          </div>
          <div class="flex items-center gap-2 mt-1">
            <span class="text-[10px] text-muted-foreground">{formatDate(run.created_at)}</span>
            {#if run.result?.score != null}
              <span class="text-[10px] text-muted-foreground">Score: {(run.result.score * 100).toFixed(0)}%</span>
            {/if}
            {#if run.result?.total_cost != null}
              <span class="text-[10px] text-muted-foreground">${run.result.total_cost.toFixed(3)}</span>
            {/if}
          </div>
        </button>
      {:else}
        <p class="text-xs text-muted-foreground p-4">No runs found</p>
      {/each}
    {/if}
  </div>

  <!-- Pagination -->
  <div class="flex items-center justify-between px-3 py-2 border-t border-border shrink-0">
    <span class="text-xs text-muted-foreground">{total} runs</span>
    <div class="flex items-center gap-1">
      <Button variant="outline" size="icon" class="h-6 w-6" disabled={page <= 1} onclick={() => onPageChange(page - 1)}>
        <ChevronLeft class="size-3" />
      </Button>
      <span class="text-xs text-muted-foreground px-1">{page}/{totalPages || 1}</span>
      <Button variant="outline" size="icon" class="h-6 w-6" disabled={page >= totalPages} onclick={() => onPageChange(page + 1)}>
        <ChevronRight class="size-3" />
      </Button>
    </div>
  </div>
</div>
