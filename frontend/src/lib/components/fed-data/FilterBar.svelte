<script lang="ts">
  import { Button } from '$lib/components/ui/button';
  import { Input } from '$lib/components/ui/input';
  import X from 'lucide-svelte/icons/x';
  import type { TableColumn } from '$lib/types';

  interface Props {
    columns: TableColumn[];
    filters: Record<string, any>;
    table: string;
    onFilterChange: (filters: Record<string, any>) => void;
  }

  let { columns, filters, table, onFilterChange }: Props = $props();

  const filterableColumns = $derived(columns.filter(c => c.filter));

  function updateFilter(key: string, value: any) {
    const newFilters = { ...filters, [key]: value };
    onFilterChange(newFilters);
  }

  function clearFilters() {
    onFilterChange({});
  }

  const hasFilters = $derived(Object.values(filters).some(v => v != null && v !== ''));
</script>

{#if filterableColumns.length > 0}
  <div class="flex items-center gap-2 px-4 py-1.5 bg-card/50 border-b border-border overflow-x-auto">
    {#each filterableColumns.slice(0, 6) as col}
      <div class="shrink-0">
        <Input
          type="text"
          placeholder={col.label}
          class="h-7 text-xs w-[130px]"
          value={filters[col.key] ?? ''}
          oninput={(e: Event) => updateFilter(col.key, (e.target as HTMLInputElement).value)}
        />
      </div>
    {/each}
    {#if hasFilters}
      <Button variant="ghost" size="sm" class="h-7 text-xs gap-1 shrink-0" onclick={clearFilters}>
        <X class="size-3" />
        Clear
      </Button>
    {/if}
  </div>
{/if}
