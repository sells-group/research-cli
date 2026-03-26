<script lang="ts">
  import MetricCard from '$lib/components/shared/MetricCard.svelte';
  import CategoryCard from './CategoryCard.svelte';
  import DatasetCard from './DatasetCard.svelte';
  import EmptyState from '$lib/components/shared/EmptyState.svelte';
  import { Input } from '$lib/components/ui/input';
  import Search from 'lucide-svelte/icons/search';
  import Database from 'lucide-svelte/icons/database';
  import Layers from 'lucide-svelte/icons/layers';
  import Building2 from 'lucide-svelte/icons/building-2';
  import Clock from 'lucide-svelte/icons/clock';
  import X from 'lucide-svelte/icons/x';
  import { Button } from '$lib/components/ui/button';
  import { categories, getCategoryMeta } from '$lib/config/categories';
  import { formatRowCount, groupTablesByCategory, formatSyncDate, type EnrichedTable } from '$lib/utils/table-helpers';

  interface Props {
    tables: EnrichedTable[];
    onSelectTable: (tableId: string) => void;
  }

  let { tables, onSelectTable }: Props = $props();

  let searchQuery = $state('');
  let selectedCategory = $state<string | null>(null);

  // Summary metrics
  const totalDatasets = $derived(tables.length);
  const totalRows = $derived(tables.reduce((sum, t) => sum + (t.estimated_row_count || 0), 0));
  const dataSources = $derived(new Set(tables.map(t => t.category)).size);
  const lastUpdated = $derived.by(() => {
    const synced = tables.filter(t => t.lastSync).sort((a, b) => {
      return new Date(b.lastSync!).getTime() - new Date(a.lastSync!).getTime();
    });
    return synced.length ? formatSyncDate(synced[0].lastSync) : 'N/A';
  });

  // Filter tables by search and category
  const filteredTables = $derived.by(() => {
    let result = tables;
    if (selectedCategory) {
      result = result.filter(t => t.category === selectedCategory);
    }
    if (searchQuery.trim()) {
      const q = searchQuery.toLowerCase();
      result = result.filter(t =>
        t.friendlyName.toLowerCase().includes(q) ||
        t.description.toLowerCase().includes(q) ||
        t.category.toLowerCase().includes(q) ||
        t.id.toLowerCase().includes(q)
      );
    }
    return result;
  });

  // Group for display
  const grouped = $derived(groupTablesByCategory(filteredTables));

  // Category stats for cards
  const categoryStats = $derived.by(() => {
    const stats = new Map<string, { count: number; rows: number }>();
    for (const t of tables) {
      const cat = t.category || 'Other';
      const existing = stats.get(cat) ?? { count: 0, rows: 0 };
      existing.count++;
      existing.rows += t.estimated_row_count || 0;
      stats.set(cat, existing);
    }
    return stats;
  });

  // Which categories to show cards for
  const visibleCategories = $derived(
    categories.filter(c => categoryStats.has(c.key))
  );

  function toggleCategory(key: string) {
    selectedCategory = selectedCategory === key ? null : key;
  }

  function clearFilters() {
    searchQuery = '';
    selectedCategory = null;
  }
</script>

<div class="flex-1 overflow-auto">
  <div class="max-w-7xl mx-auto px-4 py-6 space-y-6">
    <!-- Summary Metrics -->
    <div class="grid grid-cols-2 md:grid-cols-4 gap-3">
      <MetricCard label="Total Datasets" value={totalDatasets} icon={Layers} />
      <MetricCard label="Estimated Records" value={formatRowCount(totalRows)} icon={Database} />
      <MetricCard label="Data Sources" value={dataSources} icon={Building2} />
      <MetricCard label="Last Updated" value={lastUpdated} icon={Clock} />
    </div>

    <!-- Search Bar -->
    <div class="relative max-w-lg">
      <Search class="absolute left-3 top-1/2 -translate-y-1/2 size-4 text-muted-foreground" />
      <Input
        type="text"
        placeholder="Search datasets by name, description, or agency..."
        class="pl-10 h-10"
        value={searchQuery}
        oninput={(e: Event) => { searchQuery = (e.target as HTMLInputElement).value; }}
      />
      {#if searchQuery || selectedCategory}
        <Button
          variant="ghost"
          size="icon"
          class="absolute right-1 top-1/2 -translate-y-1/2 h-7 w-7"
          onclick={clearFilters}
        >
          <X class="size-3.5" />
        </Button>
      {/if}
    </div>

    <!-- Category Cards -->
    <div>
      <h3 class="text-xs font-medium text-muted-foreground uppercase tracking-wider mb-3">Browse by Agency</h3>
      <div class="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 gap-2">
        {#each visibleCategories as cat}
          {@const stats = categoryStats.get(cat.key)}
          <CategoryCard
            category={cat}
            datasetCount={stats?.count ?? 0}
            totalRows={stats?.rows ?? 0}
            selected={selectedCategory === cat.key}
            onclick={() => toggleCategory(cat.key)}
          />
        {/each}
      </div>
    </div>

    <!-- Dataset Grid -->
    {#if filteredTables.length === 0}
      <EmptyState
        title="No datasets found"
        description="Try adjusting your search or clearing filters."
        icon={Database}
      />
    {:else}
      {#each grouped as [category, categoryTables]}
        {@const meta = getCategoryMeta(category)}
        <div>
          <h3 class="text-sm font-semibold mb-2 flex items-center gap-2">
            {meta?.label ?? category}
            <span class="text-xs text-muted-foreground font-normal">({categoryTables.length})</span>
          </h3>
          <div class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
            {#each categoryTables as table}
              <DatasetCard
                {table}
                onclick={() => onSelectTable(table.id)}
              />
            {/each}
          </div>
        </div>
      {/each}
    {/if}
  </div>
</div>
