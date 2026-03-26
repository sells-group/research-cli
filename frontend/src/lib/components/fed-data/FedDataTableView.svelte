<script lang="ts">
  import { api } from '$lib/api';
  import DataTable from '$lib/components/shared/DataTable.svelte';
  import { Input } from '$lib/components/ui/input';
  import { Badge } from '$lib/components/ui/badge';
  import Search from 'lucide-svelte/icons/search';
  import LoaderCircle from 'lucide-svelte/icons/loader-circle';
  import Database from 'lucide-svelte/icons/database';
  import type { TableColumn } from '$lib/types';
  import { formatRowCount, formatSyncDate, type EnrichedTable } from '$lib/utils/table-helpers';

  interface Props {
    table: EnrichedTable;
    onRowClick: (row: Record<string, any>) => void;
  }

  let { table, onRowClick }: Props = $props();

  let columns = $state<TableColumn[]>([]);
  let rows = $state<Record<string, any>[]>([]);
  let total = $state(0);
  let page = $state(1);
  let perPage = 50;
  let sortCol = $state('');
  let sortOrder = $state<'asc' | 'desc'>('asc');
  let loading = $state(false);
  let searchQuery = $state('');
  let searchColumn = $state('');
  let searchTimer: ReturnType<typeof setTimeout>;
  let tableError = $state('');
  let selectedRowId = $state<number | string | null>(null);

  const textColumns = $derived(columns.filter(c => c.type === 'text'));

  // React to table changes
  $effect(() => {
    if (table) {
      columns = table.columns;
      const firstText = table.columns.find(c => c.type === 'text');
      searchColumn = firstText?.key ?? '';
      page = 1;
      sortCol = '';
      sortOrder = 'asc';
      searchQuery = '';
      tableError = '';
      selectedRowId = null;
      loadData();
    }
  });

  async function loadData() {
    if (!table) return;
    loading = true;
    tableError = '';
    try {
      const offset = (page - 1) * perPage;
      const params: Record<string, any> = {
        limit: perPage,
        offset,
        sort: sortCol,
        dir: sortOrder,
      };
      if (searchQuery && searchColumn) {
        params.search_col = searchColumn;
        params.search_val = searchQuery;
      }
      const result = await api.queryDataTable(table.id, params);
      rows = result.rows ?? [];
      total = result.total_rows ?? 0;
      perPage = result.page_size ?? perPage;
    } catch (e) {
      console.error('Failed to load data:', e);
      rows = [];
      total = 0;
      tableError = 'Failed to load table data';
    } finally {
      loading = false;
    }
  }

  function handleSort(col: string) {
    if (sortCol === col) {
      sortOrder = sortOrder === 'asc' ? 'desc' : 'asc';
    } else {
      sortCol = col;
      sortOrder = 'asc';
    }
    page = 1;
    loadData();
  }

  function handleSearch(value: string) {
    searchQuery = value;
    page = 1;
    loadData();
  }

  function handleRowClick(row: Record<string, any>) {
    selectedRowId = row.id ?? null;
    onRowClick(row);
  }
</script>

<div class="flex-1 flex flex-col overflow-hidden">
  <!-- Header -->
  <div class="px-4 py-3 bg-card border-b border-border space-y-2">
    <div class="flex items-start justify-between gap-4">
      <div>
        <h2 class="text-lg font-semibold">{table.friendlyName}</h2>
        {#if table.description}
          <p class="text-xs text-muted-foreground mt-0.5">{table.description}</p>
        {/if}
      </div>
      <div class="flex items-center gap-2 shrink-0">
        <span class="inline-flex items-center gap-1 text-xs text-muted-foreground">
          <Database class="size-3" />
          ~{formatRowCount(table.estimated_row_count)} rows
        </span>
        {#if table.cadence}
          <Badge variant="outline" class="text-[10px] capitalize">{table.cadence}</Badge>
        {/if}
        {#if table.lastSync}
          <span class="text-xs text-muted-foreground">
            Synced {formatSyncDate(table.lastSync)}
          </span>
        {/if}
      </div>
    </div>

    <!-- Search toolbar -->
    <div class="flex items-center gap-3">
      {#if textColumns.length > 0}
        <div class="flex items-center gap-1.5 flex-1 max-w-[400px]">
          <select
            class="h-8 text-xs bg-background border border-input rounded-md px-1.5 shrink-0"
            value={searchColumn}
            onchange={(e: Event) => {
              searchColumn = (e.target as HTMLSelectElement).value;
              if (searchQuery) { page = 1; loadData(); }
            }}
          >
            {#each textColumns as col}
              <option value={col.key}>{col.label}</option>
            {/each}
          </select>
          <div class="relative flex-1">
            <Search class="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground" />
            <Input
              type="text"
              placeholder="Search..."
              class="h-8 text-xs pl-8"
              value={searchQuery}
              oninput={(e: Event) => {
                const val = (e.target as HTMLInputElement).value;
                clearTimeout(searchTimer);
                searchTimer = setTimeout(() => handleSearch(val), 300);
              }}
            />
          </div>
        </div>
      {/if}

      {#if loading}
        <div class="flex items-center gap-1.5 text-xs text-muted-foreground shrink-0">
          <LoaderCircle class="size-3.5 animate-spin" />
          <span>Loading...</span>
        </div>
      {/if}
    </div>
  </div>

  {#if tableError}
    <div class="px-4 py-2 text-xs text-destructive bg-destructive/10 border-b border-border">
      {tableError}
    </div>
  {/if}

  <DataTable
    {columns}
    {rows}
    {total}
    {page}
    {perPage}
    {sortCol}
    {sortOrder}
    {loading}
    selectedId={selectedRowId}
    onSort={handleSort}
    onPageChange={(p) => { page = p; loadData(); }}
    onRowClick={handleRowClick}
  />
</div>
