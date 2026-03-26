<script lang="ts">
  import { onMount, onDestroy } from 'svelte';
  import { api } from '$lib/api';
  import FedDataCatalog from './FedDataCatalog.svelte';
  import FedDataTableView from './FedDataTableView.svelte';
  import FedDataBreadcrumb from './FedDataBreadcrumb.svelte';
  import RowDetailSheet from './RowDetailSheet.svelte';
  import LoadingSpinner from '$lib/components/shared/LoadingSpinner.svelte';
  import { Button } from '$lib/components/ui/button';
  import ArrowLeft from 'lucide-svelte/icons/arrow-left';
  import { enrichTables, type EnrichedTable } from '$lib/utils/table-helpers';
  import { getCategoryMeta } from '$lib/config/categories';
  import { getHashSubPath, pushHash } from '$lib/stores/view';
  import type { TableMeta, DatasetStatus } from '$lib/types';

  let mode = $state<'catalog' | 'table'>('catalog');
  let tables = $state<EnrichedTable[]>([]);
  let selectedTableId = $state('');
  let loading = $state(true);
  let error = $state('');

  // Row detail sheet state
  let showRowDetail = $state(false);
  let selectedRow = $state<Record<string, any> | null>(null);

  const selectedTable = $derived(tables.find(t => t.id === selectedTableId));

  const breadcrumbs = $derived.by(() => {
    const crumbs: { label: string; onclick?: () => void }[] = [
      { label: 'Fed Data', onclick: () => navigateToCatalog() },
    ];
    if (mode === 'table' && selectedTable) {
      const cat = getCategoryMeta(selectedTable.category);
      if (cat) {
        crumbs.push({ label: cat.label, onclick: () => navigateToCatalog() });
      }
      crumbs.push({ label: selectedTable.friendlyName });
    }
    return crumbs;
  });

  /** Sync component state from the URL hash sub-path. */
  function syncFromHash() {
    const sub = getHashSubPath();
    if (sub.length > 0 && sub[0]) {
      selectedTableId = sub[0];
      mode = 'table';
    } else {
      mode = 'catalog';
      selectedTableId = '';
    }
    showRowDetail = false;
    selectedRow = null;
  }

  function onPopState() {
    // Only react if we're still on the fed-data view
    const hash = window.location.hash.replace(/^#\/?/, '');
    if (hash.startsWith('fed-data')) {
      syncFromHash();
    }
  }

  onMount(async () => {
    window.addEventListener('popstate', onPopState);

    // Load data first, then sync from hash (so table lookup works)
    try {
      const [tablesRes, statusesRes] = await Promise.all([
        api.listDataTables(),
        api.fedsyncStatuses().catch(() => ({ datasets: [] as DatasetStatus[] })),
      ]);
      const rawTables: TableMeta[] = tablesRes.tables ?? [];
      const statuses: DatasetStatus[] = statusesRes.datasets ?? [];
      tables = enrichTables(rawTables, statuses);

      // Restore state from URL if it has a table sub-path
      syncFromHash();
    } catch (e) {
      console.error('Failed to load fed data:', e);
      error = 'Failed to load data catalog';
    } finally {
      loading = false;
    }
  });

  onDestroy(() => {
    if (typeof window !== 'undefined') {
      window.removeEventListener('popstate', onPopState);
    }
  });

  function navigateToTable(tableId: string) {
    selectedTableId = tableId;
    mode = 'table';
    showRowDetail = false;
    selectedRow = null;
    pushHash(`fed-data/${tableId}`);
  }

  function navigateToCatalog() {
    mode = 'catalog';
    selectedTableId = '';
    showRowDetail = false;
    selectedRow = null;
    pushHash('fed-data');
  }

  function handleRowClick(row: Record<string, any>) {
    selectedRow = row;
    showRowDetail = true;
  }

  function closeRowDetail() {
    showRowDetail = false;
    selectedRow = null;
  }
</script>

<div class="flex-1 flex flex-col overflow-hidden">
  <!-- Top bar with breadcrumb and back button -->
  <div class="flex items-center gap-2 px-4 py-2 bg-card border-b border-border shrink-0">
    {#if mode === 'table'}
      <Button
        variant="ghost"
        size="icon"
        class="h-7 w-7 shrink-0"
        onclick={navigateToCatalog}
      >
        <ArrowLeft class="size-4" />
      </Button>
    {/if}
    <FedDataBreadcrumb crumbs={breadcrumbs} />
  </div>

  {#if loading}
    <div class="flex-1 flex items-center justify-center">
      <LoadingSpinner label="Loading data catalog..." />
    </div>
  {:else if error}
    <div class="px-4 py-8 text-center">
      <p class="text-sm text-destructive">{error}</p>
    </div>
  {:else if mode === 'catalog'}
    <FedDataCatalog {tables} onSelectTable={navigateToTable} />
  {:else if mode === 'table' && selectedTable}
    <FedDataTableView table={selectedTable} onRowClick={handleRowClick} />
  {/if}

  <!-- Row Detail Sheet -->
  {#if selectedTable}
    <RowDetailSheet
      open={showRowDetail}
      row={selectedRow}
      columns={selectedTable.columns}
      tableName={selectedTable.friendlyName}
      onClose={closeRowDetail}
    />
  {/if}
</div>
