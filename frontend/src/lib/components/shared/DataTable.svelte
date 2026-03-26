<script lang="ts">
  import * as Table from '$lib/components/ui/table';
  import { Button } from '$lib/components/ui/button';
  import { Skeleton } from '$lib/components/ui/skeleton';
  import ChevronUp from 'lucide-svelte/icons/chevron-up';
  import ChevronDown from 'lucide-svelte/icons/chevron-down';
  import ChevronsUpDown from 'lucide-svelte/icons/chevrons-up-down';
  import ChevronLeft from 'lucide-svelte/icons/chevron-left';
  import ChevronRight from 'lucide-svelte/icons/chevron-right';

  import type { TableColumn } from '$lib/types';

  interface Props {
    columns: TableColumn[];
    rows: Record<string, any>[];
    total: number;
    page: number;
    perPage: number;
    sortCol: string;
    sortOrder: 'asc' | 'desc';
    loading: boolean;
    selectedId?: number | string | null;
    onSort: (col: string) => void;
    onPageChange: (page: number) => void;
    onRowClick?: (row: Record<string, any>) => void;
  }

  let { columns, rows, total, page, perPage, sortCol, sortOrder, loading, selectedId = null, onSort, onPageChange, onRowClick }: Props = $props();

  const totalPages = $derived(Math.ceil(total / perPage));

  function formatCell(value: any, type: string): string {
    if (value == null) return '-';
    if (type === 'currency') return `$${Number(value).toLocaleString()}`;
    if (type === 'number') return Number(value).toLocaleString();
    if (type === 'date') return new Date(value).toLocaleDateString();
    return String(value);
  }
</script>

<div class="flex-1 flex flex-col overflow-hidden">
  <div class="flex-1 overflow-auto">
    <Table.Root>
      <Table.Header class="sticky top-0 bg-card z-10">
        <Table.Row>
          {#each columns as col}
            <Table.Head class="text-xs whitespace-nowrap">
              {#if col.sortable}
                <button class="flex items-center gap-1 hover:text-foreground transition-colors" onclick={() => onSort(col.key)}>
                  {col.label}
                  {#if sortCol === col.key}
                    {#if sortOrder === 'asc'}
                      <ChevronUp class="size-3" />
                    {:else}
                      <ChevronDown class="size-3" />
                    {/if}
                  {:else}
                    <ChevronsUpDown class="size-3 opacity-30" />
                  {/if}
                </button>
              {:else}
                {col.label}
              {/if}
            </Table.Head>
          {/each}
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#if loading}
          {#each Array(5) as _}
            <Table.Row>
              {#each columns as _col}
                <Table.Cell><Skeleton class="h-4 w-20" /></Table.Cell>
              {/each}
            </Table.Row>
          {/each}
        {:else}
          {#if rows.length === 0}
            <Table.Row>
              <Table.Cell colspan={columns.length} class="text-center text-xs text-muted-foreground py-8">
                No data found
              </Table.Cell>
            </Table.Row>
          {:else}
            {#each rows as row}
              <Table.Row
                class="cursor-pointer transition-colors {row.id === selectedId ? 'bg-accent' : 'hover:bg-accent/50'}"
                onclick={() => onRowClick?.(row)}
              >
                {#each columns as col}
                  <Table.Cell class="text-xs py-2 max-w-[200px] truncate">
                    {formatCell(row[col.key], col.type)}
                  </Table.Cell>
                {/each}
              </Table.Row>
            {/each}
          {/if}
        {/if}
      </Table.Body>
    </Table.Root>
  </div>

  <div class="flex items-center justify-between px-4 py-2 border-t border-border bg-card shrink-0">
    <span class="text-xs text-muted-foreground">
      {total.toLocaleString()} total rows
    </span>
    <div class="flex items-center gap-2">
      <Button variant="outline" size="icon" class="h-7 w-7" disabled={page <= 1} onclick={() => onPageChange(page - 1)}>
        <ChevronLeft class="size-3.5" />
      </Button>
      <span class="text-xs text-muted-foreground">
        Page {page} of {totalPages}
      </span>
      <Button variant="outline" size="icon" class="h-7 w-7" disabled={page >= totalPages} onclick={() => onPageChange(page + 1)}>
        <ChevronRight class="size-3.5" />
      </Button>
    </div>
  </div>
</div>
