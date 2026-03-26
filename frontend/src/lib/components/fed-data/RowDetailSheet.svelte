<script lang="ts">
  import * as Sheet from '$lib/components/ui/sheet';
  import { Button } from '$lib/components/ui/button';
  import { ScrollArea } from '$lib/components/ui/scroll-area';
  import Copy from 'lucide-svelte/icons/copy';
  import Check from 'lucide-svelte/icons/check';
  import type { TableColumn } from '$lib/types';
  import { humanizeTableName } from '$lib/utils/table-helpers';

  interface Props {
    open: boolean;
    row: Record<string, any> | null;
    columns: TableColumn[];
    tableName: string;
    onClose: () => void;
  }

  let { open, row, columns, tableName, onClose }: Props = $props();

  let copiedKey = $state<string | null>(null);

  function formatValue(value: any, type: string): string {
    if (value == null || value === '') return '-';
    if (type === 'currency') return `$${Number(value).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
    if (type === 'number') return Number(value).toLocaleString();
    if (type === 'date') {
      const d = new Date(value);
      return isNaN(d.getTime()) ? String(value) : d.toLocaleDateString('en-US', { year: 'numeric', month: 'short', day: 'numeric' });
    }
    return String(value);
  }

  async function copyValue(key: string, value: any) {
    try {
      await navigator.clipboard.writeText(String(value ?? ''));
      copiedKey = key;
      setTimeout(() => { copiedKey = null; }, 1500);
    } catch {
      // Clipboard not available
    }
  }

  // Build ordered entries: columns first (with labels), then any extra row keys
  const entries = $derived.by(() => {
    if (!row) return [];
    const colMap = new Map(columns.map(c => [c.key, c]));
    const result: { key: string; label: string; value: any; type: string }[] = [];
    const seen = new Set<string>();

    // Columns in order
    for (const col of columns) {
      if (col.key in row) {
        result.push({ key: col.key, label: col.label, value: row[col.key], type: col.type });
        seen.add(col.key);
      }
    }
    // Extra keys not in columns
    for (const key of Object.keys(row)) {
      if (!seen.has(key)) {
        result.push({ key, label: humanizeTableName(key), value: row[key], type: 'text' });
      }
    }
    return result;
  });
</script>

<Sheet.Root bind:open onOpenChange={(v) => { if (!v) onClose(); }}>
  <Sheet.Content side="right" class="!w-[420px] sm:!max-w-[480px] sm:!w-[480px] p-0 flex flex-col">
    <Sheet.Header class="px-4 pt-4 pb-3 border-b border-border shrink-0">
      <Sheet.Title class="text-base">{tableName} Record</Sheet.Title>
      <Sheet.Description class="text-xs">
        {#if row?.id}ID: {row.id}{/if}
      </Sheet.Description>
    </Sheet.Header>
    <ScrollArea class="flex-1">
      <div class="px-4 py-3 space-y-1">
        {#each entries as entry}
          <div class="group flex items-start gap-3 py-2 border-b border-border/50 last:border-0">
            <div class="w-[140px] shrink-0">
              <span class="text-xs text-muted-foreground">{entry.label}</span>
            </div>
            <div class="flex-1 min-w-0 flex items-start gap-1">
              <span class="text-sm break-words whitespace-pre-wrap">
                {formatValue(entry.value, entry.type)}
              </span>
              {#if entry.value != null && entry.value !== ''}
                <button
                  class="opacity-0 group-hover:opacity-100 transition-opacity shrink-0 p-0.5 rounded hover:bg-accent"
                  onclick={() => copyValue(entry.key, entry.value)}
                >
                  {#if copiedKey === entry.key}
                    <Check class="size-3 text-green-500" />
                  {:else}
                    <Copy class="size-3 text-muted-foreground" />
                  {/if}
                </button>
              {/if}
            </div>
          </div>
        {/each}
      </div>
    </ScrollArea>
  </Sheet.Content>
</Sheet.Root>
