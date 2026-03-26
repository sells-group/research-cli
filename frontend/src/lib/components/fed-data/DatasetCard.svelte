<script lang="ts">
  import * as Card from '$lib/components/ui/card';
  import { Badge } from '$lib/components/ui/badge';
  import { formatRowCount, formatSyncDate, type EnrichedTable } from '$lib/utils/table-helpers';
  import Database from 'lucide-svelte/icons/database';
  import Clock from 'lucide-svelte/icons/clock';
  import Circle from 'lucide-svelte/icons/circle';

  interface Props {
    table: EnrichedTable;
    onclick: () => void;
  }

  let { table, onclick }: Props = $props();

  const statusColor = $derived(
    table.lastStatus === 'complete' ? 'text-green-500' :
    table.lastStatus === 'failed' ? 'text-red-500' :
    table.lastStatus === 'running' ? 'text-yellow-500' :
    'text-muted-foreground'
  );
</script>

<button class="text-left w-full" {onclick}>
  <Card.Root class="p-4 h-full transition-all hover:ring-2 hover:ring-primary/30 hover:shadow-sm cursor-pointer">
    <div class="flex items-start justify-between gap-2">
      <p class="text-sm font-medium leading-tight">{table.friendlyName}</p>
      {#if table.lastStatus}
        <Circle class="size-2.5 shrink-0 mt-1 fill-current {statusColor}" />
      {/if}
    </div>
    {#if table.description}
      <p class="text-xs text-muted-foreground mt-1 line-clamp-2">{table.description}</p>
    {/if}
    <div class="flex items-center gap-2 mt-3 flex-wrap">
      <span class="inline-flex items-center gap-1 text-xs text-muted-foreground">
        <Database class="size-3" />
        ~{formatRowCount(table.estimated_row_count)}
      </span>
      {#if table.cadence}
        <Badge variant="outline" class="text-[10px] px-1.5 py-0 h-4 capitalize">{table.cadence}</Badge>
      {/if}
      {#if table.lastSync}
        <span class="inline-flex items-center gap-1 text-[10px] text-muted-foreground">
          <Clock class="size-2.5" />
          {formatSyncDate(table.lastSync)}
        </span>
      {/if}
    </div>
  </Card.Root>
</button>
