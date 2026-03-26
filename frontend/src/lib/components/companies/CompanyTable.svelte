<script lang="ts">
  import * as Table from '$lib/components/ui/table';
  import { Button } from '$lib/components/ui/button';
  import { Skeleton } from '$lib/components/ui/skeleton';
  import ConfidenceBar from '$lib/components/shared/ConfidenceBar.svelte';
  import ChevronLeft from 'lucide-svelte/icons/chevron-left';
  import ChevronRight from 'lucide-svelte/icons/chevron-right';
  import type { CompanyRecord } from '$lib/types';

  interface Props {
    companies: CompanyRecord[];
    total: number;
    loading: boolean;
    selected: CompanyRecord | null;
    page: number;
    perPage: number;
    onSelect: (c: CompanyRecord) => void;
    onPageChange: (p: number) => void;
  }

  let { companies, total, loading, selected, page, perPage, onSelect, onPageChange }: Props = $props();

  const totalPages = $derived(Math.ceil(total / perPage));
</script>

<div class="flex-1 flex flex-col overflow-hidden">
  <div class="flex-1 overflow-auto">
    <Table.Root>
      <Table.Header class="sticky top-0 bg-card z-10">
        <Table.Row>
          <Table.Head class="text-xs">Name</Table.Head>
          <Table.Head class="text-xs">Domain</Table.Head>
          <Table.Head class="text-xs">State</Table.Head>
          <Table.Head class="text-xs">NAICS</Table.Head>
          <Table.Head class="text-xs w-28">Score</Table.Head>
        </Table.Row>
      </Table.Header>
      <Table.Body>
        {#if loading}
          {#each Array(8) as _}
            <Table.Row>
              <Table.Cell><Skeleton class="h-4 w-32" /></Table.Cell>
              <Table.Cell><Skeleton class="h-4 w-24" /></Table.Cell>
              <Table.Cell><Skeleton class="h-4 w-8" /></Table.Cell>
              <Table.Cell><Skeleton class="h-4 w-16" /></Table.Cell>
              <Table.Cell><Skeleton class="h-4 w-20" /></Table.Cell>
            </Table.Row>
          {/each}
        {:else}
          {#each companies as company}
            <Table.Row
              class="cursor-pointer transition-colors {selected?.id === company.id ? 'bg-accent' : 'hover:bg-accent/50'}"
              onclick={() => onSelect(company)}
            >
              <Table.Cell class="text-xs py-2 font-medium truncate max-w-[200px]">{company.name ?? '-'}</Table.Cell>
              <Table.Cell class="text-xs py-2 text-muted-foreground truncate max-w-[150px]">{company.domain ?? '-'}</Table.Cell>
              <Table.Cell class="text-xs py-2">{company.state ?? '-'}</Table.Cell>
              <Table.Cell class="text-xs py-2 font-mono">{company.naics_code ?? '-'}</Table.Cell>
              <Table.Cell class="text-xs py-2">
                {#if company.enrichment_score != null}
                  <ConfidenceBar value={company.enrichment_score} />
                {:else}
                  <span class="text-muted-foreground">-</span>
                {/if}
              </Table.Cell>
            </Table.Row>
          {/each}
        {/if}
      </Table.Body>
    </Table.Root>
  </div>

  <div class="flex items-center justify-between px-4 py-2 border-t border-border shrink-0">
    <span class="text-xs text-muted-foreground">{total.toLocaleString()} companies</span>
    <div class="flex items-center gap-2">
      <Button variant="outline" size="icon" class="h-7 w-7" disabled={page <= 1} onclick={() => onPageChange(page - 1)}>
        <ChevronLeft class="size-3.5" />
      </Button>
      <span class="text-xs text-muted-foreground">Page {page} of {totalPages}</span>
      <Button variant="outline" size="icon" class="h-7 w-7" disabled={page >= totalPages} onclick={() => onPageChange(page + 1)}>
        <ChevronRight class="size-3.5" />
      </Button>
    </div>
  </div>
</div>
