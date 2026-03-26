<script lang="ts">
  import { api } from '$lib/api';
  import * as Card from '$lib/components/ui/card';
  import * as Tabs from '$lib/components/ui/tabs';
  import StatusBadge from '$lib/components/shared/StatusBadge.svelte';
  import ConfidenceBar from '$lib/components/shared/ConfidenceBar.svelte';
  import PhaseTimeline from './PhaseTimeline.svelte';
  import type { Run, FieldProvenance } from '$lib/types';

  interface Props {
    run: Run;
  }

  let { run }: Props = $props();
  let provenance = $state<FieldProvenance[]>([]);

  async function loadProvenance() {
    try {
      const data = await api.getRunProvenance(run.id);
      provenance = data.provenance ?? [];
    } catch {
      provenance = [];
    }
  }

  $effect(() => {
    if (run?.id) loadProvenance();
  });
</script>

<div class="p-6 space-y-6">
  <!-- Header -->
  <div class="flex items-center justify-between">
    <div>
      <h2 class="text-lg font-semibold">{run.company?.name ?? run.company?.url ?? 'Unknown'}</h2>
      <p class="text-xs text-muted-foreground mt-1">{run.company?.url}</p>
    </div>
    <StatusBadge status={run.status} />
  </div>

  <!-- Summary cards -->
  {#if run.result}
    <div class="grid grid-cols-4 gap-3">
      <Card.Root class="p-3">
        <p class="text-xs text-muted-foreground">Score</p>
        <p class="text-xl font-bold">{(run.result.score * 100).toFixed(0)}%</p>
      </Card.Root>
      <Card.Root class="p-3">
        <p class="text-xs text-muted-foreground">Fields</p>
        <p class="text-xl font-bold">{run.result.fields_found}/{run.result.fields_total}</p>
      </Card.Root>
      <Card.Root class="p-3">
        <p class="text-xs text-muted-foreground">Tokens</p>
        <p class="text-xl font-bold">{run.result.total_tokens.toLocaleString()}</p>
      </Card.Root>
      <Card.Root class="p-3">
        <p class="text-xs text-muted-foreground">Cost</p>
        <p class="text-xl font-bold">${run.result.total_cost.toFixed(3)}</p>
      </Card.Root>
    </div>
  {/if}

  <Tabs.Root value="phases">
    <Tabs.List>
      <Tabs.Trigger value="phases" class="text-xs">Phases</Tabs.Trigger>
      <Tabs.Trigger value="answers" class="text-xs">Answers</Tabs.Trigger>
      <Tabs.Trigger value="provenance" class="text-xs">Provenance</Tabs.Trigger>
    </Tabs.List>

    <Tabs.Content value="phases" class="mt-4">
      {#if run.result?.phases}
        <PhaseTimeline phases={run.result.phases} />
      {:else}
        <p class="text-xs text-muted-foreground">No phase data</p>
      {/if}
    </Tabs.Content>

    <Tabs.Content value="answers" class="mt-4">
      {#if run.result?.answers?.length}
        <div class="space-y-2">
          {#each run.result.answers as answer}
            <div class="flex items-center gap-3 px-3 py-2 rounded-md bg-muted/50">
              <span class="text-xs font-medium w-40 truncate">{answer.field_key}</span>
              <span class="text-xs flex-1 truncate">{answer.value ?? '-'}</span>
              <div class="w-24">
                <ConfidenceBar value={answer.confidence} />
              </div>
              <span class="text-[10px] text-muted-foreground w-8">T{answer.tier}</span>
            </div>
          {/each}
        </div>
      {:else}
        <p class="text-xs text-muted-foreground">No answers extracted</p>
      {/if}
    </Tabs.Content>

    <Tabs.Content value="provenance" class="mt-4">
      {#if provenance.length}
        <div class="space-y-2">
          {#each provenance as p}
            <div class="px-3 py-2 rounded-md bg-muted/50">
              <div class="flex items-center gap-3">
                <span class="text-xs font-medium w-40 truncate">{p.field_key}</span>
                <span class="text-xs flex-1 truncate">{p.winner_value ?? '-'}</span>
                <div class="w-24">
                  <ConfidenceBar value={p.effective_confidence} />
                </div>
                <span class="text-[10px] {p.threshold_met ? 'text-green-500' : 'text-red-500'}">
                  {p.threshold_met ? 'PASS' : 'FAIL'}
                </span>
              </div>
              {#if p.value_changed}
                <p class="text-[10px] text-amber-500 mt-1">Changed from: {p.previous_value ?? 'null'}</p>
              {/if}
            </div>
          {/each}
        </div>
      {:else}
        <p class="text-xs text-muted-foreground">No provenance data</p>
      {/if}
    </Tabs.Content>
  </Tabs.Root>

  <!-- Error -->
  {#if run.error}
    <Card.Root class="p-4 border-red-500/20">
      <p class="text-xs font-medium text-red-500">Error: {run.error.category}</p>
      <p class="text-xs text-muted-foreground mt-1">{run.error.message}</p>
      {#if run.error.failed_phase}
        <p class="text-xs text-muted-foreground mt-1">Failed at: {run.error.failed_phase}</p>
      {/if}
    </Card.Root>
  {/if}
</div>
