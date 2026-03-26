<script lang="ts">
  import { enrichmentPhases } from '$lib/config/enrichment-phases';
  import type { PhaseResult } from '$lib/types';
  import CheckCircle from 'lucide-svelte/icons/check-circle-2';
  import XCircle from 'lucide-svelte/icons/x-circle';
  import Clock from 'lucide-svelte/icons/clock';
  import MinusCircle from 'lucide-svelte/icons/minus-circle';

  interface Props {
    phases: PhaseResult[];
  }

  let { phases }: Props = $props();

  function getPhase(name: string): PhaseResult | undefined {
    return phases.find(p => p.name === name);
  }

  function formatDuration(ms: number): string {
    if (ms < 1000) return `${ms}ms`;
    return `${(ms / 1000).toFixed(1)}s`;
  }
</script>

<div class="space-y-1">
  {#each enrichmentPhases as config}
    {@const phase = getPhase(config.name)}
    <div class="flex items-center gap-3 px-3 py-1.5 rounded-md {phase ? 'bg-muted/30' : ''}">
      <!-- Status icon -->
      <div class="shrink-0">
        {#if phase?.status === 'complete'}
          <CheckCircle class="size-4 text-green-500" />
        {:else if phase?.status === 'failed'}
          <XCircle class="size-4 text-red-500" />
        {:else if phase?.status === 'running'}
          <Clock class="size-4 text-blue-500 animate-pulse" />
        {:else if phase?.status === 'skipped'}
          <MinusCircle class="size-4 text-muted-foreground" />
        {:else}
          <div class="size-4 rounded-full border border-border"></div>
        {/if}
      </div>

      <!-- Phase name -->
      <span class="text-xs w-28 {config.color}">{config.label}</span>

      <!-- Duration bar -->
      {#if phase?.duration_ms}
        <div class="flex-1">
          <div class="h-1.5 bg-muted rounded-full overflow-hidden">
            <div
              class="h-full rounded-full {phase.status === 'complete' ? 'bg-green-500/60' : phase.status === 'failed' ? 'bg-red-500/60' : 'bg-blue-500/60'}"
              style="width: {Math.min(100, (phase.duration_ms / 30000) * 100)}%"
            ></div>
          </div>
        </div>
        <span class="text-[10px] text-muted-foreground tabular-nums w-14 text-right">{formatDuration(phase.duration_ms)}</span>
      {:else}
        <div class="flex-1"></div>
        <span class="text-[10px] text-muted-foreground w-14 text-right">-</span>
      {/if}

      <!-- Token cost -->
      {#if phase?.token_usage?.cost}
        <span class="text-[10px] text-muted-foreground tabular-nums w-16 text-right">${phase.token_usage.cost.toFixed(4)}</span>
      {:else}
        <span class="text-[10px] text-muted-foreground w-16 text-right">-</span>
      {/if}
    </div>
  {/each}
</div>
