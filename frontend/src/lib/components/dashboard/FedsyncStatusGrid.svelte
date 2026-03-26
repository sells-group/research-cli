<script lang="ts">
  import SyncStatusCell from '$lib/components/shared/SyncStatusCell.svelte';
  import { datasetsByPhase } from '$lib/config/datasets';
  import type { DatasetStatus } from '$lib/types';

  interface Props {
    datasets: DatasetStatus[];
  }

  let { datasets }: Props = $props();

  function getStatus(name: string): DatasetStatus | undefined {
    return datasets.find(d => d.name === name);
  }

  function cellStatus(name: string): string {
    const d = getStatus(name);
    if (!d) return 'idle';
    return d.last_status ?? 'idle';
  }

  const phases = [
    { key: '1', label: 'Phase 1' },
    { key: '1b', label: 'Phase 1B' },
    { key: '2', label: 'Phase 2' },
    { key: '3', label: 'Phase 3' },
  ] as const;
</script>

<div class="space-y-3">
  {#each phases as phase}
    <div>
      <p class="text-xs text-muted-foreground mb-1.5">{phase.label}</p>
      <div class="flex flex-wrap gap-1.5">
        {#each datasetsByPhase[phase.key] ?? [] as ds}
          <SyncStatusCell
            status={cellStatus(ds.name)}
            dataset={ds.label}
            lastSync={getStatus(ds.name)?.last_sync}
          />
        {/each}
      </div>
    </div>
  {/each}
</div>
