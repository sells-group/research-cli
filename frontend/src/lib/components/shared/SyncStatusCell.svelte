<script lang="ts">
  import * as Tooltip from '$lib/components/ui/tooltip';

  interface Props {
    status: string;
    dataset: string;
    lastSync?: string | null;
  }

  let { status, dataset, lastSync }: Props = $props();

  const colors: Record<string, string> = {
    complete: 'bg-green-500',
    synced: 'bg-green-500',
    running: 'bg-blue-500 animate-pulse',
    failed: 'bg-red-500',
    due: 'bg-yellow-500',
    idle: 'bg-muted-foreground/30',
  };
</script>

<Tooltip.Root>
  <Tooltip.Trigger>
    <div class="size-6 rounded {colors[status] ?? colors.idle} cursor-default"></div>
  </Tooltip.Trigger>
  <Tooltip.Content>
    <p class="text-xs font-medium">{dataset}</p>
    <p class="text-xs text-muted-foreground">{status}{lastSync ? ` - ${new Date(lastSync).toLocaleDateString()}` : ''}</p>
  </Tooltip.Content>
</Tooltip.Root>
