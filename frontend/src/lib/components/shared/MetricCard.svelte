<script lang="ts">
  import * as Card from '$lib/components/ui/card';

  interface Props {
    label: string;
    value: string | number;
    trend?: string;
    trendUp?: boolean;
    icon?: any;
  }

  let { label, value, trend, trendUp, icon: Icon }: Props = $props();
</script>

<Card.Root class="p-4">
  <div class="flex items-center justify-between">
    <p class="text-xs text-muted-foreground">{label}</p>
    {#if Icon}
      <Icon class="size-4 text-muted-foreground" />
    {/if}
  </div>
  <p class="text-2xl font-bold mt-1">{value}</p>
  {#if trend}
    <p class="text-xs mt-1 {trendUp ? 'text-green-500' : 'text-red-500'}">{trend}</p>
  {/if}
</Card.Root>
