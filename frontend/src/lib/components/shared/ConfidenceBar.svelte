<script lang="ts">
  interface Props {
    value: number;
    showLabel?: boolean;
  }

  let { value, showLabel = true }: Props = $props();

  function getColor(v: number): string {
    if (v >= 0.8) return 'bg-green-500';
    if (v >= 0.6) return 'bg-yellow-500';
    if (v >= 0.4) return 'bg-orange-500';
    return 'bg-red-500';
  }
</script>

<div class="flex items-center gap-2">
  <div class="flex-1 h-2 bg-muted rounded-full overflow-hidden">
    <div class="h-full rounded-full transition-all {getColor(value)}" style="width: {Math.round(value * 100)}%"></div>
  </div>
  {#if showLabel}
    <span class="text-xs text-muted-foreground tabular-nums w-10 text-right">{(value * 100).toFixed(0)}%</span>
  {/if}
</div>
