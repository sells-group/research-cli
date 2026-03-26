<script lang="ts">
  import LoaderCircle from 'lucide-svelte/icons/loader-circle';

  interface Props {
    size?: string;
    label?: string;
  }

  let { size = 'size-6', label = 'Loading...' }: Props = $props();
</script>

<div class="flex items-center justify-center gap-2 p-8 text-muted-foreground">
  <LoaderCircle class="{size} animate-spin" />
  {#if label}
    <span class="text-sm">{label}</span>
  {/if}
</div>
