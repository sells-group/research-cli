<script lang="ts">
  import ChevronRight from 'lucide-svelte/icons/chevron-right';

  interface Crumb {
    label: string;
    onclick?: (() => void) | undefined;
  }

  interface Props {
    crumbs: Crumb[];
  }

  let { crumbs }: Props = $props();
</script>

<nav class="flex items-center gap-1 text-sm">
  {#each crumbs as crumb, i}
    {#if i > 0}
      <ChevronRight class="size-3.5 text-muted-foreground shrink-0" />
    {/if}
    {#if crumb.onclick && i < crumbs.length - 1}
      <button
        class="text-muted-foreground hover:text-foreground transition-colors"
        onclick={crumb.onclick}
      >
        {crumb.label}
      </button>
    {:else}
      <span class="font-medium truncate">{crumb.label}</span>
    {/if}
  {/each}
</nav>
