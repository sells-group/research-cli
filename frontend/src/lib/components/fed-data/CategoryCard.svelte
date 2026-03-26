<script lang="ts">
  import * as Card from '$lib/components/ui/card';
  import type { CategoryMeta } from '$lib/config/categories';
  import { formatRowCount } from '$lib/utils/table-helpers';

  interface Props {
    category: CategoryMeta;
    datasetCount: number;
    totalRows: number;
    selected: boolean;
    onclick: () => void;
  }

  let { category, datasetCount, totalRows, selected, onclick }: Props = $props();
  const Icon = $derived(category.icon);
</script>

<button class="text-left w-full" {onclick}>
  <Card.Root class="p-3 transition-all hover:ring-2 hover:ring-primary/30 cursor-pointer {selected ? 'ring-2 ring-primary bg-accent' : ''}">
    <div class="flex items-start gap-3">
      <div class="rounded-md p-2 bg-{category.color}-500/10 text-{category.color}-500 shrink-0">
        <Icon class="size-4" />
      </div>
      <div class="min-w-0">
        <p class="text-sm font-medium truncate">{category.label}</p>
        <p class="text-xs text-muted-foreground mt-0.5 line-clamp-1">{category.description}</p>
        <div class="flex items-center gap-3 mt-1.5">
          <span class="text-xs text-muted-foreground">{datasetCount} datasets</span>
          <span class="text-xs text-muted-foreground">{formatRowCount(totalRows)} rows</span>
        </div>
      </div>
    </div>
  </Card.Root>
</button>
