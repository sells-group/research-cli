<script lang="ts">
  import type { TableMeta } from '$lib/types';

  interface Props {
    tables: TableMeta[];
    selected: string;
    onSelect: (table: string) => void;
  }

  let { tables, selected, onSelect }: Props = $props();

  // Dynamically group tables by their category from the API
  const groups = $derived.by(() => {
    const map = new Map<string, TableMeta[]>();
    for (const t of tables) {
      const cat = t.category || 'Other';
      if (!map.has(cat)) map.set(cat, []);
      map.get(cat)!.push(t);
    }
    // Sort categories: known ones first in a sensible order, then alphabetical
    const order = ['Census', 'BLS', 'BEA', 'SEC', 'FINRA', 'Contracts', 'SBA', 'DOL', 'IRS', 'OSHA', 'EPA', 'FDIC', 'NCUA', 'FRED', 'System', 'Other'];
    const sorted = [...map.entries()].sort((a, b) => {
      const ai = order.indexOf(a[0]);
      const bi = order.indexOf(b[0]);
      if (ai >= 0 && bi >= 0) return ai - bi;
      if (ai >= 0) return -1;
      if (bi >= 0) return 1;
      return a[0].localeCompare(b[0]);
    });
    return sorted;
  });
</script>

<select
  class="h-8 text-xs bg-background border border-input rounded-md px-2 min-w-[220px]"
  value={selected}
  onchange={(e: Event) => onSelect((e.target as HTMLSelectElement).value)}
>
  {#each groups as [category, categoryTables]}
    <optgroup label={category}>
      {#each categoryTables as table}
        <option value={table.id}>
          {table.name} (~{table.estimated_row_count?.toLocaleString() ?? '?'})
        </option>
      {/each}
    </optgroup>
  {/each}
</select>
