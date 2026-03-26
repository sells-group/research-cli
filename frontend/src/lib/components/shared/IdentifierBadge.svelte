<script lang="ts">
  import { Badge } from '$lib/components/ui/badge';

  interface Props {
    system: string;
    identifier: string;
  }

  let { system, identifier }: Props = $props();

  const colors: Record<string, string> = {
    crd: 'bg-blue-500/10 text-blue-500 border-blue-500/20',
    cik: 'bg-purple-500/10 text-purple-500 border-purple-500/20',
    ein: 'bg-green-500/10 text-green-500 border-green-500/20',
    duns: 'bg-orange-500/10 text-orange-500 border-orange-500/20',
    uei: 'bg-cyan-500/10 text-cyan-500 border-cyan-500/20',
    salesforce: 'bg-indigo-500/10 text-indigo-500 border-indigo-500/20',
    fdic: 'bg-amber-500/10 text-amber-500 border-amber-500/20',
  };
</script>

<Badge variant="outline" class="text-[10px] gap-1 {colors[system] ?? 'bg-muted text-muted-foreground'}">
  <span class="font-semibold uppercase">{system}</span>
  <span class="font-mono">{identifier}</span>
</Badge>
