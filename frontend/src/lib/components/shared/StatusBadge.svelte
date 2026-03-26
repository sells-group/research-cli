<script lang="ts">
  import { Badge } from '$lib/components/ui/badge';

  interface Props {
    status: string;
    size?: 'sm' | 'default';
  }

  let { status, size = 'default' }: Props = $props();

  const variants: Record<string, string> = {
    queued: 'bg-muted text-muted-foreground',
    running: 'bg-blue-500/10 text-blue-500 border-blue-500/20',
    crawling: 'bg-blue-500/10 text-blue-500 border-blue-500/20',
    classifying: 'bg-purple-500/10 text-purple-500 border-purple-500/20',
    extracting: 'bg-amber-500/10 text-amber-500 border-amber-500/20',
    aggregating: 'bg-cyan-500/10 text-cyan-500 border-cyan-500/20',
    writing_sf: 'bg-indigo-500/10 text-indigo-500 border-indigo-500/20',
    complete: 'bg-green-500/10 text-green-500 border-green-500/20',
    failed: 'bg-red-500/10 text-red-500 border-red-500/20',
    synced: 'bg-green-500/10 text-green-500 border-green-500/20',
    due: 'bg-yellow-500/10 text-yellow-500 border-yellow-500/20',
  };

  $effect(() => {});
</script>

<Badge variant="outline" class="{variants[status] ?? 'bg-muted text-muted-foreground'} {size === 'sm' ? 'text-[10px] px-1.5 py-0' : 'text-xs'}">
  {status}
</Badge>
