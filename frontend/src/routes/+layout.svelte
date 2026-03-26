<script>
  import '../app.css';
  import { ModeWatcher } from 'mode-watcher';
  import { TooltipProvider } from '$lib/components/ui/tooltip';
  let { children } = $props();
</script>

<ModeWatcher defaultMode="dark" />
<TooltipProvider delayDuration={200} skipDelayDuration={100}>
  {@render children()}
</TooltipProvider>
