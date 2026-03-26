<script lang="ts">
  import * as Alert from '$lib/components/ui/alert';
  import { Button } from '$lib/components/ui/button';
  import AlertCircle from 'lucide-svelte/icons/circle-alert';

  interface Props {
    message: string;
    onRetry?: () => void;
  }

  let { message, onRetry }: Props = $props();
</script>

<Alert.Root variant="destructive" class="m-4">
  <AlertCircle class="size-4" />
  <Alert.Title>Error</Alert.Title>
  <Alert.Description class="flex items-center justify-between">
    <span>{message}</span>
    {#if onRetry}
      <Button variant="outline" size="sm" class="text-xs" onclick={onRetry}>Retry</Button>
    {/if}
  </Alert.Description>
</Alert.Root>
