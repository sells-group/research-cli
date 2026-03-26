<script lang="ts">
  import Inbox from 'lucide-svelte/icons/inbox';

  interface Props {
    title?: string;
    description?: string;
    icon?: any;
  }

  let { title = 'No data', description = 'Nothing to display yet.', icon: Icon = Inbox }: Props = $props();
</script>

<div class="flex flex-col items-center justify-center gap-3 p-12 text-muted-foreground">
  <Icon class="size-10 opacity-40" />
  <div class="text-center">
    <p class="text-sm font-medium">{title}</p>
    <p class="text-xs mt-1">{description}</p>
  </div>
</div>
