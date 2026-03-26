<script lang="ts">
  import { api } from '$lib/api';
  import { selectCompany } from '$lib/stores/selection';
  import * as Popover from '$lib/components/ui/popover';
  import { Input } from '$lib/components/ui/input';
  import { Badge } from '$lib/components/ui/badge';
  import { ScrollArea } from '$lib/components/ui/scroll-area';
  import Search from 'lucide-svelte/icons/search';
  import type { CompanyRecord } from '$lib/types';

  let query = $state('');
  let results = $state<CompanyRecord[]>([]);
  let open = $state(false);
  let searchTimer: ReturnType<typeof setTimeout>;

  async function handleSearch() {
    if (query.length < 2) {
      results = [];
      open = false;
      return;
    }
    try {
      const data = await api.searchCompanies({ q: query, limit: '10' });
      results = data.companies ?? [];
      open = results.length > 0;
    } catch {
      results = [];
    }
  }

  function handleInput() {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(handleSearch, 300);
  }

  function select(company: CompanyRecord) {
    open = false;
    query = company.name ?? company.domain ?? '';
    selectCompany(company);
  }
</script>

<div class="relative flex-1 max-w-[400px]">
  <Popover.Root bind:open>
    <Popover.Trigger class="w-full">
      <div class="relative">
        <Search class="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground" />
        <Input
          type="text"
          placeholder="Search companies..."
          class="h-8 text-xs pl-8"
          bind:value={query}
          oninput={handleInput}
          onfocus={() => open = results.length > 0}
        />
      </div>
    </Popover.Trigger>
    <Popover.Content class="w-[400px] p-0" align="start" sideOffset={4}>
      <ScrollArea class="max-h-[300px]">
        {#each results as company}
          <button
            class="w-full text-left px-3 py-2 text-xs hover:bg-accent flex items-center gap-2 transition-colors"
            onclick={() => select(company)}
          >
            <span class="truncate font-medium">{company.name ?? 'Unknown'}</span>
            {#if company.domain}
              <Badge variant="outline" class="text-[10px] shrink-0">{company.domain}</Badge>
            {/if}
            {#if company.state}
              <span class="text-muted-foreground shrink-0">{company.state}</span>
            {/if}
          </button>
        {/each}
      </ScrollArea>
    </Popover.Content>
  </Popover.Root>
</div>
