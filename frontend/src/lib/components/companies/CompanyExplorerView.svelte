<script lang="ts">
  import { onMount } from 'svelte';
  import { api } from '$lib/api';
  import { companies, selectedCompany, companyFilter, companiesTotal, companiesLoading } from '$lib/stores/companies';
  import CompanyTable from './CompanyTable.svelte';
  import CompanyDetail from './CompanyDetail.svelte';
  import EmptyState from '$lib/components/shared/EmptyState.svelte';
  import LoadingSpinner from '$lib/components/shared/LoadingSpinner.svelte';
  import ErrorAlert from '$lib/components/shared/ErrorAlert.svelte';
  import SearchBar from '$lib/components/shared/SearchBar.svelte';
  import { Input } from '$lib/components/ui/input';
  import Search from 'lucide-svelte/icons/search';

  let error = $state('');
  let page = $state(1);
  const perPage = 50;

  async function loadCompanies() {
    companiesLoading.set(true);
    error = '';
    try {
      const filter = $companyFilter;
      const params: Record<string, string> = {
        limit: String(perPage),
        offset: String((page - 1) * perPage),
        sort: filter.sortBy,
        order: filter.sortOrder,
      };
      if (filter.search) params.search = filter.search;
      if (filter.state) params.state = filter.state;

      const result = await api.listCompanies(params);
      companies.set(result.companies ?? []);
      companiesTotal.set(result.total);
    } catch (e: any) {
      error = e.message;
    } finally {
      companiesLoading.set(false);
    }
  }

  onMount(loadCompanies);

  let searchTimer: ReturnType<typeof setTimeout>;
  function handleSearch(value: string) {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      companyFilter.update(f => ({ ...f, search: value }));
      page = 1;
      loadCompanies();
    }, 300);
  }
</script>

<div class="flex-1 flex overflow-hidden">
  <!-- Left: Company Table -->
  <div class="flex-1 flex flex-col border-r border-border">
    <div class="flex items-center gap-3 px-4 py-2 border-b border-border">
      <div class="relative flex-1 max-w-[300px]">
        <Search class="absolute left-2.5 top-1/2 -translate-y-1/2 size-3.5 text-muted-foreground" />
        <Input
          type="text"
          placeholder="Search companies..."
          class="h-8 text-xs pl-8"
          oninput={(e: Event) => handleSearch((e.target as HTMLInputElement).value)}
        />
      </div>
      <span class="text-xs text-muted-foreground">{$companiesTotal.toLocaleString()} companies</span>
    </div>

    {#if error}
      <ErrorAlert message={error} onRetry={loadCompanies} />
    {:else}
      <CompanyTable
        companies={$companies}
        total={$companiesTotal}
        loading={$companiesLoading}
        selected={$selectedCompany}
        {page}
        {perPage}
        onSelect={(c) => selectedCompany.set(c)}
        onPageChange={(p) => { page = p; loadCompanies(); }}
      />
    {/if}
  </div>

  <!-- Right: Company Detail -->
  <div class="w-[500px] overflow-auto">
    {#if $selectedCompany}
      <CompanyDetail company={$selectedCompany} />
    {:else}
      <EmptyState title="Select a company" description="Click on a company to view details" />
    {/if}
  </div>
</div>
