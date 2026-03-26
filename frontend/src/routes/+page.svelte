<script lang="ts">
  import { onMount } from 'svelte';
  import DashboardView from '$lib/components/dashboard/DashboardView.svelte';
  import EnrichmentView from '$lib/components/enrichment/EnrichmentView.svelte';
  import CompanyExplorerView from '$lib/components/companies/CompanyExplorerView.svelte';
  import FedDataView from '$lib/components/fed-data/FedDataView.svelte';
  import MapView from '$lib/components/map/MapView.svelte';
  import AnalyticsView from '$lib/components/analytics/AnalyticsView.svelte';
  import { activeView, type ActiveView } from '$lib/stores/view';
  import { toggleMode } from 'mode-watcher';
  import * as ToggleGroup from '$lib/components/ui/toggle-group';
  import { Button } from '$lib/components/ui/button';
  import Sun from 'lucide-svelte/icons/sun';
  import Moon from 'lucide-svelte/icons/moon';
  import LayoutDashboard from 'lucide-svelte/icons/layout-dashboard';
  import Workflow from 'lucide-svelte/icons/workflow';
  import Building2 from 'lucide-svelte/icons/building-2';
  import Database from 'lucide-svelte/icons/database';
  import MapIcon from 'lucide-svelte/icons/map';
  import ChartBar from 'lucide-svelte/icons/chart-bar';

  onMount(() => {
    activeView.init();
  });
</script>

<div class="flex flex-col h-screen w-screen">
  <header class="h-12 bg-card border-b border-border flex items-center px-4 gap-3 z-10 shrink-0">
    <h1 class="text-sm font-semibold whitespace-nowrap">Research Platform</h1>

    <ToggleGroup.Root type="single" value={$activeView} onValueChange={(v) => { if (v) activeView.set(v as ActiveView); }} variant="outline" size="sm" class="shrink-0">
      <ToggleGroup.Item value="dashboard" class="text-xs gap-1 h-7 px-2.5">
        <LayoutDashboard class="size-3.5" />
        Dashboard
      </ToggleGroup.Item>
      <ToggleGroup.Item value="enrichment" class="text-xs gap-1 h-7 px-2.5">
        <Workflow class="size-3.5" />
        Enrichment
      </ToggleGroup.Item>
      <ToggleGroup.Item value="companies" class="text-xs gap-1 h-7 px-2.5">
        <Building2 class="size-3.5" />
        Companies
      </ToggleGroup.Item>
      <ToggleGroup.Item value="fed-data" class="text-xs gap-1 h-7 px-2.5">
        <Database class="size-3.5" />
        Fed Data
      </ToggleGroup.Item>
      <ToggleGroup.Item value="map" class="text-xs gap-1 h-7 px-2.5">
        <MapIcon class="size-3.5" />
        Map
      </ToggleGroup.Item>
      <ToggleGroup.Item value="analytics" class="text-xs gap-1 h-7 px-2.5">
        <ChartBar class="size-3.5" />
        Analytics
      </ToggleGroup.Item>
    </ToggleGroup.Root>

    <div class="flex-1"></div>

    <Button variant="ghost" size="icon" class="h-8 w-8" onclick={toggleMode}>
      <Sun class="size-4 rotate-0 scale-100 transition-all dark:-rotate-90 dark:scale-0" />
      <Moon class="absolute size-4 rotate-90 scale-0 transition-all dark:rotate-0 dark:scale-100" />
      <span class="sr-only">Toggle theme</span>
    </Button>
  </header>

  {#if $activeView === 'dashboard'}
    <DashboardView />
  {:else if $activeView === 'enrichment'}
    <EnrichmentView />
  {:else if $activeView === 'companies'}
    <CompanyExplorerView />
  {:else if $activeView === 'fed-data'}
    <FedDataView />
  {:else if $activeView === 'map'}
    <MapView />
  {:else if $activeView === 'analytics'}
    <AnalyticsView />
  {/if}
</div>
