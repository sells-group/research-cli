<script lang="ts">
  import { onMount } from 'svelte';
  import { api } from '$lib/api';
  import * as Card from '$lib/components/ui/card';
  import * as Tabs from '$lib/components/ui/tabs';
  import { Badge } from '$lib/components/ui/badge';
  import IdentifierBadge from '$lib/components/shared/IdentifierBadge.svelte';
  import ConfidenceBar from '$lib/components/shared/ConfidenceBar.svelte';
  import StatusBadge from '$lib/components/shared/StatusBadge.svelte';
  import type { CompanyRecord, Identifier, Address, Match, AddressMSA, Run } from '$lib/types';

  interface Props {
    company: CompanyRecord;
  }

  let { company }: Props = $props();

  let identifiers = $state<Identifier[]>([]);
  let addresses = $state<Address[]>([]);
  let matches = $state<Match[]>([]);
  let msas = $state<AddressMSA[]>([]);
  let runs = $state<Run[]>([]);

  async function loadDetails() {
    const id = company.id;
    const [idRes, addrRes, matchRes, msaRes, runRes] = await Promise.all([
      api.getCompanyIdentifiers(id).catch(() => ({ identifiers: [] })),
      api.getCompanyAddresses(id).catch(() => ({ addresses: [] })),
      api.getCompanyMatches(id).catch(() => ({ matches: [] })),
      api.getCompanyMSAs(id).catch(() => ({ msas: [] })),
      api.getCompanyRuns(id).catch(() => ({ runs: [] })),
    ]);
    identifiers = idRes.identifiers ?? [];
    addresses = addrRes.addresses ?? [];
    matches = matchRes.matches ?? [];
    msas = msaRes.msas ?? [];
    runs = runRes.runs ?? [];
  }

  $effect(() => {
    if (company?.id) loadDetails();
  });
</script>

<div class="p-4 space-y-4">
  <div>
    <h2 class="text-lg font-semibold">{company.name ?? 'Unknown'}</h2>
    {#if company.domain}
      <p class="text-xs text-muted-foreground">{company.domain}</p>
    {/if}
    {#if company.enrichment_score != null}
      <div class="w-32 mt-2">
        <ConfidenceBar value={company.enrichment_score} />
      </div>
    {/if}
  </div>

  <Tabs.Root value="overview">
    <Tabs.List>
      <Tabs.Trigger value="overview" class="text-xs">Overview</Tabs.Trigger>
      <Tabs.Trigger value="identifiers" class="text-xs">Identifiers ({identifiers.length})</Tabs.Trigger>
      <Tabs.Trigger value="addresses" class="text-xs">Addresses ({addresses.length})</Tabs.Trigger>
      <Tabs.Trigger value="federal" class="text-xs">Federal ({matches.length})</Tabs.Trigger>
      <Tabs.Trigger value="runs" class="text-xs">Runs ({runs.length})</Tabs.Trigger>
    </Tabs.List>

    <Tabs.Content value="overview" class="mt-3 space-y-3">
      <div class="grid grid-cols-2 gap-3">
        {#each [
          ['Legal Name', company.legal_name],
          ['Business Model', company.business_model],
          ['NAICS', company.naics_code],
          ['SIC', company.sic_code],
          ['Employees', company.employee_count ?? company.employee_estimate],
          ['Revenue', company.revenue_estimate ?? company.revenue_range],
          ['Founded', company.year_founded],
          ['Ownership', company.ownership_type],
          ['Phone', company.phone],
          ['Email', company.email],
          ['Location', [company.city, company.state].filter(Boolean).join(', ')],
        ] as [label, value]}
          {#if value}
            <div>
              <p class="text-[10px] text-muted-foreground uppercase">{label}</p>
              <p class="text-xs">{value}</p>
            </div>
          {/if}
        {/each}
      </div>
      {#if company.description}
        <div>
          <p class="text-[10px] text-muted-foreground uppercase">Description</p>
          <p class="text-xs mt-0.5">{company.description}</p>
        </div>
      {/if}
    </Tabs.Content>

    <Tabs.Content value="identifiers" class="mt-3">
      <div class="flex flex-wrap gap-2">
        {#each identifiers as id}
          <IdentifierBadge system={id.system} identifier={id.identifier} />
        {:else}
          <p class="text-xs text-muted-foreground">No identifiers</p>
        {/each}
      </div>
    </Tabs.Content>

    <Tabs.Content value="addresses" class="mt-3 space-y-2">
      {#each addresses as addr}
        <Card.Root class="p-3">
          <div class="flex items-center gap-2">
            <Badge variant="outline" class="text-[10px]">{addr.address_type}</Badge>
            {#if addr.is_primary}<Badge class="text-[10px]">Primary</Badge>{/if}
          </div>
          <p class="text-xs mt-1">{[addr.street, addr.city, addr.state, addr.zip_code].filter(Boolean).join(', ')}</p>
          {#if addr.latitude && addr.longitude}
            <p class="text-[10px] text-muted-foreground mt-0.5">{addr.latitude.toFixed(5)}, {addr.longitude.toFixed(5)}</p>
          {/if}
        </Card.Root>
      {:else}
        <p class="text-xs text-muted-foreground">No addresses</p>
      {/each}

      {#if msas.length}
        <h3 class="text-xs font-medium mt-3">MSA Associations</h3>
        {#each msas as msa}
          <div class="text-xs px-3 py-1.5 rounded bg-muted/50">
            <span class="font-medium">{msa.msa_name}</span>
            <span class="text-muted-foreground ml-2">{msa.classification} - {msa.cbsa_code}</span>
          </div>
        {/each}
      {/if}
    </Tabs.Content>

    <Tabs.Content value="federal" class="mt-3 space-y-2">
      {#each matches as match}
        <div class="flex items-center gap-3 px-3 py-2 rounded bg-muted/50">
          <Badge variant="outline" class="text-[10px]">{match.matched_source}</Badge>
          <span class="text-xs font-mono flex-1 truncate">{match.matched_key}</span>
          <span class="text-[10px] text-muted-foreground">{match.match_type}</span>
          <span class="text-xs tabular-nums">{(match.confidence * 100).toFixed(0)}%</span>
        </div>
      {:else}
        <p class="text-xs text-muted-foreground">No federal data matches</p>
      {/each}
    </Tabs.Content>

    <Tabs.Content value="runs" class="mt-3 space-y-2">
      {#each runs as run}
        <div class="flex items-center gap-3 px-3 py-2 rounded bg-muted/50">
          <StatusBadge status={run.status} size="sm" />
          <span class="text-xs flex-1">{new Date(run.created_at).toLocaleDateString()}</span>
          {#if run.result?.score != null}
            <span class="text-xs tabular-nums">{(run.result.score * 100).toFixed(0)}%</span>
          {/if}
        </div>
      {:else}
        <p class="text-xs text-muted-foreground">No enrichment runs</p>
      {/each}
    </Tabs.Content>
  </Tabs.Root>
</div>
