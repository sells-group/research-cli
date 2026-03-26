<script lang="ts">
	import * as Card from '$lib/components/ui/card';
	import * as Accordion from '$lib/components/ui/accordion';
	import { Switch } from '$lib/components/ui/switch';
	import { Input } from '$lib/components/ui/input';
	import { Button } from '$lib/components/ui/button';
	import { ScrollArea } from '$lib/components/ui/scroll-area';
	import { layerState, activeLayerCount } from '$lib/stores/layers';
	import { LAYER_GROUPS, layersByGroup } from '$lib/config/layer-defs';
	import type { LayerGroup } from '$lib/config/layer-defs';

	interface Props {
		scoreMin: number;
		scoreMax: number;
		stateFilter: string;
		onApply: (filters: { scoreMin: number; scoreMax: number; state: string }) => void;
	}

	let {
		scoreMin = $bindable(0),
		scoreMax = $bindable(100),
		stateFilter = $bindable(''),
		onApply
	}: Props = $props();

	let search = $state('');
	let state: Record<string, boolean> = $state({});

	layerState.subscribe((s) => {
		state = s;
	});

	function filteredLayers(group: LayerGroup) {
		const layers = layersByGroup(group);
		if (!search) return layers;
		const q = search.toLowerCase();
		return layers.filter((l) => l.label.toLowerCase().includes(q));
	}

	function toggleAll(group: LayerGroup, on: boolean) {
		layerState.setGroupVisible(group, on);
	}
</script>

<Card.Root class="flex max-h-[calc(100vh-120px)] w-64 flex-col p-2">
	<div class="space-y-1 px-1 pb-2">
		<div class="flex items-center justify-between">
			<h4 class="text-muted-foreground text-xs font-medium uppercase tracking-wider">Layers</h4>
			<span class="text-muted-foreground text-[10px]">{$activeLayerCount} active</span>
		</div>
		<Input
			type="text"
			bind:value={search}
			class="h-6 text-xs"
			placeholder="Search layers..."
		/>
	</div>
	<ScrollArea class="flex-1">
		<Accordion.Root type="multiple" class="w-full">
			{#each LAYER_GROUPS as group}
				{@const layers = filteredLayers(group)}
				{#if layers.length > 0}
					<Accordion.Item value={group}>
						<Accordion.Trigger class="py-1.5 text-xs font-medium">
							<div class="flex w-full items-center justify-between pr-2">
								<span>{group}</span>
								<span class="text-muted-foreground text-[10px]">
									{layersByGroup(group).filter((l) => state[l.key]).length}/{layersByGroup(group).length}
								</span>
							</div>
						</Accordion.Trigger>
						<Accordion.Content>
							<div class="space-y-0.5 pb-1">
								<div class="mb-1 flex gap-1">
									<button
										class="text-muted-foreground hover:text-foreground text-[10px]"
										onclick={() => toggleAll(group, true)}
									>
										All on
									</button>
									<span class="text-muted-foreground text-[10px]">/</span>
									<button
										class="text-muted-foreground hover:text-foreground text-[10px]"
										onclick={() => toggleAll(group, false)}
									>
										All off
									</button>
								</div>
								{#each layers as layer}
									<div
										class="hover:bg-muted/50 flex items-center justify-between rounded px-1 py-0.5"
									>
										<div class="flex items-center gap-1.5">
											<div
												class="h-2.5 w-2.5 rounded-sm"
												style="background:{layer.color}"
											></div>
											<span class="text-xs">{layer.label}</span>
										</div>
										<Switch
											checked={state[layer.key] ?? false}
											onCheckedChange={(v) => layerState.setVisible(layer.key, v)}
											class="scale-75"
										/>
									</div>
								{/each}
							</div>
						</Accordion.Content>
					</Accordion.Item>
				{/if}
			{/each}
		</Accordion.Root>

		{#if state['companies']}
			<div class="mt-1 space-y-2 border-t px-1 pt-2">
				<h5 class="text-muted-foreground text-[10px] font-medium uppercase">
					Company Filters
				</h5>
				<div class="space-y-1">
					<span class="text-muted-foreground text-[10px]">Score Range</span>
					<div class="flex gap-1">
						<Input
							type="number"
							bind:value={scoreMin}
							min={0}
							max={100}
							class="h-6 text-xs"
							placeholder="Min"
						/>
						<Input
							type="number"
							bind:value={scoreMax}
							min={0}
							max={100}
							class="h-6 text-xs"
							placeholder="Max"
						/>
					</div>
				</div>
				<div class="space-y-1">
					<span class="text-muted-foreground text-[10px]">State</span>
					<Input
						type="text"
						bind:value={stateFilter}
						class="h-6 text-xs"
						placeholder="e.g. CA, TX"
					/>
				</div>
				<Button
					size="sm"
					class="h-6 w-full text-[10px]"
					onclick={() => onApply({ scoreMin, scoreMax, state: stateFilter })}
				>
					Apply
				</Button>
			</div>
		{/if}
	</ScrollArea>
</Card.Root>
