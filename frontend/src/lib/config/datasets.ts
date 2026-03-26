import { datasetsMeta as generatedDatasetsMeta } from "./datasets.generated";

export type DatasetMeta = (typeof generatedDatasetsMeta)[number];

export const datasetsMeta: DatasetMeta[] = [...generatedDatasetsMeta];

export const datasetsByPhase = {
  "1": datasetsMeta.filter((dataset) => dataset.phase === "1"),
  "1b": datasetsMeta.filter((dataset) => dataset.phase === "1b"),
  "2": datasetsMeta.filter((dataset) => dataset.phase === "2"),
  "3": datasetsMeta.filter((dataset) => dataset.phase === "3"),
};

export function getDatasetMeta(name: string): DatasetMeta | undefined {
  return datasetsMeta.find((dataset) => dataset.name === name);
}
