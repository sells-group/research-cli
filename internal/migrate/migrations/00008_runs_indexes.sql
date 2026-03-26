-- +goose NO TRANSACTION
-- +goose Up
-- Add missing indexes on runs table used by dashboard queries.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_created_at ON public.runs (created_at DESC);
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_runs_company_url ON public.runs ((company->>'url'));

-- +goose Down
DROP INDEX CONCURRENTLY IF EXISTS public.idx_runs_company_url;
DROP INDEX CONCURRENTLY IF EXISTS public.idx_runs_created_at;
