-- +goose Up
ALTER TABLE public.runs ADD COLUMN IF NOT EXISTS error jsonb NULL;

-- +goose Down
ALTER TABLE public.runs DROP COLUMN IF EXISTS error;
