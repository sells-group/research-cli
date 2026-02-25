CREATE TABLE public.discovery_runs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    strategy        VARCHAR(20) NOT NULL,  -- 'ppp' or 'organic'
    status          VARCHAR(20) NOT NULL DEFAULT 'running',
    config          JSONB NOT NULL,
    candidates_found    INTEGER DEFAULT 0,
    candidates_qualified INTEGER DEFAULT 0,
    cost_usd        NUMERIC(10,4) DEFAULT 0,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at    TIMESTAMPTZ,
    error           TEXT
);

CREATE TABLE public.discovery_candidates (
    id              BIGSERIAL PRIMARY KEY,
    run_id          UUID NOT NULL REFERENCES public.discovery_runs(id),
    google_place_id VARCHAR(200),
    name            TEXT NOT NULL,
    domain          VARCHAR(255),
    website         VARCHAR(500),
    street          VARCHAR(300),
    city            VARCHAR(100),
    state           VARCHAR(10),
    zip_code        VARCHAR(20),
    naics_code      VARCHAR(6),
    source          VARCHAR(20) NOT NULL,  -- 'ppp', 'organic'
    source_record   JSONB,
    disqualified    BOOLEAN DEFAULT FALSE,
    disqualify_reason VARCHAR(100),
    score_t0        NUMERIC(4,2),
    score_t1        NUMERIC(4,2),
    score_t2        NUMERIC(4,2),
    promoted_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (run_id, google_place_id)
);

CREATE INDEX idx_disc_cand_run ON public.discovery_candidates (run_id, disqualified);
CREATE INDEX idx_disc_cand_domain ON public.discovery_candidates (domain) WHERE domain IS NOT NULL;
CREATE INDEX idx_disc_cand_place ON public.discovery_candidates (google_place_id) WHERE google_place_id IS NOT NULL;
