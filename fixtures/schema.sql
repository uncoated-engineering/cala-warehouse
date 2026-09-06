-- DDL for the tables cala-warehouse extracts, dumped by fixtures/schema.sh
-- from cala-ledger's migrations (cala @ a6f9da11, pg_dump 18.6).
-- Do not edit: regenerate with ./fixtures/schema.sh.
CREATE TYPE public.debitorcredit AS ENUM (
    'debit',
    'credit'
);

CREATE TYPE public.inboxeventstatus AS ENUM (
    'pending',
    'processing',
    'completed',
    'failed'
);

CREATE TYPE public.jobexecutionstate AS ENUM (
    'pending',
    'parked',
    'running'
);

CREATE TYPE public.layer AS ENUM (
    'settled',
    'pending',
    'encumbrance'
);

CREATE TYPE public.status AS ENUM (
    'active',
    'locked'
);


CREATE TABLE public.cala_account_events (
    id uuid NOT NULL,
    sequence integer NOT NULL,
    event_type character varying NOT NULL,
    event jsonb NOT NULL,
    context jsonb,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_account_set_events (
    id uuid NOT NULL,
    sequence integer NOT NULL,
    event_type character varying NOT NULL,
    event jsonb NOT NULL,
    context jsonb,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_account_set_member_account_sets (
    account_set_id uuid NOT NULL,
    member_account_set_id uuid CONSTRAINT cala_account_set_member_account__member_account_set_id_not_null NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_account_set_member_accounts (
    account_set_id uuid NOT NULL,
    member_account_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_account_sets (
    id uuid NOT NULL,
    journal_id uuid NOT NULL,
    name character varying NOT NULL,
    external_id character varying,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_accounts (
    id uuid NOT NULL,
    code character varying NOT NULL,
    name character varying NOT NULL,
    external_id character varying,
    normal_balance_type public.debitorcredit NOT NULL,
    eventually_consistent boolean NOT NULL,
    is_account_set boolean NOT NULL,
    velocity_context_values jsonb NOT NULL,
    status public.status NOT NULL,
    created_at timestamp with time zone NOT NULL
);

CREATE TABLE public.cala_balance_history (
    journal_id uuid NOT NULL,
    account_id uuid NOT NULL,
    latest_entry_id uuid NOT NULL,
    currency character varying NOT NULL,
    version integer NOT NULL,
    "values" jsonb NOT NULL,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_current_balances (
    journal_id uuid NOT NULL,
    account_id uuid NOT NULL,
    currency character varying NOT NULL,
    latest_version integer NOT NULL,
    latest_values jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
)
WITH (fillfactor='70');

CREATE TABLE public.cala_entries (
    id uuid NOT NULL,
    journal_id uuid NOT NULL,
    account_id uuid NOT NULL,
    transaction_id uuid NOT NULL,
    account_is_account_set boolean GENERATED ALWAYS AS (false) STORED NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_entry_events (
    id uuid NOT NULL,
    sequence integer NOT NULL,
    event_type character varying NOT NULL,
    event jsonb NOT NULL,
    context jsonb,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_journal_events (
    id uuid NOT NULL,
    sequence integer NOT NULL,
    event_type character varying NOT NULL,
    event jsonb NOT NULL,
    context jsonb,
    recorded_at timestamp with time zone NOT NULL
);

CREATE TABLE public.cala_journals (
    id uuid NOT NULL,
    name character varying NOT NULL,
    code character varying,
    created_at timestamp with time zone NOT NULL
);

CREATE TABLE public.cala_persistent_outbox_events (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    sequence bigint NOT NULL,
    payload jsonb,
    tracing_context jsonb,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
)
PARTITION BY RANGE (sequence);

CREATE SEQUENCE public.cala_persistent_outbox_events_sequence_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;

ALTER SEQUENCE public.cala_persistent_outbox_events_sequence_seq OWNED BY public.cala_persistent_outbox_events.sequence;

CREATE TABLE public.cala_persistent_outbox_events_default (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cala_persistent_outbox_events_id_not_null NOT NULL,
    sequence bigint DEFAULT nextval('public.cala_persistent_outbox_events_sequence_seq'::regclass) CONSTRAINT cala_persistent_outbox_events_sequence_not_null NOT NULL,
    payload jsonb,
    tracing_context jsonb,
    recorded_at timestamp with time zone DEFAULT now() CONSTRAINT cala_persistent_outbox_events_recorded_at_not_null NOT NULL
);

CREATE TABLE public.cala_persistent_outbox_events_p0 (
    id uuid DEFAULT gen_random_uuid() CONSTRAINT cala_persistent_outbox_events_id_not_null NOT NULL,
    sequence bigint DEFAULT nextval('public.cala_persistent_outbox_events_sequence_seq'::regclass) CONSTRAINT cala_persistent_outbox_events_sequence_not_null NOT NULL,
    payload jsonb,
    tracing_context jsonb,
    recorded_at timestamp with time zone DEFAULT now() CONSTRAINT cala_persistent_outbox_events_recorded_at_not_null NOT NULL
)
WITH (autovacuum_vacuum_insert_scale_factor='0.0', autovacuum_vacuum_insert_threshold='50000', autovacuum_freeze_min_age='0', fillfactor='100');

CREATE TABLE public.cala_transaction_events (
    id uuid NOT NULL,
    sequence integer NOT NULL,
    event_type character varying NOT NULL,
    event jsonb NOT NULL,
    context jsonb,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_transactions (
    id uuid NOT NULL,
    journal_id uuid NOT NULL,
    tx_template_id uuid NOT NULL,
    external_id character varying,
    correlation_id character varying NOT NULL,
    effective date NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_tx_template_events (
    id uuid NOT NULL,
    sequence integer NOT NULL,
    event_type character varying NOT NULL,
    event jsonb NOT NULL,
    context jsonb,
    recorded_at timestamp with time zone DEFAULT now() NOT NULL
);

CREATE TABLE public.cala_tx_templates (
    id uuid NOT NULL,
    code character varying NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE ONLY public.cala_persistent_outbox_events ATTACH PARTITION public.cala_persistent_outbox_events_default DEFAULT;

ALTER TABLE ONLY public.cala_persistent_outbox_events ATTACH PARTITION public.cala_persistent_outbox_events_p0 FOR VALUES FROM ('0') TO ('2000000');

ALTER TABLE ONLY public.cala_persistent_outbox_events ALTER COLUMN sequence SET DEFAULT nextval('public.cala_persistent_outbox_events_sequence_seq'::regclass);

ALTER TABLE ONLY public.cala_account_events
    ADD CONSTRAINT cala_account_events_id_sequence_key UNIQUE (id, sequence);

ALTER TABLE ONLY public.cala_account_set_events
    ADD CONSTRAINT cala_account_set_events_id_sequence_key UNIQUE (id, sequence);

ALTER TABLE ONLY public.cala_account_set_member_account_sets
    ADD CONSTRAINT cala_account_set_member_accou_account_set_id_member_account_key UNIQUE (account_set_id, member_account_set_id);

ALTER TABLE ONLY public.cala_account_set_member_accounts
    ADD CONSTRAINT cala_account_set_member_accou_member_account_id_account_set_key UNIQUE (member_account_id, account_set_id);

ALTER TABLE ONLY public.cala_account_sets
    ADD CONSTRAINT cala_account_sets_external_id_key UNIQUE (external_id);

ALTER TABLE ONLY public.cala_account_sets
    ADD CONSTRAINT cala_account_sets_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.cala_accounts
    ADD CONSTRAINT cala_accounts_code_key UNIQUE (code);

ALTER TABLE ONLY public.cala_accounts
    ADD CONSTRAINT cala_accounts_external_id_key UNIQUE (external_id);

ALTER TABLE ONLY public.cala_accounts
    ADD CONSTRAINT cala_accounts_id_is_account_set_key UNIQUE (id, is_account_set);

ALTER TABLE ONLY public.cala_accounts
    ADD CONSTRAINT cala_accounts_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.cala_balance_history
    ADD CONSTRAINT cala_balance_history_account_id_journal_id_currency_version_key UNIQUE (account_id, journal_id, currency, version);

ALTER TABLE ONLY public.cala_current_balances
    ADD CONSTRAINT cala_current_balances_account_id_journal_id_currency_key UNIQUE (account_id, journal_id, currency);

ALTER TABLE ONLY public.cala_entries
    ADD CONSTRAINT cala_entries_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.cala_entry_events
    ADD CONSTRAINT cala_entry_events_id_sequence_key UNIQUE (id, sequence);

ALTER TABLE ONLY public.cala_journal_events
    ADD CONSTRAINT cala_journal_events_id_sequence_key UNIQUE (id, sequence);

ALTER TABLE ONLY public.cala_journals
    ADD CONSTRAINT cala_journals_code_key UNIQUE (code);

ALTER TABLE ONLY public.cala_journals
    ADD CONSTRAINT cala_journals_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.cala_persistent_outbox_events
    ADD CONSTRAINT cala_persistent_outbox_events_pkey PRIMARY KEY (sequence);

ALTER TABLE ONLY public.cala_persistent_outbox_events_default
    ADD CONSTRAINT cala_persistent_outbox_events_default_pkey PRIMARY KEY (sequence);

ALTER TABLE ONLY public.cala_persistent_outbox_events_p0
    ADD CONSTRAINT cala_persistent_outbox_events_p0_pkey PRIMARY KEY (sequence);

ALTER TABLE ONLY public.cala_transaction_events
    ADD CONSTRAINT cala_transaction_events_id_sequence_key UNIQUE (id, sequence);

ALTER TABLE ONLY public.cala_transactions
    ADD CONSTRAINT cala_transactions_external_id_key UNIQUE (external_id);

ALTER TABLE ONLY public.cala_transactions
    ADD CONSTRAINT cala_transactions_pkey PRIMARY KEY (id);

ALTER TABLE ONLY public.cala_tx_template_events
    ADD CONSTRAINT cala_tx_template_events_id_sequence_key UNIQUE (id, sequence);

ALTER TABLE ONLY public.cala_tx_templates
    ADD CONSTRAINT cala_tx_templates_code_key UNIQUE (code);

ALTER TABLE ONLY public.cala_tx_templates
    ADD CONSTRAINT cala_tx_templates_pkey PRIMARY KEY (id);

CREATE INDEX idx_cala_account_set_member_account_sets_member_set ON public.cala_account_set_member_account_sets USING btree (member_account_set_id, account_set_id);

CREATE INDEX idx_cala_account_set_member_accounts_set ON public.cala_account_set_member_accounts USING btree (account_set_id);

CREATE INDEX idx_cala_account_sets_name ON public.cala_account_sets USING btree (name);

CREATE INDEX idx_cala_entries_account_id ON public.cala_entries USING btree (account_id, created_at DESC, id DESC);

CREATE INDEX idx_cala_entries_journal_id_created_at ON public.cala_entries USING btree (journal_id, created_at DESC, id DESC);

CREATE INDEX idx_cala_entries_transaction_id ON public.cala_entries USING btree (transaction_id);

CREATE INDEX idx_cala_journals_name ON public.cala_journals USING btree (name);

CREATE INDEX idx_cala_transactions_effective ON public.cala_transactions USING btree (effective);

ALTER INDEX public.cala_persistent_outbox_events_pkey ATTACH PARTITION public.cala_persistent_outbox_events_default_pkey;

ALTER INDEX public.cala_persistent_outbox_events_pkey ATTACH PARTITION public.cala_persistent_outbox_events_p0_pkey;

ALTER TABLE ONLY public.cala_account_events
    ADD CONSTRAINT cala_account_events_id_fkey FOREIGN KEY (id) REFERENCES public.cala_accounts(id);

ALTER TABLE ONLY public.cala_account_set_events
    ADD CONSTRAINT cala_account_set_events_id_fkey FOREIGN KEY (id) REFERENCES public.cala_account_sets(id);

ALTER TABLE ONLY public.cala_account_set_member_account_sets
    ADD CONSTRAINT cala_account_set_member_account_sets_account_set_id_fkey FOREIGN KEY (account_set_id) REFERENCES public.cala_account_sets(id);

ALTER TABLE ONLY public.cala_account_set_member_account_sets
    ADD CONSTRAINT cala_account_set_member_account_sets_member_account_set_id_fkey FOREIGN KEY (member_account_set_id) REFERENCES public.cala_account_sets(id);

ALTER TABLE ONLY public.cala_account_set_member_accounts
    ADD CONSTRAINT cala_account_set_member_accounts_account_set_id_fkey FOREIGN KEY (account_set_id) REFERENCES public.cala_account_sets(id);

ALTER TABLE ONLY public.cala_account_set_member_accounts
    ADD CONSTRAINT cala_account_set_member_accounts_member_account_id_fkey FOREIGN KEY (member_account_id) REFERENCES public.cala_accounts(id);

ALTER TABLE ONLY public.cala_account_sets
    ADD CONSTRAINT cala_account_sets_id_fkey FOREIGN KEY (id) REFERENCES public.cala_accounts(id);

ALTER TABLE ONLY public.cala_account_sets
    ADD CONSTRAINT cala_account_sets_journal_id_fkey FOREIGN KEY (journal_id) REFERENCES public.cala_journals(id);

ALTER TABLE ONLY public.cala_balance_history
    ADD CONSTRAINT cala_balance_history_account_id_journal_id_currency_fkey FOREIGN KEY (account_id, journal_id, currency) REFERENCES public.cala_current_balances(account_id, journal_id, currency);

ALTER TABLE ONLY public.cala_entries
    ADD CONSTRAINT cala_entries_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.cala_accounts(id);

ALTER TABLE ONLY public.cala_entries
    ADD CONSTRAINT cala_entries_account_not_account_set_fkey FOREIGN KEY (account_id, account_is_account_set) REFERENCES public.cala_accounts(id, is_account_set);

ALTER TABLE ONLY public.cala_entry_events
    ADD CONSTRAINT cala_entry_events_id_fkey FOREIGN KEY (id) REFERENCES public.cala_entries(id);

ALTER TABLE ONLY public.cala_journal_events
    ADD CONSTRAINT cala_journal_events_id_fkey FOREIGN KEY (id) REFERENCES public.cala_journals(id);

ALTER TABLE ONLY public.cala_transaction_events
    ADD CONSTRAINT cala_transaction_events_id_fkey FOREIGN KEY (id) REFERENCES public.cala_transactions(id);

ALTER TABLE ONLY public.cala_tx_template_events
    ADD CONSTRAINT cala_tx_template_events_id_fkey FOREIGN KEY (id) REFERENCES public.cala_tx_templates(id);

