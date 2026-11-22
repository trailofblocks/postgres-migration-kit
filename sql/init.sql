CREATE TABLE IF NOT EXISTS payments (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    external_id text NOT NULL UNIQUE,
    amount_minor bigint NOT NULL CHECK (amount_minor BETWEEN 1 AND 9000000000000),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status text NOT NULL CHECK (status IN ('PENDING','CAPTURED','REFUNDED'))
);

CREATE TABLE IF NOT EXISTS migration_progress (
    singleton boolean PRIMARY KEY CHECK (singleton) DEFAULT true,
    stage text NOT NULL CHECK (stage IN ('legacy', 'expanded', 'validated', 'contracted')),
    seed_rows integer NOT NULL DEFAULT 0,
    cutoff_id bigint NOT NULL DEFAULT 0,
    checkpoint_id bigint NOT NULL DEFAULT 0 CHECK (checkpoint_id <= cutoff_id),
    copied_rows bigint NOT NULL DEFAULT 0,
    batches bigint NOT NULL DEFAULT 0
);
