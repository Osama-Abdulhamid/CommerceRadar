BEGIN;

CREATE TABLE IF NOT EXISTS app_users (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL UNIQUE,
    display_name TEXT NOT NULL,
    password_hash TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (email = lower(email)),
    CHECK (length(display_name) BETWEEN 1 AND 100)
);

CREATE TABLE IF NOT EXISTS user_sessions (
    token_hash TEXT PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS user_sessions_user_idx
    ON user_sessions(user_id);

CREATE TABLE IF NOT EXISTS user_settings (
    user_id UUID PRIMARY KEY REFERENCES app_users(id) ON DELETE CASCADE,
    preferred_currency TEXT NOT NULL DEFAULT 'USD',
    alerts_enabled BOOLEAN NOT NULL DEFAULT true,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (preferred_currency IN ('EGP', 'USD', 'EUR', 'GBP', 'SAR', 'AED'))
);

CREATE TABLE IF NOT EXISTS alert_rules (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL REFERENCES app_users(id) ON DELETE CASCADE,
    source TEXT NOT NULL,
    source_product_id TEXT NOT NULL,
    rule_type TEXT NOT NULL,
    target_price NUMERIC(18, 2),
    currency TEXT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (length(source) BETWEEN 1 AND 100),
    CHECK (length(source_product_id) BETWEEN 1 AND 200),
    CHECK (rule_type IN ('price_below', 'price_drop', 'back_in_stock')),
    CHECK (
        (rule_type = 'price_below'
            AND target_price IS NOT NULL AND target_price >= 0)
        OR
        (rule_type <> 'price_below' AND target_price IS NULL)
    ),
    CHECK (
        (rule_type IN ('price_below', 'price_drop')
            AND currency IS NOT NULL
            AND currency IN ('EGP', 'USD', 'EUR', 'GBP', 'SAR', 'AED'))
        OR
        (rule_type = 'back_in_stock' AND currency IS NULL)
    )
);

CREATE INDEX IF NOT EXISTS alert_rules_user_idx
    ON alert_rules(user_id);

CREATE TABLE IF NOT EXISTS alerts (
    id UUID PRIMARY KEY,
    rule_id UUID NOT NULL REFERENCES alert_rules(id) ON DELETE CASCADE,
    event_id UUID NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    message TEXT NOT NULL,
    details JSONB NOT NULL DEFAULT '{}'::jsonb,
    is_read BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (rule_id, event_id)
);

CREATE INDEX IF NOT EXISTS alerts_rule_created_idx
    ON alerts(rule_id, created_at DESC);

COMMIT;
