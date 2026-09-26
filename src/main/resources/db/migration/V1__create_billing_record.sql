CREATE TABLE billing_record (
    order_id         UUID PRIMARY KEY,
    product           VARCHAR(255)   NOT NULL,
    quantity          INTEGER        NOT NULL,
    unit_price        NUMERIC(12, 2) NOT NULL,
    total_amount      NUMERIC(12, 2) NOT NULL,
    order_created_at  TIMESTAMPTZ    NOT NULL,
    persisted_at      TIMESTAMPTZ    NOT NULL DEFAULT now()
);
