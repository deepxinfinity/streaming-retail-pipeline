CREATE OR REPLACE FUNCTION touch_updated_at() RETURNS trigger AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TABLE products (
    sku_id        text PRIMARY KEY,
    sku_name      text NOT NULL,
    department    text NOT NULL,
    category_id   text NOT NULL,
    category_name text NOT NULL,
    brand         text NOT NULL,
    base_price    numeric(9,2) NOT NULL CHECK (base_price > 0),
    unit_cost     numeric(9,2) NOT NULL CHECK (unit_cost > 0),
    active        boolean NOT NULL DEFAULT true,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE stores (
    store_id     text PRIMARY KEY,
    store_name   text NOT NULL,
    region       text NOT NULL,
    city         text NOT NULL,
    store_format text NOT NULL,  -- hypermarket | supermarket | express | online
    opened_date  date NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE customers (
    customer_id   text PRIMARY KEY,
    full_name     text NOT NULL,
    segment       text NOT NULL,  -- value | mainstream | premium
    home_store_id text NOT NULL REFERENCES stores(store_id),
    signup_date   date NOT NULL,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE price_list (
    price_id       bigserial PRIMARY KEY,
    sku_id         text NOT NULL REFERENCES products(sku_id),
    scope          text NOT NULL DEFAULT 'all',  -- 'all' or a store_id
    price          numeric(9,2) NOT NULL CHECK (price > 0),
    effective_from date NOT NULL,
    updated_at     timestamptz NOT NULL DEFAULT now(),
    UNIQUE (sku_id, scope, effective_from)
);

CREATE TABLE promotions (
    promo_id     text PRIMARY KEY,
    promo_name   text NOT NULL,
    discount_pct numeric(4,3) NOT NULL CHECK (discount_pct > 0 AND discount_pct < 1),
    start_date   date NOT NULL,
    end_date     date NOT NULL CHECK (end_date >= start_date),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE promo_products (
    promo_id   text NOT NULL REFERENCES promotions(promo_id),
    sku_id     text NOT NULL REFERENCES products(sku_id),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (promo_id, sku_id)
);

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['products','stores','customers','price_list','promotions','promo_products']
  LOOP
    EXECUTE format(
      'CREATE TRIGGER trg_%s_touch BEFORE UPDATE ON %I FOR EACH ROW EXECUTE FUNCTION touch_updated_at()',
      t, t);
  END LOOP;
END $$;

CREATE INDEX idx_price_list_lookup ON price_list (sku_id, scope, effective_from DESC);
CREATE INDEX idx_promotions_window ON promotions (start_date, end_date);
