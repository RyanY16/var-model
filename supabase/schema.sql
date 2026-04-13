create table if not exists public.risk_metrics (
    date date not null,
    scenario text not null,
    sim_days integer not null,
    portfolio_value double precision,
    var_95 double precision,
    var_99 double precision,
    es_95 double precision,
    es_99 double precision,
    primary key (date, scenario, sim_days)
);

create table if not exists public.marginal_risk (
    date date not null,
    sim_days integer not null,
    excluded_ticker text not null,
    var_95 double precision,
    var_99 double precision,
    es_95 double precision,
    es_99 double precision,
    primary key (date, sim_days, excluded_ticker)
);

create table if not exists public.pnls (
    date date not null,
    sim_days integer not null,
    sim_index integer not null,
    pnl double precision,
    primary key (date, sim_days, sim_index)
);

create table if not exists public.portfolio_breakdown (
    date date not null,
    ticker text not null,
    company_name text,
    quantity double precision,
    price double precision,
    market_value double precision,
    weight double precision,
    primary key (date, ticker)
);

create table if not exists public.stock_prices (
    date date not null,
    ticker text not null,
    company_name text,
    price double precision,
    primary key (date, ticker)
);

create index if not exists risk_metrics_date_idx on public.risk_metrics (date desc);
create index if not exists marginal_risk_date_idx on public.marginal_risk (date desc);
create index if not exists pnls_date_idx on public.pnls (date desc);
create index if not exists portfolio_breakdown_date_idx on public.portfolio_breakdown (date desc);
create index if not exists stock_prices_date_idx on public.stock_prices (date desc);
