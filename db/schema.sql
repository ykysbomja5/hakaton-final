create extension if not exists pgcrypto;

create schema if not exists analytics;
create schema if not exists app;

create table if not exists analytics.ride_metrics_daily (
    stat_date date not null,
    city text not null,
    service_class text not null,
    source_channel text not null,
    driver_segment text not null,
    completed_rides integer not null,
    cancelled_rides integer not null,
    gross_revenue_rub numeric(14,2) not null,
    avg_fare_rub numeric(10,2) not null,
    active_drivers integer not null,
    created_at timestamptz not null default now(),
    primary key (stat_date, city, service_class, source_channel, driver_segment)
);

create or replace view analytics.v_ride_metrics as
select
    stat_date,
    city,
    service_class,
    source_channel,
    driver_segment,
    completed_rides,
    cancelled_rides,
    completed_rides + cancelled_rides as total_rides,
    gross_revenue_rub,
    avg_fare_rub,
    active_drivers
from analytics.ride_metrics_daily;

create table if not exists app.saved_reports (
    id bigserial primary key,
    name text not null,
    query_text text not null,
    sql_text text not null,
    intent jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

create table if not exists app.report_runs (
    id bigserial primary key,
    report_id bigint not null references app.saved_reports(id) on delete cascade,
    executed_at timestamptz not null default now(),
    status text not null,
    row_count integer not null default 0,
    error_text text
);

create table if not exists app.query_logs (
    id bigserial primary key,
    query_text text not null,
    intent jsonb not null,
    sql_text text,
    confidence numeric(4,2) not null,
    status text not null,
    latency_ms bigint not null default 0,
    error_text text,
    created_at timestamptz not null default now()
);

create table if not exists app.semantic_terms (
    id bigserial primary key,
    term text not null,
    kind text not null,
    canonical_value text not null,
    description text not null,
    unique (term, kind)
);

create index if not exists idx_ride_metrics_date on analytics.ride_metrics_daily (stat_date);
create index if not exists idx_ride_metrics_city on analytics.ride_metrics_daily (city);
create index if not exists idx_query_logs_created_at on app.query_logs (created_at desc);
create index if not exists idx_report_runs_report_id on app.report_runs (report_id, executed_at desc);

do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'analytics_readonly') then
        create role analytics_readonly login password 'analytics_demo';
    end if;
exception
    when insufficient_privilege then
        null;
end $$;

grant usage on schema analytics to analytics_readonly;
grant select on analytics.ride_metrics_daily to analytics_readonly;
grant select on analytics.v_ride_metrics to analytics_readonly;
