truncate table analytics.ride_metrics_daily restart identity cascade;
truncate table app.semantic_terms restart identity cascade;

insert into app.semantic_terms (term, kind, canonical_value, description) values
('поездки', 'metric', 'completed_rides', 'Количество завершенных поездок'),
('завершенные поездки', 'metric', 'completed_rides', 'Количество завершенных поездок'),
('заказы', 'metric', 'total_rides', 'Все поездки'),
('отмены', 'metric', 'cancellations', 'Количество отмен'),
('выручка', 'metric', 'revenue', 'Суммарная выручка'),
('средний чек', 'metric', 'avg_fare', 'Средняя стоимость поездки'),
('активные водители', 'metric', 'active_drivers', 'Количество активных водителей'),
('по городам', 'dimension', 'city', 'Группировка по городам'),
('по тарифам', 'dimension', 'service_class', 'Группировка по тарифам'),
('по каналам', 'dimension', 'source_channel', 'Группировка по каналам'),
('по дням', 'dimension', 'day', 'Группировка по дням'),
('по неделям', 'dimension', 'week', 'Группировка по неделям'),
('по месяцам', 'dimension', 'month', 'Группировка по месяцам'),
('москва', 'filter', 'Москва', 'Фильтр по городу'),
('питер', 'filter', 'Санкт-Петербург', 'Фильтр по городу'),
('казань', 'filter', 'Казань', 'Фильтр по городу'),
('эконом', 'filter', 'Эконом', 'Фильтр по тарифу'),
('комфорт', 'filter', 'Комфорт', 'Фильтр по тарифу'),
('бизнес', 'filter', 'Бизнес', 'Фильтр по тарифу'),
('приложение', 'filter', 'Приложение', 'Фильтр по каналу'),
('сайт', 'filter', 'Сайт', 'Фильтр по каналу'),
('партнеры', 'filter', 'Партнеры', 'Фильтр по каналу');

with days as (
    select generate_series(current_date - interval '89 day', current_date, interval '1 day')::date as stat_date
),
cities as (
    select * from (values
        ('Москва', 5),
        ('Санкт-Петербург', 4),
        ('Казань', 3),
        ('Екатеринбург', 2),
        ('Новосибирск', 1)
    ) as t(city, city_weight)
),
classes as (
    select * from (values
        ('Эконом', 1, 340.0),
        ('Комфорт', 2, 510.0),
        ('Бизнес', 3, 880.0)
    ) as t(service_class, class_weight, base_fare)
),
channels as (
    select * from (values
        ('Приложение', 3),
        ('Сайт', 2),
        ('Партнеры', 1)
    ) as t(source_channel, channel_weight)
),
segments as (
    select * from (values
        ('Новые', 1),
        ('Стабильные', 3),
        ('Премиум', 2)
    ) as t(driver_segment, segment_weight)
),
base as (
    select
        d.stat_date,
        c.city,
        cl.service_class,
        ch.source_channel,
        s.driver_segment,
        35 + ((extract(doy from d.stat_date)::int * 7 + c.city_weight * 13 + cl.class_weight * 17 + ch.channel_weight * 19 + s.segment_weight * 23) % 120) as completed_rides,
        4 + ((extract(doy from d.stat_date)::int * 5 + c.city_weight * 7 + cl.class_weight * 9 + ch.channel_weight * 11 + s.segment_weight * 13) % 25) as cancelled_rides,
        cl.base_fare + (c.city_weight * 45) + (ch.channel_weight * 15) + (s.segment_weight * 22) as avg_fare_rub,
        12 + ((extract(doy from d.stat_date)::int * 3 + c.city_weight * 5 + cl.class_weight * 7 + s.segment_weight * 11) % 65) as active_drivers
    from days d
    cross join cities c
    cross join classes cl
    cross join channels ch
    cross join segments s
)
insert into analytics.ride_metrics_daily (
    stat_date,
    city,
    service_class,
    source_channel,
    driver_segment,
    completed_rides,
    cancelled_rides,
    gross_revenue_rub,
    avg_fare_rub,
    active_drivers
)
select
    stat_date,
    city,
    service_class,
    source_channel,
    driver_segment,
    completed_rides,
    cancelled_rides,
    round((completed_rides * avg_fare_rub)::numeric, 2) as gross_revenue_rub,
    round(avg_fare_rub::numeric, 2),
    active_drivers
from base
order by stat_date, city, service_class, source_channel, driver_segment;
