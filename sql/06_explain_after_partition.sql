\timing on
\echo '=== AFTER PARTITIONING ==='

SET max_parallel_workers_per_gather = 0;
SET work_mem = '32MB';

ANALYZE orders_partitioned;

-- ============================================================
-- Финальные замеры на партиционированной таблице orders_partitioned.
-- Сравниваем три состояния:
--   1) До оптимизаций      (02_explain_before.sql — таблица orders)
--   2) После индексов      (04_explain_after_indexes.sql — таблица orders + индексы)
--   3) После партиций      (этот файл — таблица orders_partitioned + индексы)
-- ============================================================

\echo '--- Q1: Заказы пользователя (partitioned) ---'
EXPLAIN (ANALYZE, BUFFERS)
select 
  o.id
  , o.status
  , o.total_amount
  , o.created_at
from orders o
where 1=1
  and o.user_id = (select id from users where email = 'user00001@example.com')
order by o.created_at desc;

\echo '--- Q2: Оплаченные заказы за 2025-H1 (partitioned + partition pruning) ---'
-- Ожидаем: Planner задействует только orders_2025_01 … orders_2025_06 (6 из 25 партиций)
EXPLAIN (ANALYZE, BUFFERS)
select 
  id
  , user_id
  , total_amount
  , created_at
from orders
where 1=1
  and "status" = 'paid'
  and created_at >= '2025-01-01'
  and created_at <  '2025-07-01'
order by created_at desc;

\echo '--- Q3: TOP-10 пользователей по выручке (partitioned) ---'
EXPLAIN (ANALYZE, BUFFERS)
select
    u.id
    , u.email,
    , count(o.id) as order_count
    , round(sum(o.total_amount)::numeric, 2) as total_revenue
from users u
  inner join orders o 
    on o.user_id = u.id
where 1=1
  and o.status IN ('paid', 'completed')
group by u.id, u.email
order by total_revenue desc
limit 10;

\echo '--- Q4: Топ-10 товаров за 2025 год (partitioned) ---'
EXPLAIN (ANALYZE, BUFFERS)
select
    oi.product_name
    , count(*) as times_ordered
    . sum(oi.quantity) as total_qty
from order_items oi
  inner join orders o 
    on o.id = oi.order_id
where 1=1
  and o.created_at >= '2025-01-01'
  and o.created_at <  '2026-01-01'
group by oi.product_name
order by times_ordered desc
limit 10;

-- ============================================================
-- Итоговая сводка: размер каждой партиции
-- ============================================================
\echo '--- Размер партиций ---'
SELECT
    inhrelid::regclass                                    AS partition,
    pg_size_pretty(pg_relation_size(inhrelid))            AS size,
    (SELECT COUNT(*) FROM pg_class c
     JOIN pg_inherits i ON i.inhrelid = c.oid
     WHERE c.oid = inhrelid)                              AS sub_parts
FROM pg_inherits
WHERE inhparent = 'orders_partitioned'::regclass
ORDER BY partition;
