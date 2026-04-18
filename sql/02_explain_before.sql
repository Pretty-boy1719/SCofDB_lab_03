\timing on
\echo '=== BEFORE OPTIMIZATION ==='

-- Рекомендуемые настройки для сравнимых замеров
SET max_parallel_workers_per_gather = 0;
SET work_mem = '32MB';
ANALYZE;

-- ============================================================
-- Q1: Заказы конкретного пользователя, сортировка по дате
-- Q1: Фильтрация + сортировка (пример класса запроса)
-- Узкое место: Sequential Scan по всей таблице orders (100 000 строк)
-- ============================================================
\echo '--- Q1: Заказы пользователя (фильтр по user_id + сортировка) ---'
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

-- ============================================================
-- Q2: Оплаченные заказы за диапазон дат (аналитика/отчётность)
--  Q2: Фильтрация по статусу + диапазону дат
-- Узкое место: Sequential Scan + двойной фильтр (status + created_at)
-- ============================================================
\echo '--- Q2: Оплаченные заказы за первое полугодие 2025 ---'
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

-- ============================================================
-- Q3: TOP-10 пользователей по выручке (join + GROUP by)
-- Q3: join + GROUP by
-- Узкое место: Hash join + Sequential Scan на orders + Sort
-- ============================================================
\echo '--- Q3: TOP-10 пользователей по сумме оплаченных и завершённых заказов ---'
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

-- ============================================================
-- Q4: Популярные товары — join orders + order_items с фильтром дат
-- (Опционально) Q4: полный агрегат по периоду, который сложно ускорить индексами
-- Узкое место: Seq Scan на обеих таблицах + Nested Loop + GROUP by
-- ============================================================
\echo '--- Q4 (доп.): Топ-10 товаров по количеству заказов за 2025 год ---'
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
