-- Query per calcolare il valore totale, numero di ordini e clienti distinti per ogni prodotto

WITH product_orders AS (
    -- Per ogni prodotto, ottiene la lista degli ordini distinti che lo contengono
    SELECT
        t.product,
        t.order_id
    FROM
        transactions t
    GROUP BY
        t.product, t.order_id
),

product_customers AS (
    -- Per ogni prodotto, ottiene la lista dei clienti distinti che l'hanno acquistato
    SELECT
        t.product,
        o.customer_id
    FROM
        transactions t
    JOIN
        orders o ON t.order_id = o.order_id
    GROUP BY
        t.product, o.customer_id
)

SELECT
    t.product,
    -- Valore totale di tutte le transazioni per questo prodotto
    SUM(t.total_amount) AS total_revenue,
    -- Numero di ordini distinti in cui appare questo prodotto
    (
        SELECT COUNT(DISTINCT po.order_id) 
        FROM product_orders po 
        WHERE po.product = t.product
    ) AS total_orders,
    -- Quantità totale acquistata
    SUM(t.quantity) AS total_quantity,
    -- Prezzo unitario medio
    ROUND(AVG(t.unit_price), 2) AS avg_unit_price,
    -- Numero di clienti distinti che hanno acquistato questo prodotto
    (
        SELECT COUNT(DISTINCT pc.customer_id) 
        FROM product_customers pc 
        WHERE pc.product = t.product
    ) AS unique_customers
FROM
    transactions t
GROUP BY
    t.product
ORDER BY
    total_revenue DESC;