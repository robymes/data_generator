-- Query per individuare i top 10 clienti per valore medio degli ordini

WITH order_values AS (
    -- Calcola il valore totale di ogni ordine
    SELECT 
        o.order_id,
        o.customer_id,
        SUM(t.total_amount) AS order_total,
        COUNT(t.transaction_id) AS items_count
    FROM 
        orders o
    JOIN 
        transactions t ON o.order_id = t.order_id
    GROUP BY 
        o.order_id, o.customer_id
),

customer_order_stats AS (
    -- Calcola le statistiche degli ordini per ogni cliente
    SELECT 
        ov.customer_id,
        COUNT(ov.order_id) AS orders_count,
        AVG(ov.order_total) AS avg_order_value,
        SUM(ov.order_total) AS total_spent,
        AVG(ov.items_count) AS avg_items_per_order
    FROM 
        order_values ov
    GROUP BY 
        ov.customer_id
)

-- Seleziona i top 10 clienti per valore medio dell'ordine
SELECT 
    c.customer_id,
    c.name,
    c.surname,
    c.country,
    c.email,
    CASE 
        WHEN c.source_id = 1 THEN 'E-commerce'
        WHEN c.source_id = 2 THEN 'POS'
        ELSE 'Altro'
    END AS source_channel,
    cos.orders_count AS total_orders,
    ROUND(cos.avg_order_value, 2) AS avg_order_value,
    ROUND(cos.total_spent, 2) AS total_spent,
    ROUND(cos.avg_items_per_order, 1) AS avg_items_per_order
FROM 
    customer_order_stats cos
JOIN 
    customers c ON cos.customer_id = c.customer_id
-- Filtra solo clienti con almeno 2 ordini (opzionale, per risultati più significativi)
WHERE 
    cos.orders_count >= 2
ORDER BY 
    cos.avg_order_value DESC
LIMIT 10;