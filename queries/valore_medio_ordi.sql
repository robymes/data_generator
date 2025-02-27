-- Query per calcolare valore medio ordine e numero medio di transazioni per ordine, per ogni paese

WITH order_stats AS (
    -- Calcola totale e conteggio transazioni per ogni ordine
    SELECT 
        o.order_id,
        c.country,
        SUM(t.total_amount) AS order_total,
        COUNT(t.transaction_id) AS transactions_count
    FROM 
        orders o
    JOIN 
        customers c ON o.customer_id = c.customer_id
    JOIN 
        transactions t ON o.order_id = t.order_id
    GROUP BY 
        o.order_id, c.country
)

SELECT 
    -- Gestisco il caso di country NULL
    COALESCE(country, 'Non specificato') AS country,
    -- Numero di ordini per paese
    COUNT(*) AS total_orders,
    -- Valore medio dell'ordine
    ROUND(AVG(order_total), 2) AS avg_order_value,
    -- Numero medio di item per ordine
    ROUND(AVG(transactions_count), 2) AS avg_items_per_order,
    -- Valore medio per transazione
    ROUND(SUM(order_total) / SUM(transactions_count), 2) AS avg_transaction_value,
    -- Totali per paese
    ROUND(SUM(order_total), 2) AS total_country_revenue
FROM 
    order_stats
GROUP BY 
    country
ORDER BY 
    total_orders DESC, avg_order_value DESC;