-- Analisi dei duplicati dei clienti
SELECT 
    a.name, 
    a.surname, 
    a.email, 
    a.mobile_phone_number,
    COUNT(*) AS num_records,
    STRING_AGG(a.customer_id, ', ') AS customer_ids,
    STRING_AGG(CAST(a.source_id AS VARCHAR), ', ') AS source_channels
FROM 
    customers a
JOIN 
    customers b ON (
        -- Match per email (se presente)
        (a.email IS NOT NULL AND a.email = b.email)
        OR 
        -- Match per telefono (se presente)
        (a.mobile_phone_number IS NOT NULL AND a.mobile_phone_number = b.mobile_phone_number)
        OR
        -- Match per nome e cognome
        (a.name = b.name AND a.surname = b.surname)
    )
    AND a.customer_id <> b.customer_id  -- Esclude self-joins
GROUP BY 
    a.name, a.surname, a.email, a.mobile_phone_number
HAVING 
    COUNT(*) > 1  -- Mostra solo gruppi con più di un record
ORDER BY 
    COUNT(*) DESC, a.name, a.surname
LIMIT 100;

-- Analisi degli ordini e relativi prodotti
SELECT 
    o.order_id,
    o.customer_id,
    o.source_id,
    o.date,
    COUNT(t.transaction_id) AS num_items,
    SUM(t.total_amount) AS order_total,
    STRING_AGG(t.product, ', ') AS products
FROM 
    orders o
JOIN 
    transactions t ON o.order_id = t.order_id
GROUP BY 
    o.order_id, o.customer_id, o.source_id, o.date
ORDER BY 
    num_items DESC, order_total DESC
LIMIT 100;-- Query per ottenere statistiche complete per i clienti
SELECT 
    c.customer_id,
    c.country,
    c.name,
    c.surname,
    c.date_of_birth,
    c.email,
    c.mobile_phone_number,
    c.source_id,
    COALESCE(t_stats.avg_transaction_amount, 0) AS avg_transaction_amount,
    t_stats.last_transaction_date,
    t_stats.order_count,
    t_stats.transaction_count
FROM 
    customers c
LEFT JOIN (
    SELECT 
        customer_id,
        AVG(total_amount) AS avg_transaction_amount,
        MAX(date) AS last_transaction_date,
        COUNT(DISTINCT order_id) AS order_count,
        COUNT(*) AS transaction_count
    FROM 
        transactions
    GROUP BY 
        customer_id
) t_stats ON c.customer_id = t_stats.customer_id;-- Query per ottenere statistiche complete per i clienti
SELECT 
    c.customer_id,
    c.country,
    c.name,
    c.surname,
    c.date_of_birth,
    c.email,
    c.mobile_phone_number,
    c.source_id,
    COUNT(DISTINCT o.order_id) AS order_count,
    COUNT(t.transaction_id) AS transaction_count,
    COALESCE(AVG(t.total_amount), 0) AS avg_transaction_amount,
    MAX(o.date) AS last_order_date
FROM 
    customers c
LEFT JOIN 
    orders o ON c.customer_id = o.customer_id
LEFT JOIN 
    transactions t ON o.order_id = t.order_id
GROUP BY 
    c.customer_id, c.country, c.name, c.surname, c.date_of_birth, c.email, c.mobile_phone_number, c.source_id;

-- Query per ottenere numero di clienti e scontrino medio per paese
SELECT 
    COALESCE(c.country, 'Non specificato') AS country,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COUNT(DISTINCT o.order_id) AS total_orders,
    COUNT(t.transaction_id) AS total_transaction_items,
    COALESCE(AVG(t.total_amount), 0) AS avg_item_amount,
    COALESCE(SUM(t.total_amount) / COUNT(DISTINCT o.order_id), 0) AS avg_order_amount
FROM 
    customers c
LEFT JOIN 
    orders o ON c.customer_id = o.customer_id
LEFT JOIN 
    transactions t ON o.order_id = t.order_id
GROUP BY 
    c.country
ORDER BY 
    total_customers DESC, avg_order_amount DESC;

-- Distribuzione delle transazioni per canale e cliente
SELECT 
    c.source_id AS customer_source,
    o.source_id AS order_source,
    COUNT(DISTINCT c.customer_id) AS unique_customers,
    COUNT(DISTINCT o.order_id) AS unique_orders,
    COUNT(t.transaction_id) AS transaction_count,
    SUM(t.total_amount) AS total_amount,
    AVG(t.total_amount) AS avg_transaction_amount
FROM 
    customers c
JOIN 
    orders o ON c.customer_id = o.customer_id
JOIN 
    transactions t ON o.order_id = t.order_id
GROUP BY 
    c.source_id, o.source_id
ORDER BY 
    c.source_id, o.source_id;

-- Query per ottenere numero di clienti e scontrino medio per paese
SELECT 
    COALESCE(c.country, 'Non specificato') AS country,
    COUNT(DISTINCT c.customer_id) AS total_customers,
    COALESCE(AVG(t.total_amount), 0) AS avg_receipt_amount,
    COUNT(DISTINCT t.order_id) AS total_orders,
    COUNT(DISTINCT t.transaction_id) AS total_transactions
FROM 
    customers c
LEFT JOIN 
    transactions t ON c.customer_id = t.customer_id
GROUP BY 
    c.country
ORDER BY 
    total_customers DESC, avg_receipt_amount DESC;

-- Clienti più attivi (per numero di transazioni)
SELECT
    c.customer_id,
    c.name,
    c.surname,
    c.country,
    COUNT(t.transaction_id) AS transaction_count,
    SUM(t.total_amount) AS total_spent,
    AVG(t.total_amount) AS avg_amount_per_transaction
FROM
    customers c
JOIN
    transactions t ON c.customer_id = t.customer_id
GROUP BY
    c.customer_id, c.name, c.surname, c.country
ORDER BY
    transaction_count DESC
LIMIT 20;

-- Prodotti più venduti per quantità
SELECT
    product,
    SUM(quantity) AS total_quantity,
    COUNT(transaction_id) AS transaction_count,
    SUM(total_amount) AS total_revenue
FROM
    transactions
GROUP BY
    product
ORDER BY
    total_quantity DESC
LIMIT 20;

-- Analisi vendite nel tempo (mensile)
SELECT
    EXTRACT(YEAR FROM date) AS year,
    EXTRACT(MONTH FROM date) AS month,
    COUNT(transaction_id) AS transaction_count,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_transaction_amount
FROM
    transactions
GROUP BY
    year, month
ORDER BY
    year, month;

-- Distribuzione clienti per formati di email
SELECT
    REGEXP_EXTRACT(email, '@(.+)$') AS email_domain,
    COUNT(*) AS customer_count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers WHERE email IS NOT NULL), 2) AS percentage
FROM
    customers
WHERE
    email IS NOT NULL
GROUP BY
    email_domain
ORDER BY
    customer_count DESC
LIMIT 25;

-- Distribuzione formati data di nascita
SELECT
    CASE
        WHEN date_of_birth LIKE '____-__-__' THEN 'YYYY-MM-DD'
        WHEN date_of_birth LIKE '__/__/____' THEN 'DD/MM/YYYY'
        WHEN date_of_birth LIKE '__/__/____' THEN 'MM/DD/YYYY'
        WHEN date_of_birth LIKE '____/__/__' THEN 'YYYY/MM/DD'
        WHEN date_of_birth LIKE '__-__-____' THEN 'DD-MM-YYYY'
        ELSE 'Altri formati'
    END AS date_format,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM customers WHERE date_of_birth IS NOT NULL), 2) AS percentage
FROM
    customers
WHERE
    date_of_birth IS NOT NULL
GROUP BY
    date_format
ORDER BY
    count DESC;
