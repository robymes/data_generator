# Generatore di Dati Sintetici per Retail

Questo progetto genera dati sintetici per un sistema retail e li carica in un database DuckDB. Crea tre tabelle:

1. Una tabella di anagrafiche clienti con 500.000 record
2. Una tabella di ordini con 500.000 record
3. Una tabella di transazioni d'acquisto associate agli ordini

## Requisiti

- Python 3.8+
- WSL (Windows Subsystem for Linux)
- Pacchetti richiesti elencati in requirements.txt

## Installazione

```bash
# Clona il repository
git clone https://github.com/your-username/synthetic-retail-data.git
cd synthetic-retail-data

# Crea e attiva un ambiente virtuale (opzionale ma consigliato)
python -m venv venv
source venv/bin/activate  # Su Windows: venv\Scripts\activate

# Installa i pacchetti richiesti
pip install -r requirements.txt
```

## Utilizzo

```bash
# Esegui lo script principale per generare i dati e caricarli in DuckDB
python src/main.py

# Opzioni disponibili
python src/main.py --customers 100000 --orders 50000 --customer-batch 5000 --order-batch 1000
```

## Struttura del Progetto

```
synthetic_retail_data/
├── README.md
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── config.py
│   ├── customer_generator.py
│   ├── order_generator.py
│   ├── transaction_generator.py
│   ├── db_manager.py
│   └── main.py
└── data/
    └── .gitkeep
```

## Criteri di Generazione dei Dati

### Generazione Clienti

La generazione dei clienti è progettata per simulare un database realistico di e-commerce con le seguenti caratteristiche:

- **Identificazione univoca**: Ogni cliente ha un `customer_id` univoco di 10 caratteri alfanumerici.

- **Distribuzione geografica**: La distribuzione dei clienti per paese rispecchia il PIL relativo tra le varie nazioni, con una maggiore concentrazione nei paesi con economie più forti. Il 95% dei clienti ha un paese associato, che può essere rappresentato nella lingua originale, con il codice ISO o in inglese.

- **Dati personali**: Nomi e cognomi sono generati in modo coerente con il paese di origine, con possibili errori di battitura (typo) per simulare l'inserimento manuale. I dati di nascita sono presenti nel 50% dei record, con vari formati di data (YYYY-MM-DD, DD/MM/YYYY, ecc.).

- **Contatti**: Le email sono generate per l'80% dei clienti, utilizzando 50 domini diversi, con predominanza di quelli globali (gmail, outlook) ma anche domini specifici per paese. Il 60% delle email contiene riferimenti al nome e cognome del cliente, spesso con l'aggiunta di cifre numeriche. I numeri di telefono sono presenti nel 75% dei casi, in vari formati (con/senza spazi, con/senza prefisso internazionale).

- **Canali di acquisizione**: I clienti provengono da due canali: e-commerce (40%) e POS (60%), indicati nel campo `source_id` (1 per e-commerce, 2 per POS).

- **Duplicazione clienti**: Il 20% dei clienti è duplicato a causa della registrazione su entrambi i canali. L'80% delle duplicazioni è identificabile tramite corrispondenza esatta di email o numero di telefono, mentre il 20% è riconoscibile solo tramite nome e cognome (con possibili typo in metà dei casi).

### Generazione Ordini

Gli ordini collegano i clienti alle transazioni e sono generati secondo questi criteri:

- **Identificazione**: Ogni ordine ha un `order_id` univoco e un riferimento al `customer_id` che mantiene l'integrità referenziale.

- **Distribuzione temporale**: Le date degli ordini vanno dal 2023 ad oggi, con una distribuzione realistica nel tempo.

- **Canali di vendita**: Gli ordini sono equamente distribuiti tra e-commerce (50%) e POS (50%), indicati nel campo `source_id`.

- **Associazione cliente-ordine**: Ogni ordine è associato a un solo cliente, ma un cliente può avere più ordini.

### Generazione Transazioni

Le transazioni rappresentano i singoli prodotti acquistati in un ordine:

- **Struttura ordine-transazione**: Ogni ordine contiene da 1 a 10 transazioni (prodotti acquistati), con un `order_id` che mantiene l'integrità referenziale con la tabella degli ordini.

- **Prodotti e prezzi**: I prodotti sono selezionati da categorie tipiche di supermercato, con una distribuzione bilanciata per tipologia di spesa. I prezzi unitari variano in base alla categoria di prodotto e al potere d'acquisto del paese del cliente, rispecchiando le differenze di reddito pro capite tra le nazioni.

- **Quantità e importi**: Ogni transazione include una quantità acquistata e calcola l'importo totale (quantità × prezzo unitario).

Il sistema di generazione utilizza un approccio a batch per ottimizzare l'utilizzo della memoria, consentendo di generare dataset di grandi dimensioni senza problemi di memoria. Tutti i dati vengono caricati incrementalmente nel database DuckDB, garantendo l'integrità referenziale tra le tabelle.

## Interrogazioni di Esempio

Il database generato può essere interrogato per ottenere insight interessanti, come:

```sql
-- Valore medio degli ordini e numero medio di transazioni per paese
WITH order_stats AS (
    SELECT o.order_id, c.country, SUM(t.total_amount) AS order_total, 
           COUNT(t.transaction_id) AS transactions_count
    FROM orders o
    JOIN customers c ON o.customer_id = c.customer_id
    JOIN transactions t ON o.order_id = t.order_id
    GROUP BY o.order_id, c.country
)
SELECT COALESCE(country, 'Non specificato') AS country,
       COUNT(*) AS total_orders,
       ROUND(AVG(order_total), 2) AS avg_order_value,
       ROUND(AVG(transactions_count), 2) AS avg_items_per_order
FROM order_stats
GROUP BY country
ORDER BY total_orders DESC;
```

## Contribuire

Sei invitato a contribuire al progetto! Puoi farlo nei seguenti modi:

1. Segnalando bug o problemi
2. Suggerendo nuove funzionalità
3. Inviando pull request con miglioramenti al codice

## Licenza

Questo progetto è rilasciato sotto licenza MIT.