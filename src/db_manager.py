"""
Module for managing the DuckDB database operations.
"""

import os
import duckdb
import polars as pl

from config import DB_FILE

class DBManager:
    """Class to manage DuckDB database operations."""
    
    def __init__(self, db_file: str = DB_FILE):
        """Initialize the database connection."""
        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(db_file), exist_ok=True)
        
        # Connect to the database
        self.conn = duckdb.connect(db_file)
        
        # Initialize database structure
        self._init_db()
    
    def _init_db(self):
        """Initialize the database structure."""
        # Create a sequence for transaction IDs
        self.conn.execute("""
        CREATE SEQUENCE IF NOT EXISTS transaction_id_seq;
        """)
        
        # Create customers table
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS customers (
            customer_id VARCHAR(10) PRIMARY KEY,
            country VARCHAR,
            name VARCHAR NOT NULL,
            surname VARCHAR NOT NULL,
            date_of_birth VARCHAR,
            email VARCHAR,
            mobile_phone_number VARCHAR,
            source_id INTEGER NOT NULL
        )
        """)
        
        # Create orders table
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id VARCHAR(15) PRIMARY KEY,
            customer_id VARCHAR(10) NOT NULL,
            source_id INTEGER NOT NULL,
            date DATE NOT NULL,
            FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
        )
        """)
        
        # Create transactions table without source_id and date
        self.conn.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id BIGINT PRIMARY KEY,
            order_id VARCHAR(15) NOT NULL,
            product VARCHAR NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price DECIMAL(10, 2) NOT NULL,
            total_amount DECIMAL(10, 2) NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id)
        )
        """)
    
    def load_customers(self, df: pl.DataFrame):
        """Load customer data into the database."""
        try:
            # Check if dataframe is empty
            if df.is_empty():
                print("Warning: Empty customer dataframe provided, nothing to load")
                return
                
            # Convert Polars DataFrame to pandas for DuckDB compatibility
            pandas_df = df.to_pandas()  # noqa: F841
            
            # Insert data into the customers table
            self.conn.execute("""
            INSERT INTO customers 
            SELECT * FROM pandas_df
            """)
            
            # Show the count of inserted records
            result = self.conn.execute("SELECT COUNT(*) FROM customers").fetchone()
            print(f"Loaded {result[0]} customer records into the database.")
        except Exception as e:
            print(f"ERROR loading customers: {str(e)}")
            raise
    
    def load_orders(self, df: pl.DataFrame):
        """Load order data into the database."""
        try:
            # Check if dataframe is empty
            if df.is_empty():
                print("Warning: Empty order dataframe provided, nothing to load")
                return
                
            # Convert Polars DataFrame to pandas for DuckDB compatibility
            pandas_df = df.to_pandas()  # noqa: F841
            
            # Insert data into the orders table
            self.conn.execute("""
            INSERT INTO orders (order_id, customer_id, source_id, date)
            SELECT order_id, customer_id, source_id, date
            FROM pandas_df
            """)
            
            # Show the count of inserted records
            result = self.conn.execute("SELECT COUNT(*) FROM orders").fetchone()
            print(f"Loaded orders into the database. Total orders now: {result[0]}")
        except Exception as e:
            print(f"ERROR loading orders: {str(e)}")
            
            # Diagnostic information
            try:
                # Check if the order table exists
                table_check = self.conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='orders'").fetchone()
                if not table_check:
                    print("The orders table does not exist! Database schema issue.")
                    
                # Check dataframe structure
                if not df.is_empty():
                    print(f"Order dataframe columns: {df.columns}")
                    print(f"First row example: {df.head(1).to_dict(as_series=False)}")
                    
                # Check for customer references
                if 'customer_id' in df.columns:
                    customer_check = self.conn.execute("""
                    SELECT COUNT(*) FROM customers 
                    WHERE customer_id IN (SELECT DISTINCT customer_id FROM pandas_df)
                    """).fetchone()
                    print(f"Found {customer_check[0]} corresponding customers for these orders.")
            except Exception as diag_e:
                print(f"Diagnostic error: {diag_e}")
            
            raise
            
    def load_transactions(self, df: pl.DataFrame):
        """Load transaction data into the database."""
        try:
            # Check if dataframe is empty
            if df.is_empty():
                print("Warning: Empty transaction dataframe provided, nothing to load")
                return
                
            # Convert Polars DataFrame to pandas for DuckDB compatibility
            pandas_df = df.to_pandas()
            
            # Insert data into the transactions table using the sequence for transaction_id
            self.conn.execute("""
            INSERT INTO transactions (
                transaction_id, order_id, product, quantity, unit_price, total_amount
            )
            SELECT 
                nextval('transaction_id_seq'), order_id, product, quantity, unit_price, total_amount
            FROM pandas_df
            """)
            
            # Show the count of inserted records
            result = self.conn.execute("SELECT COUNT(*) FROM transactions").fetchone()
            print(f"Loaded transactions into the database. Total transactions now: {result[0]}")
        except Exception as e:
            print(f"ERROR loading transactions: {str(e)}")
            
            # Diagnostic information
            try:
                # Check if the order_ids exist
                if 'order_id' in df.columns:
                    order_check = self.conn.execute("""
                    SELECT COUNT(DISTINCT order_id) FROM orders 
                    WHERE order_id IN (SELECT DISTINCT order_id FROM pandas_df)
                    """).fetchone()
                    valid_orders = order_check[0] if order_check else 0
                    
                    # Get the count of distinct order_ids in the dataframe
                    order_ids_in_df = len(pandas_df['order_id'].unique())
                    
                    print(f"DataFrame has {order_ids_in_df} distinct order_ids")
                    print(f"Found {valid_orders} corresponding orders in the database out of {order_ids_in_df} in the dataframe.")
                    
                    if valid_orders < order_ids_in_df:
                        # Show some examples of missing order_ids
                        missing_orders_query = """
                        SELECT DISTINCT o.order_id FROM pandas_df o
                        LEFT JOIN orders db ON o.order_id = db.order_id
                        WHERE db.order_id IS NULL
                        LIMIT 5
                        """
                        missing_orders = self.conn.execute(missing_orders_query).fetchall()
                        if missing_orders:
                            print(f"Examples of missing order_ids: {[o[0] for o in missing_orders]}")
            except Exception as diag_e:
                print(f"Diagnostic error: {diag_e}")
            
            raise
    
    def verify_data(self):
        """Verify that the data has been properly loaded."""
        try:
            # Check customer count
            customer_count = self.conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
            print(f"Total customers: {customer_count}")
            
            # Check order count
            order_count = self.conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
            print(f"Total orders: {order_count}")
            
            # Check transaction count
            transaction_count = self.conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
            print(f"Total transactions: {transaction_count}")
            
            # Average transactions per order
            if order_count > 0:
                avg_trans_per_order = transaction_count / order_count
                print(f"Average transactions per order: {avg_trans_per_order:.2f}")
            
            # Check referential integrity customer-order
            integrity_check1 = self.conn.execute("""
            SELECT COUNT(*) FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE c.customer_id IS NULL
            """).fetchone()[0]
            
            if integrity_check1 == 0:
                print("Referential integrity verified: All orders have valid customer IDs.")
            else:
                print(f"Warning: Found {integrity_check1} orders with invalid customer IDs!")
            
            # Check referential integrity order-transaction
            integrity_check2 = self.conn.execute("""
            SELECT COUNT(*) FROM transactions t
            LEFT JOIN orders o ON t.order_id = o.order_id
            WHERE o.order_id IS NULL
            """).fetchone()[0]
            
            if integrity_check2 == 0:
                print("Referential integrity verified: All transactions have valid order IDs.")
            else:
                print(f"Warning: Found {integrity_check2} transactions with invalid order IDs!")
            
            # Sample data
            print("\nSample customers:")
            sample_customers = self.conn.execute("SELECT * FROM customers LIMIT 5").fetchall()
            for customer in sample_customers:
                print(customer)
            
            print("\nSample orders:")
            sample_orders = self.conn.execute("SELECT * FROM orders LIMIT 5").fetchall()
            for order in sample_orders:
                print(order)
            
            print("\nSample transactions:")
            sample_transactions = self.conn.execute("SELECT * FROM transactions LIMIT 5").fetchall()
            for transaction in sample_transactions:
                print(transaction)
        except Exception as e:
            print(f"Error verifying data: {e}")
    
    def close(self):
        """Close the database connection."""
        self.conn.close()

if __name__ == "__main__":
    # Test the database manager
    db_manager = DBManager()
    db_manager.verify_data()
    db_manager.close()