"""
Module for generating synthetic transaction data.
"""

import random
import polars as pl
import numpy as np
from tqdm import tqdm
from typing import List, Dict, Any, Optional

from config import (
    PRODUCT_CATEGORIES, PRODUCT_PRICE_RANGES, PRODUCTS_BY_CATEGORY,
    INCOME_PER_CAPITA_FACTORS, TRANSACTION_BATCH_SIZE
)

# Import DBManager for batch processing
from db_manager import DBManager

# Initialize random seed
random.seed(42)
np.random.seed(42)

# Function to generate transactions for an order
def generate_transactions_for_order(
    order_info: Dict[str, Any],
    num_items: Optional[int] = None
) -> List[Dict[str, Any]]:
    """
    Generate a list of transaction records for an order.
    
    Parameters:
    - order_info: Dict containing order_id and customer country
    - num_items: Optional number of items, otherwise random between 1-10
    
    Returns:
    - List of transaction records
    """
    # Validate required fields
    if not order_info or not order_info.get("order_id"):
        print("Warning: Invalid order info, skipping transaction generation")
        return []
    
    # Get order ID
    order_id = order_info["order_id"]
    
    # Get customer country for price adjustment (if available)
    customer_country = order_info.get("country", "United States")
    
    # If num_items not specified, generate randomly between 1-10
    if num_items is None:
        num_items = random.randint(1, 10)
    
    transactions = []
    
    # Generate transactions for each item in the order
    for _ in range(num_items):
        try:
            # Select product category based on weights
            categories = list(PRODUCT_CATEGORIES.keys())
            category_weights = list(PRODUCT_CATEGORIES.values())
            
            # Normalize weights to ensure they sum to 1
            category_weights = np.asarray(category_weights).astype('float64')
            category_weights /= category_weights.sum()
            
            category = np.random.choice(categories, p=category_weights)
            
            # Select a product from the chosen category
            product = random.choice(PRODUCTS_BY_CATEGORY[category])
            
            # Generate quantity (typically 1-5 for most items)
            quantity = random.choices([1, 2, 3, 4, 5], weights=[0.5, 0.25, 0.15, 0.07, 0.03])[0]
            
            # Generate base unit price based on product category
            min_price, max_price = PRODUCT_PRICE_RANGES[category]
            base_unit_price = round(random.uniform(min_price, max_price), 2)
            
            # Apply price adjustment based on customer's country
            price_factor = INCOME_PER_CAPITA_FACTORS.get(customer_country, 1.0)
            unit_price = round(base_unit_price * price_factor, 2)
            
            # Calculate total amount
            total_amount = round(quantity * unit_price, 2)
            
            # Create transaction record (with order_id only)
            transaction = {
                "order_id": order_id,
                "product": product,
                "quantity": quantity,
                "unit_price": unit_price,
                "total_amount": total_amount
            }
            
            transactions.append(transaction)
        except Exception as e:
            print(f"Error generating transaction item: {e}")
            # Continue with next item
    
    return transactions

# Function to generate transaction dataset in batches
def generate_transaction_dataset_in_batches(
    orders_info: List[Dict[str, Any]],
    batch_size: int = TRANSACTION_BATCH_SIZE
) -> None:
    """
    Generate transaction data in batches for all orders and store in database.
    
    Parameters:
    - orders_info: List of order information dictionaries
    - batch_size: Size of transaction batches for database loading
    """
    # Initialize DB manager
    db_manager = DBManager()
    
    # Check if we have orders
    if not orders_info:
        print("Error: No orders available to generate transactions for.")
        db_manager.close()
        return
    
    # All transactions for batch processing
    all_transactions = []
    total_transactions = 0
    
    # Estimate total transactions (assuming average of 5.5 items per order)
    estimated_total = len(orders_info) * 5.5
    
    print(f"Generating transactions for {len(orders_info)} orders...")
    
    # Generate transactions for each order
    with tqdm(total=estimated_total, desc="Generating transaction data") as pbar:
        for order_info in orders_info:
            # Generate random number of items for this order (1-10)
            num_items = random.randint(1, 10)
            
            # Generate transactions for this order
            order_transactions = generate_transactions_for_order(order_info, num_items)
            
            # Add to batch
            all_transactions.extend(order_transactions)
            total_transactions += len(order_transactions)
            
            # Update progress bar
            pbar.update(len(order_transactions))
            
            # If we've collected enough transactions, load a batch to the database
            if len(all_transactions) >= batch_size:
                try:
                    # Take only up to batch_size
                    current_batch = all_transactions[:batch_size]
                    all_transactions = all_transactions[batch_size:]
                    
                    # Convert to DataFrame and load
                    batch_df = pl.DataFrame(current_batch)
                    db_manager.load_transactions(batch_df)
                except Exception as e:
                    print(f"Error loading transaction batch: {e}")
    
    # Load any remaining transactions
    if all_transactions:
        try:
            batch_df = pl.DataFrame(all_transactions)
            db_manager.load_transactions(batch_df)
        except Exception as e:
            print(f"Error loading final transaction batch: {e}")
    
    print(f"Generated {total_transactions} transactions for {len(orders_info)} orders.")
    
    # Close the DB connection
    db_manager.close()
    
    # Verify data was loaded
    verify_db = DBManager()
    transaction_count = verify_db.conn.execute("SELECT COUNT(*) FROM transactions").fetchone()[0]
    print(f"Verification: {transaction_count} transactions in database.")
    verify_db.close()

if __name__ == "__main__":
    # Test the transaction generator
    test_orders = [
        {"order_id": f"ORD-{i}", "country": "United States"}
        for i in range(10)
    ]
    generate_transaction_dataset_in_batches(test_orders, 50)