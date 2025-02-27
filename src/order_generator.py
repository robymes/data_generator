"""
Module for generating synthetic order data.
"""

import random
import string
import polars as pl
import numpy as np
from faker import Faker
from datetime import timedelta
from tqdm import tqdm
from typing import List, Dict, Any
from dateutil.parser import parse

from config import (
    NUM_ORDERS, TRANSACTION_START_DATE, TRANSACTION_END_DATE,
    TRANSACTION_SOURCES, ORDER_BATCH_SIZE
)

# Import DBManager for batch processing
from db_manager import DBManager

# Initialize faker and random seeds
fake = Faker()
Faker.seed(42)  # For reproducibility
random.seed(42)
np.random.seed(42)

# Parse date strings to datetime objects
start_date = parse(TRANSACTION_START_DATE).date()
end_date = parse(TRANSACTION_END_DATE).date()
date_range = (end_date - start_date).days

# Function to generate order ID
def generate_order_id() -> str:
    """Generate a unique order ID."""
    return 'ORD-' + ''.join(random.choices(string.ascii_uppercase + string.digits, k=10))

# Function to generate a single order
def generate_order(customer_info: Dict[str, Any]) -> Dict[str, Any]:
    """
    Generate a single order record.
    
    Parameters:
    - customer_info: Dict containing customer_id and other info
    
    Returns:
    - Order record dict
    """
    # Ensure we have a valid customer_id
    if not customer_info or not customer_info.get("customer_id"):
        raise ValueError("Invalid customer info provided to generate_order")
    
    # Get customer ID
    customer_id = customer_info["customer_id"]
    
    # Generate order ID
    order_id = generate_order_id()
    
    # Determine the source channel for this order (50% e-commerce, 50% POS)
    # Try to use the same source as the customer if possible (70% probability)
    if random.random() < 0.7 and "source_id" in customer_info:
        source_id = customer_info["source_id"]
    else:
        source_options = list(TRANSACTION_SOURCES.keys())
        source_weights = list(TRANSACTION_SOURCES.values())
        source_id = random.choices(source_options, weights=source_weights)[0]
    
    # Generate order date
    random_days = random.randint(0, date_range)
    order_date = start_date + timedelta(days=random_days)
    date_str = order_date.strftime("%Y-%m-%d")
    
    # Create and return order record
    return {
        "order_id": order_id,
        "customer_id": customer_id,
        "source_id": source_id,
        "date": date_str
    }

# Function to generate order dataset in batches
def generate_order_dataset_in_batches(
    customer_info_list: List[Dict[str, Any]], 
    num_orders: int = NUM_ORDERS,
    batch_size: int = ORDER_BATCH_SIZE
) -> List[Dict[str, Any]]:
    """
    Generate the order dataset in batches and store in database.
    Returns a list of all generated orders for transaction generation.
    """
    # Initialize DB manager
    db_manager = DBManager()
    
    # Check if we have customers
    if not customer_info_list:
        print("Error: No customers available to generate orders for.")
        db_manager.close()
        return []
    
    print(f"Starting order generation with {len(customer_info_list)} customers...")
    
    # Keep track of generated orders for transaction creation
    all_orders_info = []
    
    # Calculate number of batches
    num_batches = max(1, (num_orders + batch_size - 1) // batch_size)
    
    print(f"Will generate {num_orders} orders in {num_batches} batches of size {batch_size}...")
    
    try:
        # Generate and store orders in batches
        for batch_idx in tqdm(range(num_batches), desc="Generating order data"):
            # Calculate batch size (last batch may be smaller)
            current_batch_size = min(batch_size, num_orders - batch_idx * batch_size)
            
            # Ensure we generate at least one order if needed
            if current_batch_size <= 0:
                continue
            
            print(f"Generating batch {batch_idx+1}/{num_batches} with {current_batch_size} orders...")
            
            # Generate batch of orders
            orders_batch = []
            for _ in range(current_batch_size):
                try:
                    # Select a random customer
                    customer_info = random.choice(customer_info_list)
                    
                    # Generate order
                    order = generate_order(customer_info)
                    
                    # Add to batch
                    orders_batch.append(order)
                    
                    # Store for transaction generation
                    order_with_country = order.copy()
                    if "country" in customer_info:
                        order_with_country["country"] = customer_info["country"]
                    all_orders_info.append(order_with_country)
                    
                except Exception as e:
                    print(f"Error generating order: {e}")
                    continue
            
            # Convert batch to DataFrame and load to database
            if orders_batch:
                try:
                    orders_df = pl.DataFrame(orders_batch)
                    db_manager.load_orders(orders_df)
                    print(f"Loaded {len(orders_batch)} orders to database.")
                except Exception as e:
                    print(f"Error loading order batch: {e}")
    except Exception as e:
        print(f"Critical error in order generation: {e}")
    
    # Close the DB connection
    db_manager.close()
    
    print(f"Generated {len(all_orders_info)} orders.")
    return all_orders_info

if __name__ == "__main__":
    # Test the order generator
    fake_customers = [
        {"customer_id": f"CUST{i}", "country": "United States", "source_id": 1}
        for i in range(10)
    ]
    orders = generate_order_dataset_in_batches(fake_customers, 20, 10)
    print(f"Generated {len(orders)} test orders")