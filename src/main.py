"""
Main script to generate synthetic retail data and load it into DuckDB.
"""

import os
import time
import argparse

from customer_generator import generate_customer_dataset_in_batches
from order_generator import generate_order_dataset_in_batches
from transaction_generator import generate_transaction_dataset_in_batches
from db_manager import DBManager
from config import (
    NUM_CUSTOMERS, NUM_ORDERS, DB_FILE, 
    CUSTOMER_BATCH_SIZE, ORDER_BATCH_SIZE, TRANSACTION_BATCH_SIZE
)

def main():
    """Main function to generate data and load it into DuckDB."""
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description='Generate synthetic retail data and load into DuckDB')
    parser.add_argument('--customers', type=int, default=NUM_CUSTOMERS, 
                        help=f'Number of customer records to generate (default: {NUM_CUSTOMERS:,})')
    parser.add_argument('--orders', type=int, default=NUM_ORDERS, 
                        help=f'Number of order records to generate (default: {NUM_ORDERS:,})')
    parser.add_argument('--customer-batch', type=int, default=CUSTOMER_BATCH_SIZE, 
                        help=f'Batch size for customer generation (default: {CUSTOMER_BATCH_SIZE:,})')
    parser.add_argument('--order-batch', type=int, default=ORDER_BATCH_SIZE, 
                        help=f'Batch size for order generation (default: {ORDER_BATCH_SIZE:,})')
    parser.add_argument('--transaction-batch', type=int, default=TRANSACTION_BATCH_SIZE, 
                        help=f'Batch size for transaction generation (default: {TRANSACTION_BATCH_SIZE:,})')
    
    args = parser.parse_args()
    
    start_time = time.time()
    
    print("Starting synthetic retail data generation process...")
    print(f"Target: {args.customers:,} customers, {args.orders:,} orders")
    print(f"Batch sizes: customers={args.customer_batch:,}, orders={args.order_batch:,}, transactions={args.transaction_batch:,}")
    print(f"Database file: {DB_FILE}")
    
    # Step 1: Generate customer data
    print("\nStep 1: Generating customer data...")
    customer_info_list = generate_customer_dataset_in_batches(
        num_customers=args.customers, 
        batch_size=args.customer_batch
    )
    print(f"Generated {len(customer_info_list):,} customer records.")
    
    # Check customers were generated
    if not customer_info_list:
        print("ERROR: No customers were generated. Cannot proceed with order generation.")
        return
    
    # Step 2: Generate order data
    print("\nStep 2: Generating order data...")
    
    # Check database status before generating orders
    db_check = DBManager()
    customer_count = db_check.conn.execute("SELECT COUNT(*) FROM customers").fetchone()[0]
    print(f"Database contains {customer_count} customers before order generation.")
    db_check.close()
    
    order_info_list = generate_order_dataset_in_batches(
        customer_info_list=customer_info_list,
        num_orders=args.orders,
        batch_size=args.order_batch
    )
    print(f"Generated {len(order_info_list):,} order records.")
    
    # Check orders were generated
    if not order_info_list:
        print("ERROR: No orders were generated. Cannot proceed with transaction generation.")
        return
    
    # Step 3: Generate transaction data
    print("\nStep 3: Generating transaction data...")
    generate_transaction_dataset_in_batches(
        orders_info=order_info_list,
        batch_size=args.transaction_batch
    )
    
    # Verify the data
    print("\nStep 4: Verifying data integrity...")
    db_manager = DBManager()
    db_manager.verify_data()
    db_manager.close()
    
    # Report completion time
    elapsed_time = time.time() - start_time
    hours, remainder = divmod(elapsed_time, 3600)
    minutes, seconds = divmod(remainder, 60)
    time_str = f"{int(hours)}h {int(minutes)}m {seconds:.2f}s" if hours > 0 else f"{int(minutes)}m {seconds:.2f}s"
    
    print(f"\nData generation completed in {time_str}.")
    print(f"Database file location: {os.path.abspath(DB_FILE)}")

if __name__ == "__main__":
    main()