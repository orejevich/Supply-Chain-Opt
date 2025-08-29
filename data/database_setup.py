import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import os
from datetime import datetime

class DatabaseSetup:
    def __init__(self, user=None):
        self.user = user or os.getenv('USER')
        self.host = 'localhost'
        self.database = 'supply_chain_optimizer'
        
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user
        )
    
    def drop_tables(self):
        """Drop existing tables"""
        conn = self.get_connection()
        cur = conn.cursor()
        tables = [
            "supply_events",
            "demand_history",
            "inventory",
            "warehouses",
            "suppliers",
            "products"
        ]

        for table in tables:
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE;")

        conn.commit()
        cur.close()
        conn.close()
        print("Existing tables dropped successfully!")
    
    def create_tables(self):
        """Create all necessary tables for the supply chain project"""
        
        conn = self.get_connection()
        cur = conn.cursor()
        
        # Products table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS products (
                product_id SERIAL PRIMARY KEY,
                sku VARCHAR(50) UNIQUE NOT NULL,
                name VARCHAR(200) NOT NULL,
                category VARCHAR(50) NOT NULL,
                unit_cost DECIMAL(10,2) NOT NULL,
                selling_price DECIMAL(10,2) NOT NULL,
                shelf_life_days INTEGER, -- for perishable goods
                financial_classification CHAR(1) CHECK (financial_classification IN ('A', 'B', 'C')), -- Revenue-based ABC
                operational_risk CHAR(1) CHECK (operational_risk IN ('A', 'B', 'C')), -- Risk-based classification
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Suppliers table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS suppliers (
                supplier_id SERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                location VARCHAR(100) NOT NULL,
                reliability_score DECIMAL(3,2) CHECK (reliability_score >= 0 AND reliability_score <= 1),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        cur.execute("""
            CREATE TABLE IF NOT EXISTS product_suppliers (
                product_supplier_id SERIAL PRIMARY KEY,
                product_id INTEGER REFERENCES products(product_id) UNIQUE, -- Only one supplier per product
                supplier_id INTEGER REFERENCES suppliers(supplier_id),
                supplier_sku VARCHAR(100),
                average_lead_time_days INTEGER NOT NULL,
                lead_time_std_dev DECIMAL(5,2) DEFAULT 0.0, -- Standard deviation of lead times
                lead_time_reliability_score DECIMAL(5,4), -- Percentage of on-time deliveries (0.0-1.0)
                worst_case_lead_time INTEGER, -- 95th percentile lead time
                best_case_lead_time INTEGER, -- 5th percentile lead time
                unit_cost DECIMAL(10,2) NOT NULL,
                last_delivery_performance INTEGER, -- actual days for last order
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Current inventory table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS inventory (
                inventory_id SERIAL PRIMARY KEY,
                product_id INTEGER REFERENCES products(product_id) UNIQUE,
                current_stock INTEGER NOT NULL CHECK (current_stock >= 0),
                committed_stock INTEGER DEFAULT 0, -- stock allocated to orders
                available_stock INTEGER GENERATED ALWAYS AS (current_stock - committed_stock) STORED,
                reorder_point INTEGER NOT NULL,
                days_of_supply DECIMAL(5,2), -- current_stock / average_daily_demand
                last_stockout_date DATE,
                last_reorder_date DATE,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
 
        # Purchase orders table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS purchase_orders (
                po_id SERIAL PRIMARY KEY,
                supplier_id INTEGER REFERENCES suppliers(supplier_id),
                po_number VARCHAR(50) UNIQUE,
                order_date DATE NOT NULL,
                expected_delivery_date DATE NOT NULL,
                actual_delivery_date DATE,
                total_cost DECIMAL(12,2),
                status VARCHAR(20) DEFAULT 'pending' CHECK (status IN ('pending', 'confirmed', 'shipped', 'delivered', 'cancelled')),
                delivery_performance_days INTEGER, -- actual vs expected (negative = early, positive = late)
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Order items
        cur.execute(""" 
            CREATE TABLE IF NOT EXISTS order_items (
                item_id SERIAL PRIMARY KEY,
                po_id INTEGER REFERENCES purchase_orders(po_id),
                product_id INTEGER REFERENCES products(product_id),
                quantity_ordered INTEGER NOT NULL,
                quantity_received INTEGER DEFAULT 0,
                unit_cost DECIMAL(10,2) NOT NULL,
                line_total DECIMAL(12,2) GENERATED ALWAYS AS (quantity_ordered * unit_cost) STORED
            );
        """)

        # Historical demand data
        cur.execute("""
            CREATE TABLE IF NOT EXISTS demand_history (
                demand_id SERIAL PRIMARY KEY,
                product_id INTEGER REFERENCES products(product_id),
                date DATE NOT NULL,
                forecasted_demand INTEGER,
                actual_demand INTEGER NOT NULL CHECK (actual_demand >= 0),
                stockout_quantity INTEGER DEFAULT 0, -- unmet demand due to stockout
                day_of_week INTEGER CHECK (day_of_week BETWEEN 1 AND 7),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(product_id, date)
            );
        """)
        
        # Supply chain events (deliveries, stockouts, etc.)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS supply_events (
                event_id SERIAL PRIMARY KEY,
                product_id INTEGER REFERENCES products(product_id),
                supplier_id INTEGER REFERENCES suppliers(supplier_id),
                event_type VARCHAR(50) NOT NULL, -- 'delivery', 'stockout', 'reorder', 'transfer'
                quantity INTEGER,
                event_date TIMESTAMP NOT NULL,
                cost DECIMAL(10,2),
                notes TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create indexes for better performance
        cur.execute("CREATE INDEX IF NOT EXISTS idx_inventory_product ON inventory(product_id);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_demand_product_date ON demand_history(product_id, date);")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_supply_events_date ON supply_events(event_date);")
        
        conn.commit()
        cur.close()
        conn.close()
        
        print("Database tables created successfully!")
    
    def check_connection(self):
        """Test database connection"""
        try:
            conn = self.get_connection()
            cur = conn.cursor()
            cur.execute("SELECT version();")
            version = cur.fetchone()
            print(f"Connected to PostgreSQL: {version[0]}")
            cur.close()
            conn.close()
            return True
        except Exception as e:
            print(f"Connection failed: {e}")
            return False

if __name__ == "__main__":
    # Initialize and create database schema
    db_setup = DatabaseSetup()
    
    if db_setup.check_connection():
        db_setup.drop_tables()
        db_setup.create_tables()
    else:
        print("Please check your PostgreSQL installation and database setup.")