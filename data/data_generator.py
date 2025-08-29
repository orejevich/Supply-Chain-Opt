import psycopg2
from faker import Faker
import random
from datetime import datetime, timedelta

class DataPopulator:
    def __init__(self, user=None, host='localhost', database='supply_chain_optimizer'):
        import os
        self.user = user or os.getenv('USER')
        self.host = host
        self.database = database
        self.fake = Faker()

    def get_connection(self):
        return psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user
        )

    def generate_products(self, n=20):
        categories = ['Electronics', 'Food', 'Clothing', 'Pharma']
        fin_classes = ['A', 'B', 'C']
        op_risks = ['A', 'B', 'C']
        return [
            (
                f"SKU{1000+i}",
                self.fake.word().capitalize(),
                random.choice(categories),
                round(random.uniform(5.0, 100.0), 2),
                round(random.uniform(110.0, 250.0), 2),
                random.randint(30, 365),
                random.choice(fin_classes),
                random.choice(op_risks)
            ) for i in range(n)
        ]

    def generate_suppliers(self, n=10):
        return [
            (
                self.fake.company(),
                self.fake.city(),
                round(random.uniform(0.7, 0.99), 2)
            ) for _ in range(n)
        ]

    def generate_demand_history(self, cur, product_ids, days=180):
        today = datetime.now().date()
        start_date = today - timedelta(days=days)

        demand_rows = []
        for pid in product_ids:
            actual_demands = []
            for i in range(days):
                date = start_date + timedelta(days=i)
                actual_demand = random.randint(5, 20)
                actual_demands.append(actual_demand)

                if i >= 90:
                    forecasted_demand = round(sum(actual_demands[i-90:i]) / 90)
                else:
                    forecasted_demand = actual_demand  # Bootstrap early days

                # Add discrepancy
                forecasted_demand += random.choice([-3, -2, -1, 0, 1, 2, 3])
                forecasted_demand = max(0, forecasted_demand)

                day_of_week = date.isoweekday()
                stockout_quantity = random.randint(0, 2) if actual_demand > forecasted_demand else 0

                demand_rows.append((pid, date, forecasted_demand, actual_demand, stockout_quantity, day_of_week))

        cur.executemany("""
            INSERT INTO demand_history (product_id, date, forecasted_demand, actual_demand, stockout_quantity, day_of_week)
            VALUES (%s, %s, %s, %s, %s, %s);
        """, demand_rows)

    def populate(self):
        conn = self.get_connection()
        cur = conn.cursor()

        # Insert into products
        products = self.generate_products()
        cur.executemany("""
            INSERT INTO products (sku, name, category, unit_cost, selling_price, shelf_life_days, financial_classification, operational_risk)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, products)
        
        # Insert into suppliers
        suppliers = self.generate_suppliers()
        cur.executemany("""
            INSERT INTO suppliers (name, location, reliability_score)
            VALUES (%s, %s, %s)
        """, suppliers)

        # Link each product with a supplier
        cur.execute("SELECT product_id FROM products;")
        product_ids = [row[0] for row in cur.fetchall()]
        cur.execute("SELECT supplier_id FROM suppliers;")
        supplier_ids = [row[0] for row in cur.fetchall()]

        product_suppliers = [
            (
                pid,
                random.choice(supplier_ids),
                f"SUP-{pid}",
                random.randint(5, 20),
                round(random.uniform(1, 4), 2),
                round(random.uniform(0.7, 1.0), 4),
                random.randint(15, 30),
                random.randint(3, 10),
                round(random.uniform(5, 50), 2),
                random.randint(4, 25)
            ) for pid in product_ids
        ]
        cur.executemany("""
            INSERT INTO product_suppliers (product_id, supplier_id, supplier_sku, average_lead_time_days,
            lead_time_std_dev, lead_time_reliability_score, worst_case_lead_time, best_case_lead_time,
            unit_cost, last_delivery_performance)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, product_suppliers)

        # Inventory
        inventory = [
            (
                pid,
                random.randint(100, 500),
                random.randint(0, 100),
                random.randint(50, 150),
                round(random.uniform(5.0, 30.0), 2),
                datetime.now().date() - timedelta(days=random.randint(5, 60)),
                datetime.now().date() - timedelta(days=random.randint(0, 30))
            ) for pid in product_ids
        ]
        cur.executemany("""
            INSERT INTO inventory (product_id, current_stock, committed_stock, reorder_point,
            days_of_supply, last_stockout_date, last_reorder_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """, inventory)

        # Demand history
        self.generate_demand_history(cur, product_ids, days=180)

        conn.commit()
        cur.close()
        conn.close()
        print("Synthetic data population complete!")

if __name__ == "__main__":
    populator = DataPopulator()
    populator.populate()
