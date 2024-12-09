import requests
from faker import Faker
import random

BASE_URL = "http://localhost:5000/api"

# Faker setup
faker = Faker()

# Simulate creating users
def simulate_create_users(num_users):
    user_ids = []
    for _ in range(num_users):
        user_data = {
            "name": faker.name(),
            "email": faker.email()
        }
        response = requests.post(f"{BASE_URL}/users", json=user_data)
        if response.status_code == 201:
            user_ids.append(response.json()['user_id'])
    return user_ids

# Simulate adding products to cart
def simulate_add_to_cart(order_id, product_ids):
    for product_id in random.sample(product_ids, k=random.randint(1, 3)):
        cart_data = {
            "order_id": order_id,
            "product_id": product_id,
            "quantity": random.randint(1, 5)
        }
        requests.post(f"{BASE_URL}/cart", json=cart_data)

# Simulate creating orders
def simulate_create_orders(user_ids, product_ids):
    for user_id in user_ids:
        order_data = {
            "customer_id": user_id,
            "total_price": random.uniform(20.0, 500.0)
        }
        response = requests.post(f"{BASE_URL}/orders", json=order_data)
        if response.status_code == 201:
            order_id = response.json()['order_id']
            simulate_add_to_cart(order_id, product_ids)

# Main simulation
def main_simulation():
    # Generate fake product IDs (e.g., from 1 to 100)
    product_ids = list(range(1, 101))

    # Create users
    user_ids = simulate_create_users(10)

    # Create orders and add products to cart
    simulate_create_orders(user_ids, product_ids)

if __name__ == "__main__":
    main_simulation()
