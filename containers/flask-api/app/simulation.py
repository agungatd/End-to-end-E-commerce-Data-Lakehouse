import requests
from faker import Faker
import random

BASE_URL = "http://localhost:5001/api"

# Faker setup
faker = Faker()


# Simulate creating users
def simulate_create_users(num_users):
    user_ids = []
    genders = ['M', 'F']
    for _ in range(num_users):
        user_data = {
            "name": faker.name(),
            "gender": random.choice(genders),
            "email": faker.email(),
            "phone": faker.msisdn(),
            "country": faker.country_code(),
            "registration_date": str(faker.date_time_between('-4y')),
            "acquisition_channel_id": random.randint(1, 12)
        }
        response = requests.post(f"{BASE_URL}/users", json=user_data)
        if response.status_code == 201:
            user_ids.append(response.json()['user_id'])
    return user_ids


# Simulate creating orders
def simulate_create_orders(user_ids):
    order_status = ["success", "cancelled"]
    pay_method = ["VA", "CC", "COD"]

    for user_id in user_ids:
        items = []
        num_items = random.randint(1, 42)
        
        # generate order items
        for _ in range(num_items):
            item = {}
            item['product_id'] = random.randint(1,10)
            item['quantity'] = random.randint(1,10)
            items.append(item)

        # Set order data payload
        order_data = {
            "customer_id": user_id,
            "order_date": str(faker.date_time_between('-4y')),
            "order_status": "success",
            "total_amount": random.uniform(20.0, 500.0),
            "payment_method": random.choice(pay_method),
            "items": items,
        }

        response = requests.post(f"{BASE_URL}/orders", json=order_data)
        if response.status_code == 201:
            order_id = response.json()['order_id']
            print(f'Orderid: {order_id} has been created')

# Main simulation
def main_simulation():
    # Create users
    user_ids = simulate_create_users(10)

    # Create orders and add products to cart
    simulate_create_orders(user_ids)

if __name__ == "__main__":
    main_simulation()
