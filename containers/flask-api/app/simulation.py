import os
from datetime import datetime, timedelta
import requests
from faker import Faker
import random
from pymongo import MongoClient

BASE_URL = "http://localhost:5001/api"
MONGO_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'root')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'root')
MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@localhost:27017/"

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
def order_simulation():
    # Create users
    user_ids = simulate_create_users(10)

    # Create orders and add products to cart
    simulate_create_orders(user_ids)

def user_activity():
    pass

def user_activity_simulation():
    print('user_activity_simulation start')
    # insert data to mongodb
    # activities = [
    #     'SIGN_UP',
    #     'LOG_IN',
    #     'SEARCH_PRODUCT',
    #     'SELECT_PRODUCT',
    #     'VIEW_PRODUCT_DETAILS',
    #     'VIEW_PRODUCT_REVIEWS',
    #     'SELECT_PRODUCT_VARIANT',
    #     'ADD_PRODUCT_TO_CART',
    #     'REMOVE_PRODUCT_FROM_CART',
    #     'ADD_VOUCHER',
    #     'SELECT_PAYMENT_TYPE',
    #     'SELECT_ADDRESS',
    #     'SELECT_DELIVERY_SERVICE',
    #     'CANCELLED_PAYMENT',
    #     'PRODUCT_PURCHASED',
    #     'PAYMENT_SUCCESS',
    #     'SELECT_RECOMMENDED_PRODUCT',
    # ]
    payment_methods = ["VA","CC","COD"]
    user_id = random.randint(1, 10)
    ts = datetime.now()
    login = {
        "user_id": user_id,
        "activity_type": 'LOG_IN',
        "details": {"device_id": 123321},
        "timestamp": ts.isoformat()
    }
    ts = ts + timedelta(seconds=random.randint(1,5))
    search = {
        "user_id": user_id,
        "activity_type": 'SEARCH_PRODUCT',
        "details": {"search": "abc"},
        "timestamp": ts.isoformat()
    }
    ts = ts + timedelta(seconds=random.randint(1,5))
    select_prd = {
        "user_id": user_id,
        "activity_type": 'SELECT_PRODUCT',
        "details": {"product_id": 1},
        "timestamp": ts.isoformat()
    }
    ts = ts + timedelta(seconds=random.randint(1,5))
    add_to_cart = {
        "user_id": user_id,
        "activity_type": 'ADD_PRODUCT_TO_CART',
        "details": {"product_id": 1, "quantity": 3},
        "timestamp": ts.isoformat()
    }
    ts = ts + timedelta(seconds=random.randint(1,5))
    view_cart = {
        "user_id": user_id,
        "activity_type": 'VIEW_CART',
        "details": {},
        "timestamp": ts.isoformat()
    }
    ts = ts + timedelta(seconds=random.randint(1,5))
    select_payment = {
        "user_id": user_id,
        "activity_type": 'SELECT_PAYMENT',
        "details": {"payment_method": random.choice(payment_methods)},
        "timestamp": ts.isoformat()
    }
    ts = ts + timedelta(seconds=random.randint(1,5))
    place_order = {
        "user_id": user_id,
        "activity_type": 'PLACE_ORDER',
        "details": {"payment_status": "pending"},
        "timestamp": ts.isoformat()
    }
    ts = ts + timedelta(seconds=random.randint(1,5))
    order_success = {
        "user_id": user_id,
        "activity_type": 'ORDER_SUCCESS',
        "details": {"payment_status": "success"},
        "timestamp": ts.isoformat()
    }
    activities = [
        login,
        search,
        select_prd,
        add_to_cart,
        view_cart,
        select_payment,
        place_order,
        order_success
    ]
    for activity_log in activities:
        api_endpoint = f"{BASE_URL}/user/activity"
        response = requests.post(api_endpoint, json=activity_log)
        if response.status_code == 201:
            print(f'Success logged user activity: {response.json()}')

    print('user_activity_simulation end')


if __name__ == "__main__":
    # order_simulation()
    print('start')
    user_activity_simulation()
