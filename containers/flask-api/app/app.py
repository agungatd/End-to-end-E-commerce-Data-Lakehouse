import os
from datetime import datetime
from flask import Flask, request, jsonify
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from pymongo import MongoClient

BASE_URL = "http://localhost:5001/api"
MONGO_USERNAME = os.getenv('MONGO_INITDB_ROOT_USERNAME', 'root')
MONGO_PASSWORD = os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'root')
MONGO_URI = f"mongodb://{MONGO_USERNAME}:{MONGO_PASSWORD}@mongo:27017/"

app = Flask(__name__)

@app.route("/")
def home():
    return "<h1>The E-commerce dummy API is running!</h1>"

# Database connection
def get_db_connection():
    return psycopg2.connect(
        dbname="ecommerce",
        user="postgres",
        password="postgres",
        host="postgres",
        port="5432"
    )

# Get list of dict from cursor fetchall data
def cur_dict(cursor):
    columns = list(cursor.description)
    list_row = cursor.fetchall()

    # make dict
    data = []
    for row in list_row:
        row_dict = {}
        for i, col in enumerate(columns):
            row_dict[col.name] = row[i]
        data.append(row_dict)
    return data

def get_product(conn, product_id):
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM products WHERE product_id={product_id}")
    data = cur_dict(cursor)
    cursor.close()
    return data

# API: Create New User
@app.route('/api/users', methods=['POST'])
def create_user():
    data = request.json
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT INTO customers (customer_id, name, gender, email, phone, country, registration_date, acquisition_channel_id) 
            VALUES (DEFAULT, %s, %s, %s, %s, %s, %s, %s) 
            RETURNING customer_id;
            """,(
                data['name'],
                data['gender'],
                data['email'],
                data['phone'],
                data['country'],
                data['registration_date'],
                data['acquisition_channel_id']
            )
        )
        user_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'message': 'User created', 'user_id': user_id}), 201
    except Exception as e:
        return jsonify({'create_user error': str(e)}), 400

# API: Add Products to Cart
@app.route('/api/cart', methods=['POST'])
def add_to_cart():
    data = request.json
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO order_items (order_id, product_id, quantity) VALUES (%s, %s, %s);",
            (data['order_id'], data['product_id'], data['quantity'])
        )
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'message': 'Product added to cart'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route('/api/order_item', methods=['POST'])
def create_order_item():
    item = request.json
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO order_items (order_id, product_id, quantity, price) 
            VALUES (%s, %s, %s, %s);""",(
                item['order_id'],
                item['product_id'],
                item['quantity'],
                item['price'],
            )
        )
        conn.commit()
        cursor.close()
        return jsonify({'message': 'order item inserted'}), 201
    except Exception as e:
        return jsonify({'create_order_item error': str(e)}), 400

@app.route('/api/orders', methods=['PUT']) 
def update_order():
    payload = request.json
    col_set = payload['col_set']
    col_filter = payload['col_filter']
    col_set_key, col_set_val = list(col_set.items())[0]
    col_filter_key, col_filter_val = list(col_filter.items())[0]
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            f"""
            UPDATE orders
            SET {col_set_key} = {col_set_val}
            WHERE {col_filter_key} = {col_filter_val}
            """
        )
        conn.commit()
        cursor.close()
        return jsonify({'message': 'Order total amount updated'}), 201
    except Exception as e:
        return jsonify({'update_order error': str(e)}), 400

# API: Create Order
@app.route('/api/orders', methods=['POST'])
def create_order():
    order = request.json

    # Calculate total amount
    total_amount = 0
    order_items = []

    try:
        # Insert order first to get order_id
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            """INSERT INTO orders (customer_id, order_date, order_status, total_amount, payment_method) 
            VALUES (%s, %s, %s, %s, %s) 
            RETURNING order_id;""",(
                order['customer_id'],
                order['order_date'],
                order['order_status'],
                order['total_amount'],
                order['payment_method'],
            )
        )
        order_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        for item_data in order['items']:
            product = get_product(conn, item_data['product_id'])
            qty = item_data['quantity']
            # if not product or product.stock < item_data['quantity']:
            #     return jsonify({'error': f'Insufficient stock for product {item_data["product_id"]}'}), 400
            
            subtotal = product['price'] * qty
            total_amount += subtotal
            
            # Reduce product stock
            # product.stock -= qty
            
            order_item = {
                "order_id": order_id,
                "product_id": product['product_id'],
                "quantity": qty,
                "price": subtotal,
            }
            response = requests.post(f"{BASE_URL}/order_item", json=order_item)

        update_order_payload = {
            "col_filter": {"order_id": order_id}, 
            "col_set": {"total_amount": total_amount}
        }
        requests.put(f"{BASE_URL}/orders", json=update_order_payload)

        return jsonify({'message': 'Order created', 'order_id': order_id}), 201
    except Exception as e:
        return jsonify({'create_order error': str(e)}), 400

# Get customers
@app.route('/api/users', methods=['GET'])
def get_all_customers():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM customers"
        )
        data = cur_dict(cursor)

        cursor.close()
        conn.close()
        return jsonify({'message': 'Success', 'data': data}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# User Activity
@app.route('/api/user/activity', methods=['POST'])
def insert_user_activity():
    try:
        # MongoDB connection
        client = MongoClient(MONGO_URI)
        db = client["ecommerce"]  # Database name
        user_activities_coll = db["user_activities"]  # Collection for storing user activities logs

        # Get data from the request
        data = request.json
        user_id = data.get('user_id')
        activity_type = data.get('activity_type')
        details = data.get('details', {})
        timestamp = data.get('timestamp', datetime.now().isoformat())

        # Validate the data
        if not user_id or not activity_type:
            return jsonify({"error": "user_id and activity_type are required"}), 400

        log_entry = {
            "user_id": user_id,
            "activity_type": activity_type,
            "details": details,
            "timestamp": timestamp
        }
        print(f'log: {log_entry}')
        # Insert the document into MongoDB
        result = user_activities_coll.insert_one(log_entry)

        # Return success response
        return jsonify({
            "message": "User activity logged successfully",
            "result": result
        }), 201
    except Exception as e:
        return jsonify({'insert user activity error': str(e)}), 400


if __name__ == '__main__':
    # test api
    app.run(host='0.0.0.0', port=5000, debug=True)
