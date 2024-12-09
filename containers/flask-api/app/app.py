from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

@app.route("/")
def hello_world():
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

# API: Create New User
@app.route('/api/users', methods=['POST'])
def create_user():
    data = request.json
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO customers (customer_id, name, gender, email, phone, country, registration_date, acquisition_channel_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING customer_id;",
            (data['customer_id'], data['name'], data['gender'], data['email'], data['phone'], data['country'], data['registration_date'], data['acquisition_channel_id'])
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

# API: Create Order
@app.route('/api/orders', methods=['POST'])
def create_order():
    data = request.json
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO orders (customer_id, status, total_price) VALUES (%s, %s, %s) RETURNING id;",
            (data['customer_id'], 'Pending', data['total_price'])
        )
        order_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        return jsonify({'message': 'Order created', 'order_id': order_id}), 201
    except Exception as e:
        return jsonify({'error': str(e)}), 400

# Get customers
@app.route('/api/users', methods=['GET'])
def get_all_customers():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM customers"
        )
        columns = list(cursor.description)
        list_row = cursor.fetchall()

        # make dict
        data = []
        for row in list_row:
            row_dict = {}
            for i, col in enumerate(columns):
                row_dict[col.name] = row[i]
            data.append(row_dict)

        cursor.close()
        conn.close()
        return jsonify({'message': 'Success', 'data': data}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 400


if __name__ == '__main__':
    # test api
    app.run(host='0.0.0.0', port=5000, debug=True)
