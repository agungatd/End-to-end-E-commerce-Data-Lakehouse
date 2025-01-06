from pymongo import MongoClient

def main():
    # MongoDB connection
    client = MongoClient("mongodb://root:root@localhost:27017/")  # Replace with your MongoDB URI
    db = client["ecommerce"]  # Database name
    collection = db["user_activities"]  # Collection for storing user activities logs

    # # test insert
    # data = {"name": "John Doe", "age": 30, "city": "New York"}
    # collection.insert_one(data)

    # test find
    cursor = collection.find()
    print(list(cursor))

if __name__ == '__main__':
    # test api
    main()