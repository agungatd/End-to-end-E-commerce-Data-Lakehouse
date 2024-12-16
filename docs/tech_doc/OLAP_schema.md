# Postgres RDBMS Schema
See [E-commerce Postgres ERD](E-com_postgres_schema.md)
# OLAP Schema
To design an **OLAP schema** based on your ERD, we need to transform the relational schema into a star or snowflake schema suitable for analytical purposes. This involves identifying **fact tables** for measurable events and **dimension tables** to provide descriptive context for the facts.

### **Proposed OLAP Schema**

#### **Fact Tables**
1. **Fact_Orders**  
   - This table captures measurable details about customer purchases.  
   - **Measures**:  
     - `total_amount`  
     - `quantity` (from Order Items, as an aggregate)  
     - `order_count` (derived as a count of orders)  

   **Schema**:  
   ```
   Fact_Orders:
   - order_id (Primary Key)
   - customer_id (Foreign Key to Dim_Customers)
   - order_date_id (Foreign Key to Dim_Date)
   - payment_method
   - total_amount
   - order_status
   ```

2. **Fact_Order_Items**  
   - Tracks granular details about items in each order.  
   - **Measures**:  
     - `quantity`  
     - `price`  

   **Schema**:  
   ```
   Fact_Order_Items:
   - order_item_id (Primary Key)
   - order_id (Foreign Key to Fact_Orders)
   - product_id (Foreign Key to Dim_Products)
   - quantity
   - price
   ```

3. **Fact_Inventory**  
   - Logs inventory status for stock analysis.  
   - **Measures**:  
     - `quantity`  

   **Schema**:  
   ```
   Fact_Inventory:
   - inventory_id (Primary Key)
   - product_id (Foreign Key to Dim_Products)
   - warehouse_location
   - quantity
   ```

#### **Dimension Tables**
1. **Dim_Customers**  
   - Describes customers.  
   **Schema**:  
   ```
   Dim_Customers:
   - customer_id (Primary Key)
   - name
   - email
   - phone
   - address
   - registration_date
   - acquisition_channel_id (Foreign Key to Dim_Acquisition_Channels)
   ```

2. **Dim_Acquisition_Channels**  
   - Provides details about customer acquisition channels.  
   **Schema**:  
   ```
   Dim_Acquisition_Channels:
   - channel_id (Primary Key)
   - channel_name
   - description
   ```

3. **Dim_Products**  
   - Describes products.  
   **Schema**:  
   ```
   Dim_Products:
   - product_id (Primary Key)
   - name
   - description
   - price
   - category_id (Foreign Key to Dim_Product_Categories)
   - created_at
   - updated_at
   ```

4. **Dim_Product_Categories**  
   - Details product categories (may be a snowflake dimension).  
   **Schema**:  
   ```
   Dim_Product_Categories:
   - category_id (Primary Key)
   - category_name
   - parent_category_id (Self-referencing Foreign Key)
   ```

5. **Dim_Date**  
   - Tracks dates for temporal analysis.  
   **Schema**:  
   ```
   Dim_Date:
   - date_id (Primary Key)
   - calendar_date
   - year
   - quarter
   - month
   - week
   - day
   ```

6. **Dim_Product_Availability** (optional)  
   - Tracks stock statuses for products over time.  
   **Schema**:  
   ```
   Dim_Product_Availability:
   - log_id (Primary Key)
   - product_id (Foreign Key to Dim_Products)
   - status
   - timestamp
   ```

---

### **Star Schema Example**
Here’s a simplified **Fact_Orders** star schema:

**Fact_Orders**  
- Links to dimensions:  
  - `Dim_Customers`  
  - `Dim_Date`  
  - `Dim_Acquisition_Channels`  
  - `Dim_Products`  

This design provides a denormalized, highly efficient schema for analytical querying, supporting measures like sales performance, customer acquisition trends, and inventory analysis.