import pandas as pd 

data = {
    'Std_id': [101, 102, 103, 104, 105],
    'Student': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
    "Maths": [85, 90, 78, 92, 88],
    "Science": [80, 85, 82, 88, 90],
    "English": [78, 82, 85, 90, 87]
}   

df = pd.DataFrame(data) 
print("Student Marks Data:")
print(df)

data = {
    "Order_id": [1, 2, 3, 4, 5,6],
    "Region": ['North', 'South', 'East', 'West', 'North', 'South'],
    "Category": ["Electronics", "Clothing", "Home", "Electronics", "Clothing", "Home"],
    "Product": ["Laptop", "Shirt", "Sofa", "Smartphone", "Jeans", "Table"],
    "Sales": [1000, 1500, 1200, 2000, 1800, 1300],
    "Quantity": [10, 15, 12, 20, 18, 13]
}

import numpy as np
# Create a Dataframe with few columns as missing data 
data_with_missing = {
    "EmpID": [1, 2, 3, 4, 5],
    "Name": ['John', 'Jane', 'Doe', 'Alice', 'Bob'],
    "Department": ['HR', 'Finance', np.nan, 'IT', 'Marketing'],
    "Salary": [50000, 60000, 55000, np.nan, 65000],
    "Experience": [5, 7, np.nan, 4, 6]
}

# Implement merge 
employees_df = pd.DataFrame({
    "emp_id": [1, 2, 3, 4, 5],
    "name": ['John', 'Jane', 'Doe', 'Alice', 'Bob'],
    "dept_id": [101, 102, 103, 104, 105],
    "salary": [50000, 60000, 55000, 70000, 65000]
})

departments_df = pd.DataFrame({
    "dept_id": [101, 102, 103, 104, 105],
    "department": ['HR', 'Finance', 'IT', 'Marketing', 'Sales']    
})


order_df = pd.DataFrame({"order_id": [1, 2, 3, 4, 5],
                         "customer_id": [101, 102, 103, 104, 105],
                         "order_amount": [1000, 1500, 1200, 2000, 1800]})
customer_df = pd.DataFrame({"customer_id": [101, 102, 103, 104, 105],
                            "customer_name": ['Alice', 'Bob', 'Charlie', 'David', 'Eve']})



sales_df = pd.DataFrame({
    "product_name": ["Laptop", "Shirt", "Sofa", "Smartphone", "Jeans"],
    "Region": ['North', 'South', 'East', 'West', 'North'],
    "Year": [2020, 2020, 2021, 2021, 2022],
    "Sales": [1000, 1500, 1200, 2000, 1800]
})

targer_sales_df = pd.DataFrame({
    "product_name": ["Laptop", "Shirt", "Sofa", "Smartphone", "Jeans"],
    "Region": ['North', 'South', 'East', 'West', 'North'],
    "Year": [2020, 2020, 2021, 2021, 2022],
    "Target_Sales": [1100, 1400, 1300, 1900, 1700]
})



raw_data = pd.DataFrame({
    "Emp Id": [1, 2, 3, 4, 5],
    "Emp-Names": ['John', 'Jane', 'Doe', 'Alice', 'Bob'],
    "DEPT": ['HR', 'Finance', 'IT', 'Marketing', 'Sales'],
    "Salary(INR)": [50000, 60000, 55000, 70000, 65000],
    "Joining Date": ['2020-01-15', '2019-03-20', '2021-07-10', '2018-11-05', '2020-06-25']
})


sales_raw = pd.DataFrame({
    "Order ID": [1, 2, 3, 4, 5],
    "Customer Id": [101, 102, 103, 104, 105],
    "Product Id": ["P1", "P2", "P3", "P3", "P2"],
    "Region": ['North', 'South', 'East', 'West', 'North'],  
    "Sales Amount": [1000, 1500, 1200, 2000, 1800],
    "Order Date": ['2020-01-15', '2020-02-20', '2021-03-10', '2021-04-05', '2022-05-25']
})

product_df = pd.DataFrame({
    "Product Id": ["P1", "P2", "P3"],
    "Product Name": ["Laptop", "Shirt", "Sofa"],
    "category": ["Electronics", "Clothing", "Home"]
})