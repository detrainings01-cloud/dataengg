data = [(1,"2025-01-01","Laptop","Electronics","North",50000,1),
        (2,"2025-01-02","Phone","Electronics","South",30000,2),
        (3,"2025-01-03","Table","Furniture","East",15000,1),
        (4,"2025-01-04","Chair","Furniture","West",10000,4),
        (5,"2025-01-05","Headphones","Electronics","North",20000,3),
        (6,"2025-01-06","Sofa","Furniture","South",25000,1),
        (7,"2025-01-07","Camera","Electronics","East",35000,2)
        ]

columns = ["OrderID", "OrderDate", "Product", "Category", "Region", "SalesAmount", "Quantity"]  

orders = [(1,101,"laptop",50000),
          (2,102,"phone",30000),
          (3,103,"table",15000),
          (4,104,"chair",10000),
          (5,105,"headphones",20000),
          (6,106,"sofa",25000),
          (7,107,"camera",35000)
          ]
order_columns = ["OrderID", "CustomerID", "Product", "SalesAmount"]

customer = [(101,"Alice","North"),
            (102,"Bob","South"),
            (103,"Charlie","East"),
            (104,"David","West"),
            (105,"Eve","North"),
            (106,"Frank","South"),
            (107,"Grace","East")
            ]
customer_columns = ["CustomerID", "CustomerName", "Region"]

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Test").getOrCreate()

df1 = spark.createDataFrame([(101,"North",100),
                             (101,"South",200),
                             (102,"East",150),
                             (103,"West",50)],
                            ["CustomerID", "Region", "SalesAmount"])
df2 = spark.createDataFrame([(101,"North","Gold"),
                             (101,"South","Silver"),    
                             (102,"East","Bronze"),
                             (103,"West","Platinum")],
                            ["CustomerID", "Region", "Tier"])

import pandas as pd 
data = [(1,"Alice","North",1200),
        (2,"Bob","South",None),
        (3,"Charlie",None,1800),
        (4,"David","West",2000)
        ]

columns = ["CustomerID", "CustomerName", "Region", "SalesAmount"]
df = pd.DataFrame(data, columns=columns)