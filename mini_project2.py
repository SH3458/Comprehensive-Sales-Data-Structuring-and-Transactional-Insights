### Utility Functions
import pandas as pd
import sqlite3
from sqlite3 import Error

def create_connection(db_file, delete_db=False):
    import os
    if delete_db and os.path.exists(db_file):
        os.remove(db_file)

    conn = None
    try:
        conn = sqlite3.connect(db_file)
        conn.execute("PRAGMA foreign_keys = 1")
    except Error as e:
        print(e)

    return conn


def create_table(conn, create_table_sql, drop_table_name=None):
    
    if drop_table_name: # You can optionally pass drop_table_name to drop the table. 
        try:
            c = conn.cursor()
            c.execute("""DROP TABLE IF EXISTS %s""" % (drop_table_name))
        except Error as e:
            print(e)
    
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e)
        
def execute_sql_statement(sql_statement, conn):
    cur = conn.cursor()
    cur.execute(sql_statement)

    rows = cur.fetchall()

    return rows

def step1_create_region_table(customer_dict_filename, normalized_customer_dictbase_filename):
    # Inputs: Name of the customer_dict and normalized customer_dictbase filename
    # Output: None
    
    ### BEGIN SOLUTION
    with open(customer_dict_filename, 'r') as file:
        rgs = set(line.strip().split('\t')[4] for i, line in enumerate(file) if i > 0)
    with create_connection(normalized_customer_dictbase_filename) as conn:
        create_table(conn, """CREATE TABLE IF NOT EXISTS Region (RegionID INTEGER PRIMARY KEY, Region TEXT NOT NULL)
                    """, drop_table_name='Region')
    with conn:
        cur = conn.cursor()
        cur.executemany("INSERT INTO Region (Region) VALUES (?)", [tuple(region.split(', ')) for region in sorted(rgs)])
        
    ### END SOLUTION

def step2_create_region_to_regionid_dictionary(normalized_customer_dictbase_filename):
    
    
    ### BEGIN SOLUTION
    with create_connection(normalized_customer_dictbase_filename) as conn:
        rg_dict = execute_sql_statement("SELECT * FROM Region", conn)
    return {row[1]: row[0] for row in rg_dict}
    ### END SOLUTION


def step3_create_country_table(customer_dict_filename, normalized_customer_dictbase_filename):
    # Inputs: Name of the customer_dict and normalized customer_dictbase filename
    # Output: None
    
    ### BEGIN SOLUTION
    with open(customer_dict_filename, 'r') as f:
        Int_split = [line.strip('\n').split('\t') for line in f.readlines()[1:]]
    new = [[row[3], row[4]] for row in Int_split]
    dup = []   #duplicates
    i = 0
    while i < len(new):
        if new[i] not in dup:
            dup.append(new[i])
        i += 1
    dup.sort()
    rg_dict = step2_create_region_to_regionid_dictionary(normalized_customer_dictbase_filename)
    l1 = [(row[0], rg_dict[row[1]]) if row[1] in rg_dict else row for row in dup]
    with create_connection(normalized_customer_dictbase_filename) as conn:
        table = """CREATE TABLE IF NOT EXISTS Country (
                CountryID INTEGER PRIMARY KEY AUTOINCREMENT, 
                Country TEXT NOT NULL, 
                RegionID INTEGER NOT NULL);
                """
        create_table(conn, table, drop_table_name='Country')
        with conn:
            cur = conn.cursor()
            cur.executemany("INSERT INTO Country (Country, RegionID) VALUES (?, ?)", l1)
            
    ### END SOLUTION


def step4_create_country_to_countryid_dictionary(normalized_customer_dictbase_filename):
    
    
    ### BEGIN SOLUTION
    with create_connection(normalized_customer_dictbase_filename) as conn:
        cur = conn.cursor()
    cur.execute("SELECT Country, CountryID FROM Country")
    return {row[0]: row[1] for row in cur.fetchall()}
    ### END SOLUTION
        
        
def step5_create_customer_table(customer_dict_filename, normalized_customer_dictbase_filename):

    ### BEGIN SOLUTION
    with open(customer_dict_filename, 'r') as file:
        Int_split = [line.strip('\n').split('\t') for line in file.readlines()]
    Int_split.pop(0)
    new = []
    i = 0
    while i < len(Int_split):
        split1 = Int_split[i][0].split(" ", 1)
        new1 = [split1[0], split1[1], Int_split[i][1], Int_split[i][2], Int_split[i][3]]
        if new1 not in new:
            new.append(new1)
        i += 1
    new.sort()
    country_id = step4_create_country_to_countryid_dictionary(normalized_customer_dictbase_filename)
    i = 0
    while i < len(new):
        if new[i][-1] in country_id:
            new[i][-1] = country_id[new[i][-1]]
        i += 1
    conn = create_connection(normalized_customer_dictbase_filename)
    table = """CREATE TABLE Customer (
            [CustomerID] INTEGER NOT NULL Primary Key,
            [FirstName] TEXT NOT NULL,
            [LastName] TEXT NOT NULL,
            [Address] TEXT NOT NULL,
            [City] TEXT NOT NULL,
            [CountryID] INTEGER NOT NULL); 
            """
    with conn:
        create_table(conn, table, drop_table_name='Customer')
        cur = conn.cursor()
        r1 = [tuple(row) for row in new]
        cur.executemany("insert into Customer(FirstName,LastName,Address,City,CountryID) values(?,?,?,?,?)", r1)
          
        ### END SOLUTION


def step6_create_customer_to_customerid_dictionary(normalized_customer_dictbase_filename):
    
    
    ### BEGIN SOLUTION
    with create_connection(normalized_customer_dictbase_filename) as conn:
        sql_statement = "SELECT * FROM Customer"
        data = execute_sql_statement(sql_statement, conn)
        return {f"{row[1]} {row[2]}": idx+1 for idx, row in enumerate(data)}
    ### END SOLUTION
        
def step7_create_productcategory_table(customer_dict_filename, normalized_customer_dictbase_filename):
    # Inputs: Name of the customer_dict and normalized customer_dictbase filename
    # Output: None

    ### BEGIN SOLUTION
    with open(customer_dict_filename, 'r') as file:
        l1 = [line.strip().split('\t') for line in file.readlines()[1:]]
    prod_ctg = list(set([(p_ctg, pd) for line in l1 for p_ctg, pd in zip(line[6].split(';'), line[7].split(';'))]))
    prod_ctg.sort()
    conn = create_connection(normalized_customer_dictbase_filename)
    table = """CREATE TABLE ProductCategory (
            [ProductCategoryID] INTEGER NOT NULL Primary Key,
            [ProductCategory] TEXT NOT NULL,
            [ProductCategoryDescription] TEXT NOT NULL);
            """
    with conn:
        create_table(conn, table, drop_table_name='ProductCategory') 
        cur = conn.cursor()
        cur.executemany("INSERT INTO ProductCategory (ProductCategory, ProductCategoryDescription) VALUES (?, ?)", prod_ctg)
    ### END SOLUTION

def step8_create_productcategory_to_productcategoryid_dictionary(normalized_customer_dictbase_filename):
    
    
    ### BEGIN SOLUTION
    with create_connection(normalized_customer_dictbase_filename) as conn:
        sql_statement = "SELECT * FROM ProductCategory"
        dict = execute_sql_statement(sql_statement, conn)
        return {j[1]: i + 1 for i, j in enumerate(dict)}
    ### END SOLUTION
        

def step9_create_product_table(customer_dict_filename, normalized_customer_dictbase_filename):
    # Inputs: Name of the customer_dict and normalized customer_dictbase filename
    # Output: None

    
    ### BEGIN SOLUTION
    with open(customer_dict_filename, 'r') as file:
        Int_split = [line.strip('\n').split('\t') for line in file][1:]
    prod_id = step8_create_productcategory_to_productcategoryid_dictionary(normalized_customer_dictbase_filename)
    prod_list = [[prod, float(price), prod_id.get(p_id)]
                    for row in Int_split
                    for prod, price, p_id in zip(row[5].split(';'), row[8].split(';'), row[6].split(';'))]
    prod_list = [list(item) for item in set(tuple(row) for row in prod_list)]
    prod_list.sort()
    table = """CREATE TABLE Product (
            [ProductID] INTEGER NOT NULL Primary Key,
            [ProductName] TEXT NOT NULL,
            [ProductUnitPrice] REAL NOT NULL,
            [ProductCategoryID] INTEGER NOT NULL ,
            FOREIGN KEY(ProductCategoryID) REFERENCES ProductCategory(ProductCategoryID));
            """
    conn = create_connection(normalized_customer_dictbase_filename)
    with conn:
        create_table(conn, table, drop_table_name='Product')
        cur = conn.cursor()
        cur.executemany("insert into Product(ProductName, ProductUnitPrice, ProductCategoryID) values(?,?,?)", prod_list)
    ### END SOLUTION


def step10_create_product_to_productid_dictionary(normalized_customer_dictbase_filename):
    
    ### BEGIN SOLUTION
    with create_connection(normalized_customer_dictbase_filename) as conn:
        cur = conn.cursor()
        cur.execute("SELECT ProductName, ProductID FROM Product")
        return {row[0]: row[1] for row in cur.fetchall()}
    ### END SOLUTION
        

def step11_create_orderdetail_table(customer_dict_filename, normalized_customer_dictbase_filename):
    # Inputs: Name of the customer_dict and normalized customer_dictbase filename
    # Output: None

    
    ### BEGIN SOLUTION
    from datetime import datetime
    prod_id = step10_create_product_to_productid_dictionary(normalized_customer_dictbase_filename)
    cust_id = step6_create_customer_to_customerid_dictionary(normalized_customer_dictbase_filename)
    with open(customer_dict_filename, 'r') as file:
        Int_split = [line.strip('\n').split('\t') for line in file][1:]
    order_list = [[cust_id[split[0]], prod_id[p], datetime.strptime(d.strip(), '%Y%m%d').strftime('%Y-%m-%d'), int(q)] 
                  for split in Int_split for p, q, d in zip(split[5].split(';'), split[9].split(';'), split[10].split(';'))]
    table = """CREATE TABLE OrderDetail (
            [OrderID] INTEGER NOT NULL PRIMARY KEY, 
            [CustomerID] INTEGER NOT NULL ,
            [ProductID] INTEGER NOT NULL ,
            [OrderDate] INTEGER NOT NULL, 
            [QuantityOrdered] INTEGER NOT NULL);
            """
    conn = create_connection(normalized_customer_dictbase_filename)
    with conn:
        create_table(conn, table, drop_table_name='OrderDetail')
        cur = conn.cursor()
        cur.executemany("INSERT INTO OrderDetail(CustomerID, ProductID, OrderDate,QuantityOrdered) VALUES (?,?,?,?);", order_list)   
    ### END SOLUTION


def ex1(conn, CustomerName):
    
    # Simply, you are fetching all the rows for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # ProductName
    # OrderDate
    # ProductUnitPrice
    # QuantityOrdered
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    sql_statement = f"""SELECT Customer.FirstName||' '|| Customer.LastName AS Name, Product.ProductName, OrderDetail.OrderDate, 
                    ROUND(Product.ProductUnitPrice, 2) AS ProductUnitPrice, OrderDetail.QuantityOrdered,
                    ROUND(OrderDetail.QuantityOrdered*Product.ProductUnitPrice, 2) AS Total 
                    FROM OrderDetail INNER JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID
                    JOIN Product ON Product.ProductID = OrderDetail.ProductID 
                    WHERE Name = '{CustomerName}';
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex2(conn, CustomerName):
    
    # Simply, you are summing the total for a given CustomerName. 
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # HINT: USE customer_to_customerid_dict to map customer name to customer id and then use where clause with CustomerID
    
    ### BEGIN SOLUTION
    sql_statement = f"""SELECT Customer.FirstName||' '|| Customer.LastName AS Name,
                    ROUND(SUM(OrderDetail.QuantityOrdered*Product.ProductUnitPrice), 2) AS Total 
                    FROM OrderDetail  JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID
                    JOIN Product ON Product.ProductID = OrderDetail.ProductID 
                    WHERE Name = '{CustomerName}';
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex3(conn):
    
    # Simply, find the total for all the customers
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer and Product table.
    # Pull out the following columns. 
    # Name -- concatenation of FirstName and LastName
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION
    sql_statement = """SELECT Customer.FirstName||' '|| Customer.LastName AS Name,
                    ROUND(SUM(OrderDetail.QuantityOrdered*Product.ProductUnitPrice), 2) AS Total 
                    FROM OrderDetail
                    JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID
                    JOIN Product ON Product.ProductID = OrderDetail.ProductID  
                    GROUP BY Name 
                    ORDER BY Total DESC;    
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex4(conn):
    
    # Simply, find the total for all the region
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, Country, and 
    # Region tables.
    # Pull out the following columns. 
    # Region
    # Total -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round to two decimal places
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """SELECT Region,
                    ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered), 2) as Total 
                    FROM OrderDetail 
                    JOIN Product ON Product.ProductID = OrderDetail.ProductID
                    JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID
                    JOIN Country ON Country.CountryID = Customer.CountryID
                    JOIN Region ON Region.RegionID = Country.RegionID
                    GROUP BY Region 
                    ORDER BY Total DESC     
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex5(conn):
    
     # Simply, find the total for all the countries
    # Write an SQL statement that SELECTs From the OrderDetail table and joins with the Customer, Product, and Country table.
    # Pull out the following columns. 
    # Country
    # CountryTotal -- which is calculated from multiplying ProductUnitPrice with QuantityOrdered -- sum first and then round
    # ORDER BY Total Descending 
    ### BEGIN SOLUTION

    sql_statement = """SELECT Country, 
                    ROUND(SUM(P.ProductUnitPrice * OD.QuantityOrdered)) as CountryTotal
                    FROM OrderDetail OD
                    JOIN Product P ON P.ProductID = OD.ProductID
                    JOIN Customer C ON C.CustomerID = OD.CustomerID
                    JOIN Country C1 ON C1.CountryID = C.CountryID
                    JOIN Region R ON R.RegionID = C1.RegionID
                    GROUP BY Country 
                    ORDER BY CountryTotal DESC;
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement


def ex6(conn):
    
    # Rank the countries within a region based on order total
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    ### BEGIN SOLUTION

    sql_statement = """SELECT Region, Country, CountryTotal,
                    RANK () OVER (PARTITION BY Region
                    ORDER BY CountryTotal DESC) CountryRegionalRank 
                    FROM(SELECT Region,Country, ROUND(SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered)) as CountryTotal
                    FROM OrderDetail
                    JOIN Product ON Product.ProductID = OrderDetail.ProductID
                    JOIN Customer ON Customer.CustomerID = OrderDetail.CustomerID
                    JOIN Country ON Country.CountryID = Customer.CountryID
                    JOIN Region ON Region.RegionID = Country.RegionID
                    GROUP BY Country)
                    ORDER BY Region ASC    
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement



def ex7(conn):
    
   # Rank the countries within a region based on order total, BUT only select the TOP country, meaning rank = 1!
    # Output Columns: Region, Country, CountryTotal, CountryRegionalRank
    # Hint: Round the the total
    # Hint: Sort ASC by Region
    # HINT: Use "WITH"
    ### BEGIN SOLUTION

    sql_statement = """WITH country_total AS (
                    SELECT C.RegionID, C.Country AS Country,
                    ROUND(SUM(P.ProductUnitPrice * O.QuantityOrdered)) AS CountryTotal,
                    RANK() OVER (PARTITION BY C.RegionID ORDER BY SUM(P.ProductUnitPrice * O.QuantityOrdered) DESC) AS CountryRank
                    FROM OrderDetail o
                    JOIN Product P ON O.ProductID = P.ProductID 
                    JOIN Customer cust ON O.CustomerID = cust.CustomerID 
                    JOIN Country C ON cust.CountryID = C.CountryID
                    GROUP BY C.RegionID, C.Country)

                    SELECT r.Region AS Region, crt.Country AS Country, crt.CountryTotal AS CountryTotal, crt.CountryRank AS CountryRegionalRank
                    FROM country_total crt
                    JOIN Region r ON r.RegionID = crt.RegionID
                    WHERE crt.CountryRank = 1
                    ORDER BY r.Region ASC
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex8(conn):
    
    # Sum customer sales by Quarter and year
    # Output Columns: Quarter,Year,CustomerID,Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    ### BEGIN SOLUTION

    sql_statement = """WITH quarters AS (
                    SELECT CustomerID, strftime('%Y', OrderDate) AS Year,
                    CASE 
                    WHEN strftime('%m', OrderDate) BETWEEN '01' AND '03' THEN 'Q1'
                    WHEN strftime('%m', OrderDate) BETWEEN '04' AND '06' THEN 'Q2'
                    WHEN strftime('%m', OrderDate) BETWEEN '07' AND '09' THEN 'Q3'
                    WHEN strftime('%m', OrderDate) BETWEEN '10' AND '12' THEN 'Q4'
                    END AS Quarter,
                    SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered) AS Total
                    FROM OrderDetail
                    JOIN Product ON OrderDetail.ProductID = Product.ProductID
                    GROUP BY CustomerID, Year, Quarter)

                    SELECT Quarter, CAST(Year AS INTEGER) AS Year, CustomerID, ROUND(Total) AS Total
                    FROM quarters
                    ORDER BY Year, Quarter, CustomerID ASC
                    """ 
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex9(conn):
    
    # Rank the customer sales by Quarter and year, but only select the top 5 customers!
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    # HINT: YOU MUST CAST YEAR TO TYPE INTEGER!!!!
    # HINT: You can have multiple CTE tables;
    # WITH table1 AS (), table2 AS ()
    ### BEGIN SOLUTION

    sql_statement = """WITH quarters AS (
                    SELECT CustomerID, strftime('%Y', OrderDate) AS Year,
                    CASE 
                    WHEN strftime('%m', OrderDate) BETWEEN '01' AND '03' THEN 'Q1'
                    WHEN strftime('%m', OrderDate) BETWEEN '04' AND '06' THEN 'Q2'
                    WHEN strftime('%m', OrderDate) BETWEEN '07' AND '09' THEN 'Q3'
                    WHEN strftime('%m', OrderDate) BETWEEN '10' AND '12' THEN 'Q4'
                    END AS Quarter,
                    SUM(Product.ProductUnitPrice * OrderDetail.QuantityOrdered) AS Total
                    FROM OrderDetail
                    JOIN Product ON OrderDetail.ProductID = Product.ProductID
                    GROUP BY CustomerID, Year, Quarter),

                    yearr AS (
                    SELECT Quarter, CAST(Year AS INTEGER) AS Year, CustomerID, 
                    ROUND(Total) AS Total, 
                    RANK() OVER(PARTITION BY Quarter, Year ORDER BY Total DESC) AS CustomerRank
                    FROM quarters)

                    SELECT Quarter, Year, CustomerID, Total, CustomerRank
                    FROM yearr
                    WHERE CustomerRank <= 5
                    ORDER BY Year, Quarter, CustomerRank ASC
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex10(conn):
    
    # Rank the monthly sales
    # Output Columns: Quarter, Year, CustomerID, Total
    # HINT: Use "WITH"
    # Hint: Round the the total
    ### BEGIN SOLUTION

    sql_statement = """WITH monthly_sales AS (
                    SELECT 
                    CASE
                    WHEN strftime('%m', O.OrderDate) = '01' THEN 'January'
                    WHEN strftime('%m', O.OrderDate) = '02' THEN 'February'
                    WHEN strftime('%m', O.OrderDate) = '03' THEN 'March'
                    WHEN strftime('%m', O.OrderDate) = '04' THEN 'April'
                    WHEN strftime('%m', O.OrderDate) = '05' THEN 'May'
                    WHEN strftime('%m', O.OrderDate) = '06' THEN 'June'
                    WHEN strftime('%m', O.OrderDate) = '07' THEN 'July'
                    WHEN strftime('%m', O.OrderDate) = '08' THEN 'August'
                    WHEN strftime('%m', O.OrderDate) = '09' THEN 'September'
                    WHEN strftime('%m', O.OrderDate) = '10' THEN 'October'
                    WHEN strftime('%m', O.OrderDate) = '11' THEN 'November'
                    WHEN strftime('%m', O.OrderDate) = '12' THEN 'December'
                    END Month, strftime('%Y', O.OrderDate) AS Year, C.CustomerID, P.ProductUnitPrice * O.QuantityOrdered AS Total
                    FROM OrderDetail o
                    JOIN Customer C on O.CustomerID = C.CustomerID
                    JOIN Product P on O.ProductID = P.ProductID)

                    SELECT Month, 
                    SUM(Round(Total)) AS Total,
                    RANK() over (ORDER BY SUM(Total) DESC) AS TotalRank
                    FROM monthly_Sales
                    GROUP BY Month;
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement

def ex11(conn):
    
    # Find the MaxDaysWithoutOrder for each customer 
    # Output Columns: 
    # CustomerID,
    # FirstName,
    # LastName,
    # Country,
    # OrderDate, 
    # PreviousOrderDate,
    # MaxDaysWithoutOrder
    # order by MaxDaysWithoutOrder desc
    # HINT: Use "WITH"; I created two CTE tables
    # HINT: Use Lag

    ### BEGIN SOLUTION

    sql_statement = """WITH Orderdays AS (
                    SELECT cust.CustomerID, cust.FirstName, cust.LastName, C.Country, O.OrderDate,
                    LAG(O.OrderDate) OVER (PARTITION BY cust.CustomerID ORDER BY O.OrderDate) AS PreviousOrderDate
                    FROM OrderDetail o
                    JOIN Product P ON o.ProductID = P.ProductID
                    JOIN Customer cust ON o.CustomerID = cust.CustomerID
                    JOIN Country C ON cust.CountryID = C.CountryID
                    JOIN Region R ON R.RegionID = C.RegionID)

                    SELECT CustomerID, FirstName, LastName, Country, OrderDate, PreviousOrderDate,
                    MAX((strftime('%s', OrderDate) - strftime('%s', PreviousOrderDate)) / 86400.0) AS MaxDaysWithoutOrder
                    FROM Orderdays
                    GROUP BY CustomerID
                    ORDER BY MaxDaysWithoutOrder DESC;
                    """
    ### END SOLUTION
    df = pd.read_sql_query(sql_statement, conn)
    return sql_statement