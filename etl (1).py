import getpass
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime
import numpy as np

password = getpass.getpass("Masukkan password: ")
# === CONFIGURATION ===
SOURCE_DB_URI = f'mysql+mysqlconnector://root:{password}@10.183.12.199/classicmodels'
TARGET_DB_URI = f'mysql+mysqlconnector://root:{password}@10.183.12.199/olap'

# Create a Connection
source_engine = create_engine(SOURCE_DB_URI)
target_engine = create_engine(TARGET_DB_URI)

# === EXTRACT ===
def extract_table(table_name, source):
    print(f"Extracting {table_name}...")
    if source == 1 :
        return pd.read_sql(f"SELECT * FROM {table_name}", source_engine)
    else :
        return pd.read_sql(f"SELECT * FROM {table_name}", target_engine)

# === TRANSFORM ===

def getmaxsurrogate(dimension, engine):
    query = f"SELECT MAX({dimension}_id) as max_id FROM {dimension}"
    result = pd.read_sql(query, engine)
    return result['max_id'][0] if result['max_id'][0] is not None else 0
    
def transform_products(df_products, df_productlines):
    print("Transforming product dimension...")
    df = df_products.merge(df_productlines, on="productLine", how="left")
    
    df['productLine'] = df['productLine'].fillna("No Category")

    last_id = getmaxsurrogate('ProdDim', target_engine)
    df['ProdDim_id'] = range(last_id + 1, last_id + 1 + len(df))
    
    return df[['ProdDim_id', 'productCode', 'productName', 'productLine', 'productVendor']]

def transform_employees(df_employees):
    print("Transforming employee dimension...")

    last_id = getmaxsurrogate('EmpDim', target_engine)
    df_employees['EmpDim_id'] = range(last_id + 1, last_id + 1 + len(df_employees))

    return df_employees[['EmpDim_id','employeeNumber', 'firstName', 'lastName']]

def transform_customers(df_customers):
    print("Transforming customer dimension...")

    df_customers['roweffectivedate'] = pd.Timestamp(datetime.now().date())
    df_customers['rowexpirationdate'] = pd.Timestamp(datetime.max.date())
    df_customers['currentrowindicator'] = 'Current'
    
    df_customers['state'] = df_customers['state'].fillna('No State')

    last_id = getmaxsurrogate('CustDim', target_engine)
    df_customers['CustDim_id'] = range(last_id + 1, last_id + 1 + len(df_customers))

    return df_customers[['CustDim_id', 'customerNumber', 'city', 'state', 'country', 'roweffectivedate', 
                         'rowexpirationdate', 'currentrowindicator']]

def transform_offices(df_offices):
    print("Transforming office dimension...")

    df_offices['roweffectivedate'] = pd.Timestamp(datetime.now().date())
    df_offices['rowexpirationdate'] = pd.Timestamp(datetime.max.date())
    df_offices['currentrowindicator'] = 'Current'

    df_offices['state'] = df_offices['state'].fillna('No State')

    last_id = getmaxsurrogate('OffDim', target_engine)
    df_offices['OffDim_id'] = range(last_id + 1, last_id + 1 + len(df_offices))

    return df_offices[['OffDim_id', 'officeCode', 'city', 'state', 'country', 'roweffectivedate', 
                         'rowexpirationdate', 'currentrowindicator']]

def transform_times(df_times):
    print("Transforming time dimension...")

    df_times['orderDate'] = pd.to_datetime(df_times['orderDate'], errors='coerce')

    df_times['day'] = df_times['orderDate'].dt.day
    df_times['month'] = df_times['orderDate'].dt.month
    df_times['year'] = df_times['orderDate'].dt.year

    df_times = df_times.drop_duplicates(subset=['day', 'month', 'year'])

    df_times['is_weekend'] = np.where(
        df_times['orderDate'].dt.weekday >= 5,
        'Weekend',
        'Weekday'
    )

    last_id = getmaxsurrogate('TimeDim', target_engine)
    df_times['TimeDim_id'] = range(last_id + 1, last_id + 1 + len(df_times))
    
    return df_times[['TimeDim_id', 'day','month','year','is_weekend']]

def transform_fact_sales(df_orderdetails, df_products, df_productlines, df_orders, df_customers, df_employees, df_offices,
                         df_dim_product, df_dim_customer, df_dim_employee, df_dim_office, df_dim_time):
    print("Transforming fact_sales...")
    df = df_orderdetails.merge(df_products, on='productCode', how='left') \
                        .merge(df_productlines, on='productLine', how='left') \
                        .merge(df_orders, on='orderNumber', how='left') \
                        .merge(df_customers, on='customerNumber', how='left') \
                        .merge(df_employees, left_on='salesRepEmployeeNumber', right_on='employeeNumber', how='left') \
                        .merge(df_offices, on='officeCode', how='left')
    
    df = df.merge(df_dim_product[['productCode', 'ProdDim_id']], on='productCode', how='left')
    df = df.merge(df_dim_customer[['customerNumber', 'CustDim_id']], on='customerNumber', how='left')
    df = df.merge(df_dim_employee[['employeeNumber', 'EmpDim_id']], left_on='salesRepEmployeeNumber', right_on='employeeNumber', how='left')
    df = df.merge(df_dim_office[['officeCode', 'OffDim_id']], on='officeCode', how='left')

    df['orderDate'] = pd.to_datetime(df['orderDate'], errors='coerce')
    df['day'] = df['orderDate'].dt.day
    df['month'] = df['orderDate'].dt.month
    df['year'] = df['orderDate'].dt.year
    df = df.merge(
        df_dim_time[['TimeDim_id', 'day', 'month', 'year']],
        on=['day', 'month', 'year'],
        how='left'
    )
    df.rename(columns={'EmpDim_id_x': 'EmpDim_id',
                       'CustDim_id_x': 'CustDim_id',
                       'OffDim_id_x': 'OffDim_id'
                       }, inplace=True)

    return df[['TimeDim_id', 'EmpDim_id', 'CustDim_id', 'ProdDim_id', 'OffDim_id', 'orderNumber', 
               'buyPrice', 'priceEach', 'quantityOrdered']]
    #(df.columns.tolist())
    
# === LOAD ===
def load_table(df, table_name):
    print(f"Loading {table_name}...")
    df.to_sql(table_name, target_engine, if_exists='append', index=False)

# === MAIN ETL PROCESS ===
def run_etl():
    # Extract
    products = extract_table('products',1)
    productlines = extract_table('productlines',1)
    orders = extract_table('orders',1)
    orderdetails = extract_table('orderdetails',1)
    employees = extract_table('employees',1)
    customers = extract_table('customers',1)
    offices = extract_table('offices',1)

    # Transform dimension
    dim_products = transform_products(products, productlines)
    dim_employees = transform_employees(employees)
    dim_customers = transform_customers(customers)
    dim_offices = transform_offices(offices)
    dim_times = transform_times(orders)

    # Load dimension
    load_table(dim_products, 'ProdDim')
    load_table(dim_employees, 'EmpDim')
    load_table(dim_customers, 'CustDim')
    load_table(dim_offices, 'OffDim')
    load_table(dim_times, 'TimeDim')

    #Transform fact
    facttables = transform_fact_sales(orderdetails, products, productlines, orders, customers, employees, offices, 
                                      extract_table('ProdDim', 2), extract_table('CustDim', 2), 
                                      extract_table('EmpDim', 2), extract_table('OffDim', 2), extract_table('TimeDim', 2))

    #Load fact
    load_table(facttables, 'FactTable')

if __name__ == '__main__':
    run_etl()
