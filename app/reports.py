"""
Generates vehicle sales reports based on Sqlite database.
"""
import os
import sqlite3
import pandas as pd
import logging
import datetime as dt
from typing import Optional, List

class ReportGenerator:
    """
    Implement the following 4 functions:
    - sales_by_brand
    - new_customers
    - old_customers
    - next_vehicle
    """

    def __init__(self, dbname: str = "vehicle_crm.sqlite"):
        self.logger = logging.getLogger('Reports_logger')
        self.dbname = dbname
        if not os.path.exists(dbname):
            self.logger.error(f'param dbname: {dbname} does not exists')
        else:
            self.con = sqlite3.connect(self.dbname)
            self.cur = self.con.cursor()
            self.logger.info(f'open db {dbname} connection')


    def __del__(self):
        self.con.close()
        self.logger.info(f'close db {self.dbname} connection')

    def __to_csv(self, df, filename: str, columns: Optional[List[str]] = None) -> int:
        try:
            if columns:
                df.to_csv(filename, columns=columns)
            else:
                df.to_csv(filename)
            return 0
        except Exception as ex:
            self.logger.error(f'Can\'t write over file {filename}\n. Error: {str(ex)}')
            return 1

    def sales_by_brand(self, filename: str) -> int:
        """
        Creates a report of vehicle sales broken by vehicle brand.
        Writes the report into a CSV file.

        Report fields:
        - vehicle_brand: vehicle brand
        - n_sales: number of sales within a group
        - total_value: sum of prices of all sales within a group

        Sort by:
        - n_sales descending
        - total_value descending

        :param str filename: Output file name
        """

        # Get the whole database to do the logic with python
        sales = pd.read_sql_query("SELECT * FROM Sales", self.con)
        vehicles = pd.read_sql_query("SELECT * FROM Vehicles", self.con)
        invoices = pd.read_sql_query("SELECT * FROM Invoices", self.con)
        vehicle_models = pd.read_sql_query("SELECT * FROM Vehicle_models", self.con)

        self.logger.info(f'Querying db tables: sales, vehicles, invoices, vehicle_models')

        # join df's by Foreign keys
        df = pd.merge(
                pd.merge(
                    pd.merge(sales, invoices, on='invoice_id'),
                    vehicles, on='vehicle_id', how='left'),
                vehicle_models, on='vehicle_model_id', how='left'
        )
        del sales, vehicles, invoices, vehicle_models

        # Check nulls values
        if any(df.isnull().sum() > 0):
            self.logger.debug(f'There is records with nulls values: \n{df.isnull().sum()}')
            df.dropna(inplace=True)

        # Group rows by brand_name
        df = df.groupby('brand_name', as_index=False).agg({'sale_id': 'size', 'price': 'sum'})

        df.rename(columns={'brand_name': 'vehicle_brand',
                           'sale_id': 'n_sales',
                           'price': 'total_value'},
                  inplace=True)
        df.sort_values(['n_sales', 'total_value'], ascending=False, inplace=True)

        return self.__to_csv(df, filename)

    def new_customers(self, filename: str) -> int:
        """
        Creates a report of customers who purchased a new vehicle after 1st January 2020
        Writes the report into a CSV file.

        Report fields:
        - customer_name: customer name
        - vehicle_brand: brand of the last purchased vehicle
        - vehicle_model: model of the last purchased vehicle
        - vehicle_year: year of the last purchased vehicle
        - sale_dt: sale date

        Sort by:
        - customer_name ascending

        :param str filename: Output file name
        """
        # Get the whole database to do the logic with python
        sales = pd.read_sql_query("SELECT * FROM Sales", self.con, parse_dates=['sale_dt'])
        vehicles = pd.read_sql_query("SELECT * FROM Vehicles", self.con)
        vehicle_models = pd.read_sql_query("SELECT * FROM Vehicle_models", self.con)
        customers = pd.read_sql_query("SELECT * FROM Customers", self.con)

        df = pd.merge(
                pd.merge(pd.merge(sales, customers, on='customer_id'),
                         vehicles, on='vehicle_id', how='left'),
                vehicle_models, on='vehicle_model_id', how='left'
            )
        del sales, vehicles, vehicle_models, customers

        # Check nulls values
        if any(df.isnull().sum() > 0):
            self.logger.debug(f'There is records with nulls values: \n{df.isnull().sum()}')
            df.dropna(inplace=True)

        # filter rows by sales after 2020-01-01 and group by customers
        df = df[df.sale_dt >= dt.datetime(2020, 1, 1)]
        df = df.groupby('customer_id', as_index=False).agg({'sale_dt': 'max',
                                                            'customer_name': 'first',
                                                            'brand_name': 'first',
                                                            'model_name': 'first',
                                                            'vehicle_year': 'first'})

        df.rename(columns={'brand_name': 'vehicle_brand',
                           'model_name': 'vehicle_model'},
                  inplace=True)
        df.sort_values(['customer_name'], inplace=True)

        return self.__to_csv(df, filename, columns=['customer_name', 'vehicle_brand',
                                                    'vehicle_model', 'vehicle_year',
                                                    'sale_dt'])

    def old_customers(self, filename):
        """
        Creates a report of customers who purchased the last vehicle before 1st January 2016
        Writes the report into a CSV file.

        Report fields:
        - customer_name: customer name
        - vehicle_brand: brand of the last purchased vehicle
        - vehicle_model: model of the last purchased vehicle
        - vehicle_year: year of the last purchased vehicle
        - sale_dt: sale date

        Sort by:
        - customer_name ascending

        :param str filename: Output file name
        """
        sales = pd.read_sql_query("SELECT * FROM Sales", self.con, parse_dates=['sale_dt'])
        vehicles = pd.read_sql_query("SELECT * FROM Vehicles", self.con)
        vehicle_models = pd.read_sql_query("SELECT * FROM Vehicle_models", self.con)
        customers = pd.read_sql_query("SELECT * FROM Customers", self.con)

        df = pd.merge(
            pd.merge(pd.merge(sales, customers, on='customer_id'),
                     vehicles, on='vehicle_id', how='left'),
            vehicle_models, on='vehicle_model_id', how='left'
        )
        del sales, vehicles, vehicle_models, customers

        # Check nulls values
        if any(df.isnull().sum() > 0):
            self.logger.debug(f'There is records with nulls values: \n{df.isnull().sum()}')
            df.dropna(inplace=True)

        # filter rows by sales before 2016-01-01 and group by customers
        df = df[df.sale_dt <= dt.datetime(2016, 1, 1)]
        df = df.groupby('customer_id', as_index=False).agg({'sale_dt': 'max',
                                                            'customer_name': 'first',
                                                            'brand_name': 'first',
                                                            'model_name': 'first',
                                                            'vehicle_year': 'first'})

        df.rename(columns={'brand_name': 'vehicle_brand',
                           'model_name': 'vehicle_model'},
                  inplace=True)
        df.sort_values(['customer_name'], inplace=True)

        return self.__to_csv(df, filename, columns=['customer_name', 'vehicle_brand',
                                                    'vehicle_model', 'vehicle_year',
                                                    'sale_dt'])


    def next_vehicle(self, filename):
        """
        Creates a report of vehicles sold to customer who previously purchased another vehicle.
        Brakes the report by first vehicle brand.
        Writes the report into a CSV file.

        Report fields:
        - first_veh_brand: first vehicle brand
        - most_common_second_veh_brand: most common brand of the second vehicle purchased by the same customer
        - avg_days_between_sales: average number of days between the subsequent sales, rounded to nearest integer

        Sort by:
        - first_veh_brand ascending

        :param str filename: Output file name
        """
        sales = pd.read_sql_query("SELECT * FROM Sales", self.con, parse_dates=['sale_dt'])
        vehicles = pd.read_sql_query("SELECT * FROM Vehicles", self.con)
        vehicle_models = pd.read_sql_query("SELECT * FROM Vehicle_models", self.con)
        customers = pd.read_sql_query("SELECT * FROM Customers", self.con)

        df = pd.merge(
            pd.merge(pd.merge(sales, customers, on='customer_id'),
                     vehicles, on='vehicle_id', how='left'),
            vehicle_models, on='vehicle_model_id', how='left'
        )
        del sales, vehicles, vehicle_models, customers

        customers_id = df.groupby('customer_id')['sale_dt'].size()
        customers_id = customers_id[customers_id > 1]
        df = df[df.customer_id.isin(customers_id.index.values)]

        df.sort_values('sale_dt', inplace=True)

        df_ = df.groupby('customer_id').agg({'brand_name': lambda x: pd.Series.mode(x)[0]}) \
            .rename(columns={'brand_name': 'most_common_second_veh_brand'})
        df_ = pd.merge(df_,
                       df.groupby('customer_id').agg({'brand_name': 'first'}) \
                       .rename(columns={'brand_name': 'first_veh_brand'}),
                       on='customer_id', how='left')

        df_.sort_values(['first_veh_brand'], inplace=True)
        return self.__to_csv(df_, filename)