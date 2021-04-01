# Introduction

The marketing team needs specific vehicle sales reports generated and based on a relational CRM database. The reports need to be created with Python and Sqlite3. The marketing team expects the reports to be stored in CSV format files.

# Problem statement

Your task is to implement four methods in the `ReportGenerator` class:
1. `sales_by_brand`,
2. `new_customers`,
3. `old_customers`,
4. `next_vehicle`.

Each of the functions should create one report and save it in a file in the working directory.

## Database

The database consists of five tables with the following fields:
1. `Sales: sale_id, vehicle_id, invoice_id, sale_dt, customer_id`,
2. `Vehicles: vehicle_id, vehicle_model_id, vehicle_year`,
3. `Vehicle_models: vehicle_model_id, brand_name, model_name`,
4. `Customers: customer_id, customer_name`,
5. `Invoices: invoice_id, price`.

Primary keys:
1. `Sales.sale_id`
2. `Vehicles.vehicle_id`
3. `Vehicle_models.vehicle_model_id`
4. `Customers.customer_id`
5. `Invoices.invoice_id`

Foreign keys:
1. `Sales.vehicle_id` -> `Vehicles.vehicle_id`
2. `Sales.invoice_id` -> `Invoices.invoice_id`
3. `Sales.customer_id` -> `Customers.customer_id`
4. `Vehicles.vehicle_model_id` -> `Vehicle_models.vehicle_model_id`

## sales_by_brand

Create a report of vehicle sales divided by the vehicle brand.

Report fields: 
1. `vehicle_brand`: vehicle brand
2. `n_sales: number` of sales within a group
3. `total_value`: sum of prices of all sales within a group

Sort by:
1. `n_sales` descending
2. `total_value` descending

Example:
```csv
brand_name,n_sales,total_value
Ford,10,1134580.0
Mercedes,10,850400.0
Fiat,8,556410.0
```

## new_customers

Create a report of customers that purchased a new vehicle after 1 January 2020.

Report fields:
1. `customer_name`: customer name
2. `vehicle_brand`: brand of the last purchased vehicle
3. `vehicle_model`: model of the last purchased vehicle
4. `vehicle_year`: year of the last purchased vehicle
5. `sale_dt`: sale date

Sort by:
1. `customer_name` ascending

Example:
```csv
customer_name,brand_name,model_name,vehicle_year,sale_dt
John A,BMW,M3,2015,2020-01-02
Tom B,Buick,Encore,2019,2020-02-01
```

## old_customer

Create a report of customers that purchased the last vehicle before 1st January 2016.

Report fields: 
1. `customer_name`: customer name
2. `vehicle_brand`: brand of the last purchased vehicle
3. `vehicle_model`: model of the last purchased vehicle
4. `vehicle_year`: year of the last purchased vehicle
5. `sale_dt`: sale date

Sort by:
1. `customer_name` ascending

Example:
```csv
customer_name,brand_name,model_name,vehicle_year,sale_dt
John C,Cadillac,Escalade,2007,2015-03-01
Andrew D,Chrysler,Grand Voyager,2009,2011-07-30
```

## next_vehicle

Create a report of vehicles sold to customers that had previously purchased a vehicle. 
Divide the report by the first vehicle brand. Let's assume that:
1. No customer bought two vehicles in the same day
2. Pairs can repeat (you can have for example two BMW/VW rows)
3. First and second most common brand can be the same brand (e.g. Volvo/Volvo)

Report fields:
1. `first_veh_brand`: the first vehicle brand
2. `most_common_second_veh_brand`: the most common brand of the second vehicle
3. `avg_days_between_sales`: the average number of days between the subsequent sales, rounded to the nearest integer

Sort by:
1. `avg_days_between_sales` descending

Example:
```csv
first_veh_brand,most_common_second_veh_brand,avg_days_between_sales
Ford,Citroen,684.0
Toyota,Suzuki,1145.0
```

## Environment setup

To execute the unit tests, use:

```
pip install -q -e . && python3 setup.py pytest
```
