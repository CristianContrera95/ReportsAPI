import pandas as pd
from app.reports import ReportGenerator


class TestVehSalesReport:
    """
    Vehicle sales reports tests
    """

    COUNTS = {
        "sales_by_brand.csv": 14,
        "new_customers.csv": 4,
        "old_customers.csv": 7,
        "next_vehicle.csv": 9,
    }

    def check_count(self, filename):
        """
        Check number of records in the report
        """
        df_report = pd.read_csv(filename)
        assert df_report.count()[0] == self.COUNTS[filename]

    def test_sales_by_brand_count(self):
        """
        Check number of records in the report: sales by brand
        """
        filename = "sales_by_brand.csv"
        reports = ReportGenerator()
        reports.sales_by_brand(filename)
        self.check_count(filename)

    def test_new_customers_count(self):
        """
        Check number of records in the report: new customers
        """
        filename = "new_customers.csv"
        reports = ReportGenerator()
        reports.new_customers(filename)
        self.check_count(filename)

    def test_old_customers_count(self):
        """
        Check number of records in the report: old customers
        """
        filename = "old_customers.csv"
        reports = ReportGenerator()
        reports.old_customers(filename)
        self.check_count(filename)

    def test_next_vehicle_count(self):
        """
        Check number of records in the report: next vehicle
        """
        filename = "next_vehicle.csv"
        reports = ReportGenerator()
        reports.next_vehicle(filename)
        self.check_count(filename)
