import unittest
import pandas as pd
import sqlalchemy
from sqlalchemy import text
from pandas.testing import assert_frame_equal

class TestHotelDatabase(unittest.TestCase):
    def setUp(self):
       
        self.engine = sqlalchemy.create_engine('mysql+pymysql://root:root@localhost:3306/hotel_db')
        self.connection = self.engine.connect()

        
        full_df = pd.read_csv("hotel_bookings1.csv")
        self.df = full_df[[
            'country', 'hotel', 'meal', 'lead_time', 'arrival_date_year', 
            'arrival_date_month', 'stays_in_weekend_nights', 
            'stays_in_week_nights', 'adults', 'children', 'adr'
        ]].copy()

        query = """
        SELECT 
            c.name AS 'country',
            h.hotel_name AS 'hotel',
            m.type AS 'meal',
            b.lead_time,
            b.arrival_date_year,
            b.arrival_date_month,
            b.stays_in_weekend_nights,
            b.stays_in_week_nights,
            b.adults,
            b.children,
            b.adr
        FROM booking b
        JOIN country c ON b.country_fk = c.id
        JOIN hotel h ON b.hotel_fk = h.id
        JOIN meal m ON b.meal_fk = m.id
        ORDER BY b.id ASC
        """
        
        result = self.connection.execute(text(query))
        self.db_df = pd.DataFrame(result.fetchall(), columns=result.keys())

    def test_columns(self):
        self.assertListEqual(list(self.df.columns), list(self.db_df.columns))

    def test_dataframes(self):
        
        df_sorted = self.df.sort_values(['lead_time', 'adr']).reset_index(drop=True)
        db_df_sorted = self.db_df.sort_values(['lead_time', 'adr']).reset_index(drop=True)
        
       
        assert_frame_equal(df_sorted, db_df_sorted, check_dtype=False)

    def tearDown(self):
        self.connection.close()

if __name__ == '__main__':
    unittest.main()