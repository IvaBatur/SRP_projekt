import pandas as pd
import os
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, insert, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
from datetime import datetime, timezone

current_dir = os.path.dirname(os.path.abspath(__file__))
CSV_FILE_PATH = os.path.join(current_dir, "..", "checkpoint2", "hotel_bookings1.csv")
df = pd.read_csv(CSV_FILE_PATH)

Base = declarative_base()

class DimCustomer(Base):
    __tablename__ = 'dim_customer'
    customer_tk = Column(Integer, primary_key=True, autoincrement=True)
    customer_type = Column(String(50))
    is_repeated_guest = Column(Integer)
    date_from = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    date_to = Column(DateTime, nullable=True)

class DimTime(Base):
    __tablename__ = 'dim_time'
    time_tk = Column(Integer, primary_key=True, autoincrement=True)
    arrival_date_year = Column(Integer)
    arrival_date_month = Column(String(20))
    arrival_date_day_of_month = Column(Integer)
    arrival_date_week_number = Column(Integer)
    is_weekend = Column(Integer)

class DimLocation(Base):
    __tablename__ = 'dim_location'
    location_tk = Column(Integer, primary_key=True, autoincrement=True)
    country = Column(String(100))

class DimHotel(Base):
    __tablename__ = 'dim_hotel'
    hotel_tk = Column(Integer, primary_key=True, autoincrement=True)
    hotel = Column(String(50))
    hotel_type = Column(String(50)) 

class DimMeal(Base):
    __tablename__ = 'dim_meal'
    meal_tk = Column(Integer, primary_key=True, autoincrement=True)
    meal = Column(String(45))


class FactBooking(Base):
    __tablename__ = 'fact_booking'
    booking_tk = Column(Integer, primary_key=True, autoincrement=True)
    adr = Column(Float)
    lead_time = Column(Integer)
    total_nights = Column(Integer)
    adults = Column(Integer)
    children = Column(Integer)
    is_canceled = Column(Integer)
    reservation_status = Column(String(20))  
    time_fk = Column(Integer, ForeignKey('dim_time.time_tk'))
    customer_fk = Column(Integer, ForeignKey('dim_customer.customer_tk'))
    location_fk = Column(Integer, ForeignKey('dim_location.location_tk'))
    hotel_fk = Column(Integer, ForeignKey('dim_hotel.hotel_tk'))
    meal_fk = Column(Integer, ForeignKey('dim_meal.meal_tk'))

engine = create_engine('mysql+pymysql://root:root@localhost:3306/dw_hotel_bookings', echo=False)
Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

df['hotel_type'] = df['hotel'].apply(lambda x: x.split(' ')[0])
df['is_weekend'] = df['stays_in_weekend_nights'].apply(lambda x: 1 if x > 0 else 0)
df['total_nights'] = df['stays_in_weekend_nights'] + df['stays_in_week_nights']

time_cols = ['arrival_date_year', 'arrival_date_month', 'arrival_date_day_of_month', 'arrival_date_week_number', 'is_weekend']


session.execute(insert(DimTime), df[time_cols].drop_duplicates().to_dict(orient="records"))
session.execute(insert(DimHotel), df[['hotel', 'hotel_type']].drop_duplicates().to_dict(orient="records"))
session.execute(insert(DimMeal), df[['meal']].drop_duplicates().to_dict(orient="records"))
session.execute(insert(DimLocation), df[['country']].drop_duplicates().to_dict(orient="records"))
session.commit()


def upsert_dim_customer(df_data):
    now = datetime.now(timezone.utc)
    unique_customers = df_data[['customer_type', 'is_repeated_guest']].drop_duplicates()
    
    for _, row in unique_customers.iterrows():
        
        existing = session.query(DimCustomer).filter_by(
            customer_type=row['customer_type'], 
            is_repeated_guest=row['is_repeated_guest'], 
            date_to=None
        ).first()
        
        if not existing:
            session.add(DimCustomer(
                customer_type=row['customer_type'], 
                is_repeated_guest=row['is_repeated_guest'], 
                date_from=now
            ))
    session.commit()

upsert_dim_customer(df)

time_map = {(t.arrival_date_year, t.arrival_date_month, t.arrival_date_day_of_month): t.time_tk for t in session.query(DimTime).all()}
hotel_map = {h.hotel: h.hotel_tk for h in session.query(DimHotel).all()}
meal_map = {m.meal: m.meal_tk for m in session.query(DimMeal).all()}
loc_map = {l.country: l.location_tk for l in session.query(DimLocation).all()}

cust_map = {(c.customer_type, c.is_repeated_guest): c.customer_tk for c in session.query(DimCustomer).filter_by(date_to=None).all()}

df['time_fk'] = df.apply(lambda x: time_map.get((x['arrival_date_year'], x['arrival_date_month'], x['arrival_date_day_of_month'])), axis=1)
df['hotel_fk'] = df['hotel'].map(hotel_map)
df['meal_fk'] = df['meal'].map(meal_map)
df['location_fk'] = df['country'].map(loc_map)
df['customer_fk'] = df.apply(lambda x: cust_map.get((x['customer_type'], x['is_repeated_guest'])), axis=1)

final_fact = df[[
    'adr', 'lead_time', 'total_nights', 'adults', 'children',
    'is_canceled', 'reservation_status', 'time_fk', 'customer_fk', 'location_fk', 'hotel_fk', 'meal_fk'
]].dropna()

session.execute(insert(FactBooking), final_fact.to_dict(orient="records"))
session.commit()

print("Dimenzijski model (star schema) kreiran uspješno.")