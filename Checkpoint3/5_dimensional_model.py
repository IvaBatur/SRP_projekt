from sqlalchemy import create_engine, Column, Integer, BigInteger, String, DateTime, ForeignKey, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime, timezone

# 1. POSTAVKE KONEKCIJE 
DATABASE_URL = "mysql+pymysql://root:root@localhost:3306/dw"
engine = create_engine(DATABASE_URL, echo=True)
Session = sessionmaker(bind=engine)
session = Session()
Base = declarative_base()

# 2. DIMENZIJE

class DimHotel(Base):
    __tablename__ = 'dim_hotel'
    __table_args__ = {'schema': 'dw'}

    hotel_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    hotel = Column(String(50))
    hotel_type = Column(String(50)) 

class DimCustomer(Base):
    __tablename__ = 'dim_customer'
    __table_args__ = {'schema': 'dw'}

    customer_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    customer_type = Column(String(50))
    is_repeated_guest = Column(Integer)
    
    # Sporo mijenjajuće dimenzije 
    version = Column(Integer, default=1) 
    date_from = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    date_to = Column(DateTime, nullable=True)

class DimLocation(Base):
    __tablename__ = 'dim_location'
    __table_args__ = {'schema': 'dw'}

    location_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    country = Column(String(100))
    region = Column(String(100))     
    continent = Column(String(100))   

class DimMeal(Base):
    __tablename__ = 'dim_meal'
    __table_args__ = {'schema': 'dw'}

    meal_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    meal = Column(String(45))      
    meal_description = Column(String(100)) 
    is_package = Column(Integer)      

class DimStatus(Base):               
    __tablename__ = 'dim_status'
    __table_args__ = {'schema': 'dw'}

    status_tk = Column(Integer, primary_key=True, autoincrement=True)
    reservation_status = Column(String(20)) # 'Canceled', 'Check-Out', 'No-Show'

class DimTime(Base):
    __tablename__ = 'dim_time'
    __table_args__ = {'schema': 'dw'}

    time_tk = Column(Integer, primary_key=True, autoincrement=True)
    arrival_date_year = Column(Integer, nullable=False)
    arrival_date_month = Column(String(20), nullable=False)
    arrival_date_day_of_month = Column(Integer, nullable=False)
    arrival_date_week_number = Column(Integer)
    is_weekend = Column(Integer)

# 3. TABLICA ČINJENICA 

class FactBooking(Base):
    __tablename__ = 'fact_booking'
    __table_args__ = {'schema': 'dw'}

    booking_tk = Column(BigInteger, primary_key=True, autoincrement=True)
    adr = Column(Float)
    lead_time = Column(Integer)
    total_nights = Column(Integer)
    adults = Column(Integer)
    children = Column(Integer)
    is_canceled = Column(Integer)

    time_tk = Column(Integer, ForeignKey('dw.dim_time.time_tk'))
    customer_tk = Column(BigInteger, ForeignKey('dw.dim_customer.customer_tk'))
    location_tk = Column(BigInteger, ForeignKey('dw.dim_location.location_tk'))
    hotel_tk = Column(BigInteger, ForeignKey('dw.dim_hotel.hotel_tk'))
    meal_tk = Column(BigInteger, ForeignKey('dw.dim_meal.meal_tk'))
    status_tk = Column(Integer, ForeignKey('dw.dim_status.status_tk'))

#  4. KREIRANJE TABLICA

if __name__ == "__main__":
    Base.metadata.create_all(engine)
    print(" Dimenzijski model (star schema) kreiran uspješno.")
