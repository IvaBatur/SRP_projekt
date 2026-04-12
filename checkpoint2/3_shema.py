import pandas as pd
from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, insert
from sqlalchemy.orm import sessionmaker, declarative_base

CSV_FILE_PATH = "hotel_bookings1.csv" 
df = pd.read_csv(CSV_FILE_PATH)

Base = declarative_base()

class Country(Base):
    __tablename__ = 'country'
    id = Column(Integer, primary_key=True)
    name = Column(String(100), nullable=False, unique=True)

class Hotel(Base):
    __tablename__ = 'hotel'
    id = Column(Integer, primary_key=True, autoincrement=True)
    hotel_name = Column(String(50), nullable=False, unique=True)

class Meal(Base):
    __tablename__ = 'meal'
    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(45), nullable=False, unique=True)

class Room(Base): 
    __tablename__ = 'rooms'
    id = Column(Integer, primary_key=True, autoincrement=True)
    reserved_room_type = Column(String(10), nullable=False)
    assigned_room_type = Column(String(10))

class Customer(Base): 
    __tablename__ = 'customers'
    id = Column(Integer, primary_key=True, autoincrement=True)
    customer_type = Column(String(50), nullable=False, unique=True)
    is_repeated_guest = Column(Integer)     
    previous_cancellations = Column(Integer) 
    previous_bookings_not_canceled = Column(Integer)

class Booking(Base):
    __tablename__ = 'booking'
    id = Column(Integer, primary_key=True, autoincrement=True)
    lead_time = Column(Integer)
    arrival_date_year = Column(Integer)
    arrival_date_month = Column(String(20))
    stays_in_weekend_nights = Column(Integer)
    stays_in_week_nights = Column(Integer)
    is_canceled = Column(Integer)  
    reservation_status = Column(String(20))
    adults = Column(Integer)
    children = Column(Integer)
    adr = Column(Float) 
    country_fk = Column(Integer, ForeignKey('country.id'))
    hotel_fk = Column(Integer, ForeignKey('hotel.id'))
    meal_fk = Column(Integer, ForeignKey('meal.id'))
    room_fk = Column(Integer, ForeignKey('rooms.id')) 
    customer_fk = Column(Integer, ForeignKey('customers.id'))
    market_segment = Column(String(50))
    distribution_channel = Column(String(50))


engine = create_engine('mysql+pymysql://root:root@localhost:3306/hotel_db', echo=False)

Base.metadata.drop_all(engine)
Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()

hotel_data = df[['hotel']].drop_duplicates().rename(columns={'hotel': 'hotel_name'})
session.execute(insert(Hotel), hotel_data.to_dict(orient="records"))

meals_data = df[['meal']].drop_duplicates().rename(columns={'meal': 'type'})
session.execute(insert(Meal), meals_data.to_dict(orient="records"))

rooms_data = df[['reserved_room_type','assigned_room_type']].drop_duplicates()
session.execute(insert(Room), rooms_data.to_dict(orient="records"))

countries_data = df[['country']].drop_duplicates().dropna().rename(columns={'country': 'name'})
session.execute(insert(Country), countries_data.to_dict(orient="records"))

customers_data = df[['customer_type', 'is_repeated_guest', 'previous_cancellations', 'previous_bookings_not_canceled']].drop_duplicates(subset=['customer_type'])

session.execute(insert(Customer), customers_data.to_dict(orient="records"))
session.commit()

country_map = {c.name: c.id for c in session.query(Country).all()}
hotel_map = {h.hotel_name: h.id for h in session.query(Hotel).all()}
meal_map = {m.type: m.id for m in session.query(Meal).all()}
customer_map = {c.customer_type: c.id for c in session.query(Customer).all()}
room_map = {(r.reserved_room_type, r.assigned_room_type): r.id for r in session.query(Room).all()}

df['country_fk'] = df['country'].map(country_map)
df['hotel_fk'] = df['hotel'].map(hotel_map)
df['meal_fk'] = df['meal'].map(meal_map)
df['room_fk'] = df.apply(lambda x: room_map.get((x['reserved_room_type'], x['assigned_room_type'])), axis=1)
df['customer_fk'] = df['customer_type'].map(customer_map)
df['children'] = df['children'].fillna(0).astype(int)

final_bookings = df[[
    'lead_time', 'arrival_date_year', 'arrival_date_month', 
    'stays_in_weekend_nights', 'stays_in_week_nights', 
    'adults', 'children', 'adr', 'distribution_channel',
    'country_fk', 'hotel_fk', 'meal_fk', 'room_fk', 'customer_fk','market_segment', 'is_canceled', 'reservation_status'
]].dropna(subset=['country_fk', 'hotel_fk','room_fk', 'customer_fk','meal_fk'])

session.execute(insert(Booking), final_bookings.to_dict(orient="records"))
session.commit()

print("Data imported successfully!")