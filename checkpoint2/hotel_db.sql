CREATE DATABASE dotel_db;
USE hotel_db; 
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
ORDER BY b.id ASC;