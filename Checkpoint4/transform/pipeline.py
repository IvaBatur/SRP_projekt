from transform.dimensions.dim_customer import transform_customer_dim
from transform.dimensions.dim_hotel import transform_hotel_dim
from transform.dimensions.dim_location import transform_location_dim
from transform.dimensions.dim_meal import transform_meal_dim
from transform.dimensions.dim_status import transform_status_dim
from transform.dimensions.dim_time import transform_time_dim
from transform.facts.fact_booking import transform_booking_fact

def run_transformations(raw_data, spark):
    dim_customer = transform_customer_dim(raw_data["csv_booking"])
    dim_hotel = transform_hotel_dim(raw_data["csv_booking"])
    dim_meal = transform_meal_dim(raw_data["csv_booking"])
    dim_status = transform_status_dim(raw_data["csv_booking"])
    dim_location = transform_location_dim(raw_data["csv_booking"], spark)
    dim_time = transform_time_dim(raw_data["csv_booking"])

    fact_booking = transform_booking_fact(
        raw_data["csv_booking"],
        dim_customer,
        dim_hotel,
        dim_location,
        dim_meal,
        dim_status,
        dim_time
    )

    return {
        "dim_customer": dim_customer,
        "dim_hotel": dim_hotel,
        "dim_location": dim_location,
        "dim_meal": dim_meal,
        "dim_status": dim_status,
        "dim_time": dim_time,
        "fact_booking": fact_booking
    }