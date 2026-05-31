from pyspark import pipelines as dp

#Dim passenger (Type-1)Start

#create source view
@dp.view
def dim_passenger_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("passenger_id","passenger_name","passenger_email","passenger_phone")
    df = df.dropDuplicates(subset=['passenger_id'])
    return df

#Create empty straming table(target)
dp.create_streaming_table("dim_passenger")
dp.create_auto_cdc_flow(
    target = "dim_passenger",
    source = "dim_passenger_view",
    keys = ["passenger_id"],
    sequence_by = "passenger_id",
    stored_as_scd_type = 1
)
#Dim passenger End

#Dim driver (Type-1)Start

#create source view
@dp.view
def dim_driver_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("driver_id","driver_name","driver_license","driver_phone","driver_rating")
    df = df.dropDuplicates(subset=['driver_id'])
    return df

#Create empty straming table(target)
dp.create_streaming_table("dim_driver")
dp.create_auto_cdc_flow(
    target = "dim_driver",
    source = "dim_driver_view",
    keys = ["driver_id"],
    sequence_by = "driver_id",
    stored_as_scd_type = 1
)
#Dim driver End


#Dim vehicle (Type-1)Start

#create source view
@dp.view
def dim_vehicle_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("vehicle_id","vehicle_type_id","vehicle_make_id","vehicle_make","vehicle_model","vehicle_type","vehicle_color","license_plate")
    df = df.dropDuplicates(subset=['vehicle_id'])
    return df

#Create empty straming table(target)
dp.create_streaming_table("dim_vehicle")
dp.create_auto_cdc_flow(
    target = "dim_vehicle",
    source = "dim_vehicle_view",
    keys = ["vehicle_id"],
    sequence_by = "vehicle_id",
    stored_as_scd_type = 1
)
#Dim driver End


#Dim payment_method (Type-1)Start

#create source view
@dp.view
def dim_payment_method_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("payment_method_id","payment_method","is_card","requires_auth")
    df = df.dropDuplicates(subset=['payment_method_id'])
    return df

#Create empty straming table(target)
dp.create_streaming_table("dim_payment_method")
dp.create_auto_cdc_flow(
    target = "dim_payment_method",
    source = "dim_payment_method_view",
    keys = ["payment_method_id"],
    sequence_by = "payment_method_id",
    stored_as_scd_type = 1
)
#Dim payment_method End



#Dim bookings (Type-1)Start

#create source view
@dp.view
def dim_bookings_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("ride_id","confirmation_number","dropoff_location_id","ride_status_id","dropoff_city_id","cancellation_reason_id","dropoff_address","dropoff_latitude","dropoff_longitude","booking_timestamp","dropoff_timestamp","pickup_address","pickup_latitude","pickup_longitude","pickup_location_id")
    df = df.dropDuplicates(subset=['ride_id'])
    return df

#Create empty straming table(target)
dp.create_streaming_table("dim_bookings")
dp.create_auto_cdc_flow(
    target = "dim_bookings",
    source = "dim_bookings_view",
    keys = ["ride_id"],
    sequence_by = "ride_id",
    stored_as_scd_type = 1
)
#Dim bookings End

#Dim Location (Type-2)Start

#create source view
@dp.table
def dim_location_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("pickup_city_id","pickup_city","region","state","city_updated_at",)
    df = df.dropDuplicates(subset=['pickup_city_id','city_updated_at'])
    return df

#Create empty straming table(target)
dp.create_streaming_table("dim_location")
dp.create_auto_cdc_flow(
    target = "dim_location",
    source = "dim_location_view",
    keys = ["pickup_city_id"],
    sequence_by = "city_updated_at",
    stored_as_scd_type = 2
)
#Dim bookings End

# Create Fact table Start

#create source table
@dp.view
def fact_uber_view():
    df = spark.readStream.table("silver_obt")
    df = df.select("ride_id","pickup_city_id","driver_id","passenger_id","payment_method_id","vehicle_id","distance_miles","duration_minutes","base_fare","distance_fare","time_fare","surge_multiplier","subtotal","tip_amount","total_fare","rating","base_rate","per_mile","per_minute")
    df = df.dropDuplicates(subset = ["ride_id"])
    return df

#Create empty straming table(target)
dp.create_streaming_table("fact_uber")
dp.create_auto_cdc_flow(
    target = "fact_uber",
    source = "fact_uber_view",
    keys = ["ride_id","pickup_city_id","driver_id","passenger_id","payment_method_id","vehicle_id"],
    sequence_by = "ride_id",
    stored_as_scd_type = 1
)
#Dim bookings End

# Create Fact table End