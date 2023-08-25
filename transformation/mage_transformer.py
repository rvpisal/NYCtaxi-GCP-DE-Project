import datetime as dt
import pandas as pd

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(uber_df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    
    # convert the object dates(happens if data is csv) to datetime
    uber_df['tpep_pickup_datetime'] = pd.to_datetime(uber_df['tpep_pickup_datetime'])
    uber_df['tpep_dropoff_datetime'] = pd.to_datetime(uber_df['tpep_dropoff_datetime'])

    dim_datetime = uber_df[['tpep_pickup_datetime','tpep_dropoff_datetime']].drop_duplicates().reset_index(drop=True)

    # Add other needed columns:
    # dim datetime
    dim_datetime['pick_hour'] = dim_datetime['tpep_pickup_datetime'].dt.hour
    dim_datetime['pick_day'] = dim_datetime['tpep_pickup_datetime'].dt.day
    dim_datetime['pick_month'] = dim_datetime['tpep_pickup_datetime'].dt.month
    dim_datetime['pick_year'] = dim_datetime['tpep_pickup_datetime'].dt.year
    dim_datetime['pick_weekday'] = dim_datetime['tpep_pickup_datetime'].dt.weekday
    dim_datetime['drop_hour'] = dim_datetime['tpep_pickup_datetime'].dt.hour
    dim_datetime['drop_day'] = dim_datetime['tpep_pickup_datetime'].dt.day
    dim_datetime['drop_month'] = dim_datetime['tpep_pickup_datetime'].dt.month
    dim_datetime['drop_year'] = dim_datetime['tpep_pickup_datetime'].dt.year
    dim_datetime['drop_weekday'] = dim_datetime['tpep_pickup_datetime'].dt.weekday
    dim_datetime['drop_weekday'] = dim_datetime['tpep_pickup_datetime'].dt.weekday

    #PK
    dim_datetime ['datetime_id'] = dim_datetime.index + 1

    dim_datetime = dim_datetime[['datetime_id', 'tpep_pickup_datetime', 'pick_hour', 'pick_day', 'pick_month', 'pick_year', 'pick_weekday', 'tpep_dropoff_datetime', 'drop_hour', 'drop_day', 'drop_month', 'drop_year', 'drop_weekday']]

    dim_passenger_count = uber_df[['passenger_count']].drop_duplicates().reset_index(drop=True)
    dim_passenger_count['passenger_count_id'] = dim_passenger_count.index + 1
    dim_passenger_count = dim_passenger_count[['passenger_count_id','passenger_count']]

    dim_trip_distance = uber_df[['trip_distance']].drop_duplicates().reset_index(drop=True)
    dim_trip_distance['trip_distance_id'] = dim_trip_distance.index + 1
    dim_trip_distance = dim_trip_distance[['trip_distance_id','trip_distance']]

    rate_code_type = {
    1:"Standard rate",
    2:"JFK",
    3:"Newark",
    4:"Nassau or Westchester",
    5:"Negotiated fare",
    6:"Group ride"
    }

    dim_ratecode = uber_df[['RatecodeID']].drop_duplicates().reset_index(drop=True)
    dim_ratecode['rate_code_id'] = dim_ratecode.index + 1 #PK
    dim_ratecode['rate_code_name'] = dim_ratecode ['RatecodeID'].map(rate_code_type)
    dim_ratecode = dim_ratecode [['rate_code_id','RatecodeID','rate_code_name']]

    payment_type_name = {
    1:"Credit card",
    2:"Cash",
    3:"No charge",
    4:"Dispute",
    5:"Unknown",
    6:"Voided trip"
    }
    dim_payment_type = uber_df[['payment_type']].drop_duplicates().reset_index(drop=True)
    dim_payment_type ['payment_type_id'] = dim_payment_type.index + 1
    dim_payment_type ['payment_type_name'] = dim_payment_type['payment_type'].map(payment_type_name)
    dim_payment_type = dim_payment_type [['payment_type_id','payment_type','payment_type_name']]

    dim_pick_location = uber_df [['PULocationID']].drop_duplicates().reset_index(drop=True)
    dim_pick_location['pick_location_id'] = dim_pick_location.index + 1
    dim_pick_location = dim_pick_location [['pick_location_id','PULocationID']]

    dim_drop_location = uber_df [['DOLocationID']].drop_duplicates().reset_index(drop=True)
    dim_drop_location['drop_location_id'] = dim_drop_location.index + 1
    dim_drop_location = dim_drop_location [['drop_location_id','DOLocationID']]

    vendor_name = {
    1:'Creative Mobile Technologies',
    2:'VeriFone Inc.'
    }

    dim_vendor = uber_df [['VendorID']].drop_duplicates().reset_index(drop=True)
    dim_vendor['vendor_id'] = dim_vendor.index + 1
    dim_vendor ['vendor_name'] = dim_vendor['VendorID'].map(vendor_name)
    dim_vendor = dim_vendor[['vendor_id','VendorID','vendor_name']]

    fact_table = uber_df.merge (dim_passenger_count,on='passenger_count')\
                    .merge (dim_trip_distance, on='trip_distance')\
                    .merge (dim_ratecode, on='RatecodeID')\
                    .merge (dim_payment_type, on='payment_type')\
                    .merge (dim_pick_location, on='PULocationID')\
                    .merge (dim_drop_location, on='DOLocationID')\
                    .merge (dim_datetime, on=['tpep_pickup_datetime','tpep_dropoff_datetime'])\
                    .merge (dim_vendor, on='VendorID')\
                    [['vendor_id', 'datetime_id', 'pick_location_id', 'drop_location_id', 'passenger_count_id', 'rate_code_id',
                        'payment_type_id', 'trip_distance_id', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount',
                        'improvement_surcharge', 'congestion_surcharge', 'airport_fee']]

    # print (fact_table)
    #convert tables into dictionaries to pass to bigquery

    return {"dim_datetime":dim_datetime.to_dict(orient = "dict"),
    "dim_passenger_count":dim_passenger_count.to_dict(orient = "dict"),
    "dim_trip_distance":dim_trip_distance.to_dict(orient = "dict"),
    "dim_ratecode":dim_ratecode.to_dict(orient = "dict"),
    "dim_payment_type":dim_payment_type.to_dict(orient = "dict"),
    "dim_pick_location":dim_pick_location.to_dict(orient = "dict"),
    "dim_drop_location":dim_drop_location.to_dict(orient = "dict"),
    "dim_vendor":dim_vendor.to_dict(orient = "dict"),
    "fact_table":fact_table.to_dict(orient = "dict")
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
