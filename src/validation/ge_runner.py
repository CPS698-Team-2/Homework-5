import pandas as pd
import great_expectations as gx

def validate_sensor_batch(df: pd.DataFrame):

    context = gx.get_context()

    datasource = context.sources.add_or_update_pandas(name="pandas_ds")
    asset = datasource.add_dataframe_asset(name="sensors_asset")
    batch_request = asset.build_batch_request(dataframe=df)

    suite_name = "sensor_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except:
        suite = context.add_expectation_suite(suite_name)

    v = context.get_validator(batch_request=batch_request, expectation_suite=suite)

    # ranges
    v.expect_column_values_to_be_between("temperature_f", -50, 150, mostly=0.98)
    v.expect_column_values_to_be_between("humidity_pct", 0, 100, mostly=0.98)
    v.expect_column_values_to_be_between("wind_mph", 0, 200, mostly=0.98)

    # null % checks
    v.expect_column_values_to_not_be_null("sensor_id", mostly=0.99)
    v.expect_column_values_to_not_be_null("timestamp", mostly=0.99)

    results = v.validate()
    return results["success"], results