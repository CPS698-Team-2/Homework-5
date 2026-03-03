import pandas as pd
import great_expectations as gx

def validate_sensor_dataframe(df: pd.DataFrame):
    """
    Great Expectations validation using GX context API (works with GE 0.18.x).
    Returns: (success: bool, results: dict)
    """
    context = gx.get_context()

    datasource = context.sources.add_or_update_pandas(name="pandas_ds")
    asset = datasource.add_dataframe_asset(name="sensor_asset")
    batch_request = asset.build_batch_request(dataframe=df)

    suite_name = "sensor_suite"
    try:
        suite = context.get_expectation_suite(suite_name)
    except Exception:
        suite = context.add_expectation_suite(suite_name)

    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )

    # Expectations required in assignment
    validator.expect_column_values_to_not_be_null("sensor_id", mostly=0.99)
    validator.expect_column_values_to_not_be_null("timestamp", mostly=0.99)
    validator.expect_column_values_to_be_between("temperature_f", -50, 150, mostly=0.98)
    validator.expect_column_values_to_be_between("humidity_pct", 0, 100, mostly=0.98)
    validator.expect_column_values_to_be_between("wind_mph", 0, 200, mostly=0.98)

    results = validator.validate()
    return results["success"], results