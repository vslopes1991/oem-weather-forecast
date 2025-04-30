import os
import dotenv
import snowflake.connector
import pandas as pd
import openmeteo_requests
import requests_cache
from retry_requests import retry

dotenv.load_dotenv()

USER = os.getenv('USER')
ACCOUNT = os.getenv('ACCOUNT')
WAREHOUSE = os.getenv('WAREHOUSE')
DATABASE = os.getenv('DATABASE')
SCHEMA = os.getenv('SCHEMA')

conn = snowflake.connector.connect(
    user=USER,
    account=ACCOUNT,
    authenticator='externalbrowser',
    warehouse=WAREHOUSE,
    database=DATABASE,
    schema=SCHEMA
)

model_info = pd.DataFrame([{"NAME": "ecmwf_ifs025", "MODEL_ID": 60},
                           {"NAME": "ecmwf_aifs025_single", "MODEL_ID": 86},
                           {"NAME": "gfs_graphcast025", "MODEL_ID": 63},
                           {"NAME": "gfs_global", "MODEL_ID": 3},
                           {"NAME": "ukmo_global_deterministic_10km", "MODEL_ID": 80}])


variables_info = pd.DataFrame([{"NAME": "temperature_2m", "VAR_ID": 47},
                               {"NAME": "relative_humidity_2m", "VAR_ID": 29},
                               {"NAME": "precipitation_probability", "VAR_ID": 26},
                               {"NAME": "precipitation", "VAR_ID": 24},
                               {"NAME": "surface_pressure", "VAR_ID": 45},
                               {"NAME": "wind_speed_10m", "VAR_ID": 59},
                               {"NAME": "wind_direction_10m", "VAR_ID": 57},
                               {"NAME": "cloud_cover", "VAR_ID": 3}])


variables = variables_info["NAME"].to_list()


def get_tme():
    query = f"""
            SELECT 
                NM_TME, LATITUDE, LONGITUDE, ELEVATION
            FROM
                OEM_PROD.DEVICES.D_EOL_TME
            ORDER BY NM_TME
            """
    df = pd.read_sql(query, conn)
    df["ID"] = range(len(df))
    return df


def get_tme_historical(tme_info, start_date, end_date):
    query = f"""
            SELECT
                NM_TME, TIMESTAMP_UTC AS TIMESTAMP, WIND_SPD_TOP, WIND_DIR_TOP
            FROM
                ENERGY_PROD.TME.F_REAL_EOL_TME_FITTED_HOURLY
            WHERE 
                TIMESTAMP_UTC >= '{start_date:%Y-%m-%d}'
            AND
                TIMESTAMP_UTC <= '{end_date:%Y-%m-%d}'
            AND
                WS_FILLED_WITH = 'STATION'
            ORDER BY NM_TME, TIMESTAMP
            """
    df = pd.read_sql(query, conn)
    combined_df = pd.merge(tme_info[["NM_TME", "ID"]], df, on="NM_TME")
    return combined_df


def get_openmeteo_historical_forecast(tme_info):
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    final_variables = []
    for var in variables:
        variables_previous_runs = [f"{var}_previous_day{x}" for x in range(1, 7)]
        final_variables = final_variables + [var] + variables_previous_runs

    url = "https://previous-runs-api.open-meteo.com/v1/forecast"
    params = {
        "latitude": tme_info['LATITUDE'].to_list(),
        "longitude": tme_info['LONGITUDE'].to_list(),
        "hourly": final_variables,
        "models": ["ecmwf_ifs025", "ecmwf_aifs025_single", "gfs_graphcast025", "gfs_global",
                   "ukmo_global_deterministic_10km"],
        "cell_selection": "nearest",
        "wind_speed_unit": "ms",
        "past_days": 31
    }
    responses = openmeteo.weather_api(url, params=params)

    all_data = []
    for response in responses:
        hourly = response.Hourly()
        id = response.LocationId()
        model_id = response.Model()
        timestamp = pd.date_range(start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                                  end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                                  freq=pd.Timedelta(seconds=hourly.Interval()), inclusive="left")
        for var in range(hourly.VariablesLength()):
            hourly_data = {"ID": id,
                           "MODEL_ID": model_id,
                           "TIMESTAMP": timestamp,
                           "STEP": hourly.Variables(var).PreviousDay(),
                           "VAR_ID": hourly.Variables(var).Variable(),
                           "VALUES": hourly.Variables(var).ValuesAsNumpy()}
            all_data.append(pd.DataFrame(hourly_data))

    long_df = pd.concat(all_data, ignore_index=True)

    # To make variables as columns
    long_df.pivot(index=['ID', 'MODEL_ID', 'TIMESTAMP', 'STEP'], columns='VAR_ID', values='VALUES').reset_index()
    long_df["TIMESTAMP"] = long_df["TIMESTAMP"].dt.tz_localize(None)
    return long_df


def get_openmeteo_current_forecast(tme_info):
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": tme_info['LATITUDE'].to_list(),
        "longitude": tme_info['LONGITUDE'].to_list(),
        "hourly": variables,
        "models": ["ecmwf_ifs025", "ecmwf_aifs025_single", "gfs_graphcast025", "gfs_global",
                   "ukmo_global_deterministic_10km"],
        "cell_selection": "nearest",
        "wind_speed_unit": "ms"
    }

    responses = openmeteo.weather_api(url, params=params)

    all_data = []

    for response in responses:
        hourly = response.Hourly()
        hourly_data = {
            "ID": response.LocationId(),
            "MODEL_ID": response.Model(),
            "TIMESTAMP": pd.date_range(
                start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                freq=pd.Timedelta(seconds=hourly.Interval()),
                inclusive="left"
            )
        }
        for var in range(hourly.VariablesLength()):
            hourly_data[hourly.Variables(var).Variable()] = hourly.Variables(var).ValuesAsNumpy()
        all_data.append(pd.DataFrame(hourly_data))

    combined_df = pd.concat(all_data, ignore_index=True)

    long_df = pd.melt(combined_df, id_vars=["ID", "MODEL_ID", "TIMESTAMP"],
                      var_name="VARIABLE", value_name="VALUE")

    # plt.close("all")
    # for group in long_df.query("VARIABLE == 'relative_humidity_2m' and ID == 50").groupby("MODEL_ID"):
    #     plt.plot(group[1]["TIMESTAMP"], group[1]["VALUE"], label=group[0])
    # plt.legend()


tme_info = get_tme()
tme_historical_forecast = get_openmeteo_historical_forecast(tme_info)
tme_historical_obs = get_tme_historical(tme_info, tme_historical_forecast["TIMESTAMP"].min(),
                                        tme_historical_forecast["TIMESTAMP"].max())


