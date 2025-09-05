from __future__ import annotations

import dagster as dg
import pandas as pd
import requests
from dagster_duckdb import DuckDBResource
from duckdb import CatalogException


@dg.asset
def pt_townsand_live(database: DuckDBResource) -> None:
    """
      Pulling real-time water level data for Port Townsand, WA. Loads into database. Cleans table every time.
    """
    url = 'https://api.tidesandcurrents.noaa.gov/api/prod/datagetter?'\
        'date=today&station=9444900&'\
        'product=water_level&datum=MLLW&time_zone=gmt&units=english&'\
        'application=DataAPI_Sample&format=json'

    response = requests.get(url)
    pt_df = pd.json_normalize(response.json()['data'])
    assert len(pt_df) > 0
    # print(df.head())  # t	v	s	f	q

    query = "CREATE OR REPLACE TABLE pt_townsand_raw AS SELECT * FROM pt_df"

    with database.get_connection() as conn:
        conn.execute(query)


@dg.asset(
    deps=["pt_townsand_live"]
)
def pt_townsand_clean(database: DuckDBResource) -> None:
    """
      Appending a clean-up verion of the water level data into a database.
    """

    # get the raw data and the last time collected so far
    with database.get_connection() as conn:
        pt_df = conn.execute("SELECT * FROM pt_townsand_raw").fetch_df()
        last_time = conn.execute("select datetime from pt_townsand_clean order by datetime desc limit 1;").fetchone()[0]

    # cleanup
    pt_df['datetime'] = pd.to_datetime(pt_df['t'])
    #df = df.drop(columns=['t'])
    pt_df = pt_df.rename(columns={
        'v': 'value',
        's': 'sigma',
        'f': 'data_flags',
        'q': 'quality_level'
    })
    pt_df['value'] = pt_df['value'].astype(float)

    # cleanup error flags
    pt_df[['scatter', 'flat', 'rate_of_change', 'bounds']] = pt_df['data_flags'].str.split(',', expand=True).astype(int)
    pt_df['flag_summary'] = pt_df['scatter'] + pt_df['rate_of_change'] + pt_df['flat'] + pt_df['bounds']

    # limit new data to only times not collected yet
    pt_df = pt_df.loc[pt_df['datetime'] > last_time]

    query = "INSERT INTO pt_townsand_clean SELECT * FROM pt_df"
    with database.get_connection() as conn:
        try:
            conn.execute(query)
            # print("Appending cleaned data to an existing table.")
        except CatalogException:
            # print("Table does not exist. Creating table of cleaned data.")
            conn.execute("CREATE OR REPLACE TABLE pt_townsand_clean AS SELECT * FROM pt_df")
