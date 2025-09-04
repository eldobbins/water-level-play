from __future__ import annotations

import dagster as dg
import pandas as pd
import requests
from dagster_duckdb import DuckDBResource


@dg.asset
def pt_townsand_live(database: DuckDBResource) -> None:
    """
      Pulling real-time water level data for Port Townsand, WA. Loads into database
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


"""

# cleanup
df['datetime'] = pd.to_datetime(df['t'])
#df = df.drop(columns=['t'])
df = df.rename(columns={
    'v': 'value',
    's': 'sigma',
    'f': 'data_flags',
    'q': 'quality_level'
})
df['value'] = df['value'].astype(float)

# error flags
df[['scatter', 'flat', 'rate_of_change', 'bounds']] = df['data_flags'].str.split(',', expand=True).astype(int)
df['flag_summary'] = df['scatter'] + df['rate_of_change'] + df['flat'] + df['bounds']


# plot as report
ax = df.loc[df['flag_summary'] == 0].plot.scatter(x='datetime', y='value', s=7, label='good')
df.loc[df['scatter'] != 0].plot.scatter(x='datetime', y='value', ax=ax, color='red', s=1, label='scatter')
df.loc[df['flat'] != 0].plot.scatter(x='datetime', y='value', ax=ax, color='yellow', s=1, label='flat')
df.loc[df['rate_of_change'] != 0].plot.scatter(x='datetime', y='value', ax=ax, color='green', s=1, label='rate_of_change')
df.loc[df['bounds'] != 0].plot.scatter(x='datetime', y='value', ax=ax, color='cyan', s=1, label='bounds')
ax.legend()
ax.tick_params("x", rotation=45)
ax.set_ylabel('Water Level (ft)')
ax.set_xlabel('Time')
"""
