from __future__ import annotations

import dagster as dg
import matplotlib.pyplot as plt
from dagster_duckdb import DuckDBResource


@dg.asset(
    deps=["pt_townsand_clean"],
)
def error_plot(database: DuckDBResource) -> None:
    """ PNG plot of water level with color coded errors. """
    with database.get_connection() as conn:
        pt_df = conn.execute("SELECT * FROM pt_townsand_clean").fetch_df()

    fig, ax = plt.subplots(figsize=(10, 10))

    # plot as report
    pt_df.loc[pt_df['flag_summary'] == 0].plot.scatter(x='datetime', y='value', ax=ax, s=7, label='good')
    pt_df.loc[pt_df['scatter'] != 0].plot.scatter(x='datetime', y='value', ax=ax, color='red', s=1, label='scatter')
    pt_df.loc[pt_df['flat'] != 0].plot.scatter(x='datetime', y='value', ax=ax, color='yellow', s=1, label='flat')
    pt_df.loc[pt_df['rate_of_change'] != 0].plot.scatter(x='datetime', y='value', ax=ax, color='green', s=1, label='rate_of_change')
    pt_df.loc[pt_df['bounds'] != 0].plot.scatter(x='datetime', y='value', ax=ax, color='cyan', s=1, label='bounds')
    ax.legend()
    ax.tick_params("x", rotation=45)
    ax.set_ylabel('Water Level (ft)')
    ax.set_xlabel('Time')

    plt.savefig("data/plots/port_townsand.png", format="png", bbox_inches="tight")
    plt.close(fig)
