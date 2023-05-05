from coinmetrics.api_client import CoinMetricsClient
import pandas as pd

fmtt = '%Y-%m-%dT%H:%M:%S'

def getMetric(asset,metric,date_start,date_end):

    # this function grabs the metric for asset within the specified date range,
    # removes the timezone, sets the date as an index and changes the column name to
    # the name of the metric
    client = CoinMetricsClient()
    frequency = "1d"
    data = client.get_asset_metrics(
        assets=asset,
        metrics=metric,
        frequency=frequency,
        start_time=date_start.strftime("%Y-%m-%d"),
        end_time=date_end.strftime("%Y-%m-%d")
        ).to_dataframe()

    data["time"] = data['time'].dt.tz_convert(None)
    data = data.rename(columns={"time": "date"})
    data[metric] = data[metric].to_numpy()
    output = data[['date',metric]].copy()
    output = output.sort_values(by=['date'])
    output = output.set_index('date')
    output.index = pd.to_datetime(output.index, utc=True, format=fmtt, errors='ignore')
    # purge
    del data
    # return output data
    return output