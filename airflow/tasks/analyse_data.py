from airflow.decorators import task
import pandas as pd


@task
def plot_analysis(data):
    """Prints some statistics on the dataframe, as a sense check of the data.
    
    Args:
        data: The returned results from the access layer dataset.
    """
    print(data)
    df = pd.DataFrame(
        data=data, 
        columns=[
            "aggregation_period",
            "ticker",
            "opening_timestamp",
            "closing_timestamp",
            "num_trades",
            "total_volume",
            "average_volume",
            "average_price",
            "max_price",
            "min_price",
            "opening_price",
            "closing_price",
            "news"
        ]
    )

    print(df.describe)