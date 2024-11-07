from dagster import (
    asset,
    AssetExecutionContext,
    AssetSpec,
    MaterializeResult,
    MetadataValue,
)
import os
import pandas as pd
from .utilities import (
    download_checksum_file,
    download_file_to_memory,
    verify_checksum,
    decompress_zip_in_memory,
)
from .partitions import daily_partition
from .configs import AdhocRequestConfig
import base64
import plotly.graph_objects as go
import plotly.io as pio

# TODO: categorize assets into groups => maybe move to "asset" folder and assign group name by batch (i.e. load_assets_from_modules)

# https://github.com/dagster-io/dagster/discussions/20054
# https://github.com/dagster-io/dagster/discussions/18211
# BUG: somehow it doesn't have "partition" tab
raw_btc_klines_1m_daily = AssetSpec(
    key="raw_btc_klines_1m_daily",
    partitions_def=daily_partition,
    description="Raw BTC 1-minute interval K-lines from Binance (external asset).",
)


@asset(partitions_def=daily_partition, deps=[raw_btc_klines_1m_daily])
def btc_klines_1m_daily(context: AssetExecutionContext) -> MaterializeResult:
    """
    BTC 1-minute interval K-lines

    NOTE: this is the simplest hard-coded show case, and need to be generalized
    """
    # TODO: the latest file might be only available 2 days later
    partition_date_str = context.partition_key

    path = f"data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-{partition_date_str}"
    # URLs to download
    file_url = f"https://data.binance.vision/{path}.zip"
    checksum_url = file_url + ".CHECKSUM"

    # Start the download, verification, and decompression process
    expected_checksum = download_checksum_file(checksum_url)

    retries = 3
    # NOTE: somehow zipfile will unzip into folder
    extract_to = os.path.dirname(f"data/binance/{path}.csv")

    for attempt in range(retries):
        context.log.info(f"Attempt {attempt + 1} of {retries}...")

        # Download the ZIP file in memory
        file_data = download_file_to_memory(file_url)

        # Verify checksum
        if verify_checksum(file_data, expected_checksum):
            # If checksum is correct, decompress the file
            decompress_zip_in_memory(file_data, extract_to)
            break
        else:
            if attempt == retries - 1:
                raise ValueError(
                    f"Failed to verify {file_url} after {retries} attempts."
                )
            else:
                context.log.warning("Checksum failed, retrying download...")

    df = pd.read_csv(f"data/binance/{path}.csv", header=None)
    return MaterializeResult(metadata={"Number of records": MetadataValue.int(len(df))})


@asset(deps=["btc_klines_1m_daily"])
def adhoc_btc_klines_1m(
    context: AssetExecutionContext, config: AdhocRequestConfig
) -> MaterializeResult:
    """
    This job will combine multiple daily BTC klines into one single file
    """
    results = []
    context.log.info(f"BTCUSDT-1m-{config.start_date}_{config.end_date}")
    dates = pd.date_range(config.start_date, config.end_date)
    assert len(dates) > 0, "Got empty date range, please check your config."
    for date_str in dates.astype(str):
        context.log.info(f"Processing {date_str}")
        path = (
            f"data/binance/data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-{date_str}.csv"
        )
        results.append(pd.read_csv(path, index_col=None, header=None))
    df = pd.concat(results, axis=0)
    df.columns = [
        "Open time",
        "Open",
        "High",
        "Low",
        "Close",
        "Volume",
        "Close time",
        "Quote asset volume",
        "Number of trades",
        "Taker buy base asset volume",
        "Taker buy quote asset volume",
        "Ignore",
    ]

    # Convert time columns to datetime
    df["Open time"] = pd.to_datetime(df["Open time"], unit="ms")
    df["Close time"] = pd.to_datetime(df["Close time"], unit="ms")

    # Generate candlestick chart using Plotly
    fig = go.Figure(
        data=[
            go.Candlestick(
                x=df["Open time"],
                open=df["Open"],
                high=df["High"],
                low=df["Low"],
                close=df["Close"],
                increasing_line_color="green",
                decreasing_line_color="red",
            )
        ]
    )
    fig.update_layout(
        title=f"BTCUSDT 1m Candlestick Chart ({config.start_date} to {config.end_date})",
        xaxis_title="Time",
        yaxis_title="Price",
    )

    # Save the chart as a PNG file
    file_path = f"data/adhoc/BTCUSDT-1m-{config.start_date}_{config.end_date}.png"
    pio.write_image(fig, file_path)

    # Convert image to base64 and create markdown preview
    with open(file_path, "rb") as image_file:
        image_data = image_file.read()

    base64_data = base64.b64encode(image_data).decode("utf-8")
    md_content = f"![Candlestick Chart](data:image/png;base64,{base64_data})"

    # Save the combined data
    df.to_feather(
        f"data/adhoc/BTCUSDT-1m-{config.start_date}_{config.end_date}.feather"
    )

    return MaterializeResult(
        metadata={
            "Number of records": MetadataValue.int(len(df)),
            "preview": MetadataValue.md(md_content),
        }
    )
