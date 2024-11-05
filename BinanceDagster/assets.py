from dagster import asset, AssetExecutionContext
import os
from .utilities import (
    download_checksum_file,
    download_file_to_memory,
    verify_checksum,
    decompress_zip_in_memory,
)
from .partitions import daily_partition


@asset(partitions_def=daily_partition)
def btc_klines_1m_daily(context: AssetExecutionContext) -> None:
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
