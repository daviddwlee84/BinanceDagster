from dagster import asset
from .utilities import (
    download_checksum_file,
    download_file_to_memory,
    verify_checksum,
    decompress_zip_in_memory,
)


@asset
def btc_klines_1m_daily() -> None:
    """
    BTC 1-minute interval K-lines

    NOTE: this is the simplest hard-coded show case, and need to be generalized
    """
    path = "data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-2024-11-02"
    # URLs to download
    file_url = f"https://data.binance.vision/{path}.zip"
    checksum_url = file_url + ".CHECKSUM"

    # Start the download, verification, and decompression process
    expected_checksum = download_checksum_file(checksum_url)

    retries = 3
    extract_to = f"data/binance/{path}.csv"

    for attempt in range(retries):
        print(f"Attempt {attempt + 1} of {retries}...")

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
                print("Checksum failed, retrying download...")
