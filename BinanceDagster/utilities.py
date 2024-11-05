import requests
import hashlib
import zipfile
import io


def download_file_to_memory(url: str) -> io.BytesIO:
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for failed requests
    print(f"Downloaded {url} into memory.")
    return io.BytesIO(response.content)


def download_checksum_file(url: str) -> str:
    response = requests.get(url)
    response.raise_for_status()
    checksum = response.text.split()[0]
    print(f"Downloaded checksum from {url}.")
    return checksum


def verify_checksum(file_data: io.BytesIO, expected_checksum: str) -> bool:
    sha256_hash = hashlib.sha256()
    file_data.seek(0)  # Ensure we're at the start of the BytesIO stream
    for byte_block in iter(lambda: file_data.read(4096), b""):
        sha256_hash.update(byte_block)
    file_data.seek(0)  # Reset for future reads if needed

    calculated_checksum = sha256_hash.hexdigest()
    is_valid = calculated_checksum == expected_checksum
    print(f"Checksum verification {'passed' if is_valid else 'failed'}.")
    return is_valid


def decompress_zip_in_memory(file_data: io.BytesIO, extract_to: str = ".") -> None:
    """
    NOTE: this will extract "entire folder", so you might want to simply provide extract_to as a directory path
    """
    with zipfile.ZipFile(file_data, "r") as zip_ref:
        zip_ref.extractall(extract_to)
    print(f"Decompressed file to {extract_to}.")
