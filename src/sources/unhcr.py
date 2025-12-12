import requests
import pandas as pd
import logging
from utils import UNHCR_DATA_PATH

METADATA_LIST_URL = "https://microdata.unhcr.org/index.php/api/catalog/search?ps=9999999&sort_by=created&sort_order=desc"
DATASET_EXPORT_URL = "https://microdata.unhcr.org/index.php/metadata/export/{}/json"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def fetch_metadata_list():
    """
    Fetch metadata list from UNHCR API.

    Returns:
    - pd.DataFrame: The metadata as a pandas DataFrame.
    """
    try:
        response = requests.get(METADATA_LIST_URL)
        response.raise_for_status()
        data = response.json()
        data = data["result"]["rows"]
        return pd.DataFrame(data)
    except requests.RequestException as e:
        logging.error(f"UNHCR Request failed: {e}")
        raise
    except (ValueError, KeyError) as e:
        logging.error(f"UNHCR Failed to process JSON response: {e}")
        raise

def fetch_dataset(id):
    """
    Fetch detailed dataset data from UNHCR API for a specific ID.

    Parameters:
    - id: Dataset ID

    Returns:
    - dict: Dataset information
    """
    response = requests.get(DATASET_EXPORT_URL.format(id))
    response.raise_for_status()
    data = response.json()
    data["id"] = id
    return data
