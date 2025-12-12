import pandas as pd
import tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from sources import unhcr, worldbank
from utils import UNHCR_DATA_PATH, WB_DATA_PATH

MAX_WORKERS = 20

def process_meta(input_file, fetch_function):
    """
    Process metadata by fetching detailed data for each ID in parallel.

    Parameters:
    - input_file (str): Path to the metadata CSV file.
    - fetch_function (callable): Function to fetch data for a single ID.

    Returns:
    - pd.DataFrame: Normalized DataFrame with all fetched data.
    """
    df = pd.read_csv(input_file)
    records = []

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_function, id): id for id in df["id"]}

        for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
            try:
                data = future.result()
                records.append(data)
            except Exception as e:
                print(f"An error occurred for ID {futures[future]}: {e}")

    normalized_df = pd.json_normalize(records)
    return normalized_df

def process_datasets(df, output_file):
    """
    Processes the dataset by normalizing nested JSON fields, renaming columns,
    and removing unnecessary columns.

    Args:
    - df (DataFrame): Metadata DataFrame.
    - output_file (str): Path to save the processed CSV file.

    Returns:
    None
    """
    try:
        patterns_to_remove = ["study_desc.", "doc_desc.", "study_info.", "method."]
        df.columns = df.columns.str.replace("|".join(patterns_to_remove), "", regex=True)
        df.columns = df.columns.str.replace("data_collection.", "method_")

        df.dropna(axis=1, how='all', inplace=True)
        if 'schematype' in df.columns:
            df.drop('schematype', axis=1, inplace=True)

        df.to_csv(output_file, index=False)
        print(f"Flattened dataset with shape {df.shape} saved to {output_file}")

    except Exception as e:
        print(f"Error processing dataframe: {e}")

def run():
    """Orchestrate fetching detailed datasets from all sources."""
    try:
        print(f"Fetching datasets from the World Bank MDL")
        input_file = WB_DATA_PATH + "metadata.csv"
        output_file = WB_DATA_PATH + "datasets.csv"
        rawdf = process_meta(input_file, worldbank.fetch_dataset)
        process_datasets(rawdf, output_file)
    except Exception as e:
        print(f"An error occurred with World Bank: {e}")

    try:
        print(f"Fetching datasets from the UNHCR MDL")
        input_file = UNHCR_DATA_PATH + "metadata.csv"
        output_file = UNHCR_DATA_PATH + "datasets.csv"
        rawdf = process_meta(input_file, unhcr.fetch_dataset)
        process_datasets(rawdf, output_file)
    except Exception as e:
        print(f"An error occurred with UNHCR: {e}")
