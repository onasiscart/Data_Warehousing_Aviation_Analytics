import pandas as pd
from tqdm import tqdm
from dw import DW
import logging

# Configure logging for information and errors
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

# ====================================================================================================================================
# loading functions


def load_dimensions(
    dw: DW,
    data: dict[str, pd.DataFrame],
    dimension_tables: list[str],
):
    """
    Prec: all dimension tables in dw match dimensions_tables names and these match dataframes in data
    Post: loads dimensions tables into the DW
    """
    for name in dimension_tables:
        # Make sure dataset is present
        if name not in data:
            raise RuntimeError(f"Dimension '{name}' not in data.")
        dataset = data[name]
        # Make sure the dimension table exists in the DW
        if not hasattr(dw, f"{name}_dim"):
            raise RuntimeError(f"Dimension table '{name}_dim' not found in DW.")
        table = getattr(dw, f"{name}_dim")
        # Insert row by row using iterators
        iterator = dataset.to_dict("records")  # type: ignore
        total = len(dataset)
        for row in tqdm(iterator, total=total, desc=f"Loading {name}"):
            try:
                table.ensure(row)
            except Exception as e:
                raise RuntimeError(f"Error loading tuple into '{name}': {e}") from e


def load_daily_aircraft(dw: DW, dataset: pd.DataFrame):
    """
    Prec: dataset contains daily_aircraft_fact data to load
    Post: loads daily_aircraft_fact table into the DW
    """
    table = getattr(dw, "daily_aircraft_fact")
    iterator = dataset.to_dict("records")
    total = len(dataset)
    # Insert row by row using iterators
    for row in tqdm(iterator, total=total, desc="Loading daily_aircraft"):
        aircraftid = dw.aircraft_dim.lookup(row)
        dateid = dw.date_dim.lookup(row)
        try:
            if aircraftid is not None and dateid is not None:
                row["aircraftid"] = aircraftid
                row["dateid"] = dateid
                table.insert(row)
        except Exception as e:
            raise RuntimeError(f"Error loading tuple into 'daily_aircraft': {e}") from e


def load_total_maintenance(dw: DW, dataset: pd.DataFrame):
    """
    Prec: dataset contains total_maintenance_fact data to load
    Post: loads total_maintenance_fact table into the DW
    """
    table = getattr(dw, "total_maintenance_fact")
    iterator = dataset.to_dict("records")
    total = len(dataset)
    # Insert row by row using iterators
    for row in tqdm(iterator, total=total, desc="Loading total_maintenance"):
        aircraftid = dw.aircraft_dim.lookup(row)
        airportid = dw.airport_dim.lookup(row)
        try:
            if aircraftid is not None and airportid is not None:
                row["aircraftid"] = aircraftid
                row["airportid"] = airportid
                table.insert(row)
        except Exception as e:
            raise RuntimeError(
                f"Error loading tuple into 'total_maintenance': {e}"
            ) from e


def load(dw: DW, data: dict[str, pd.DataFrame]):
    """
    Prec: dw: Data Warehouse object, data: dict of transformed data to load
    Post: loads data into DW using pygramETL methods
    """
    # load dimensions first
    try:
        dimension_tables = ["aircraft", "date", "airport"]
        load_dimensions(dw, data, dimension_tables)
    except Exception as e:
        logging.critical(f"Error loading dimensions: {e}")
        raise  # stop pipeline
    logging.info("Committing dimensions...")
    dw.conn_pygrametl.commit()

    # load fact tables
    try:
        load_daily_aircraft(dw, data["daily_aircraft"])
        load_total_maintenance(dw, data["total_maintenance"])
    except Exception as e:
        logging.critical(f"Error loading fact tables: {e}")
        raise  # stop pipeline
    logging.info("Committing fact tables...")
    dw.conn_pygrametl.commit()

    logging.info("Load completed successfully.")
