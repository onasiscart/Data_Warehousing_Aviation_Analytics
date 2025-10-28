import pandas as pd
from tqdm import tqdm
from dw import DW
import logging
from pygrametl.datasources import TransformingSource, PandasSource

# Configure logging for information and errors
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# ====================================================================================================================================
# loading functions
def _close_csv_source(dataset: TransformingSource):
    """Helper to close underlying CSV file if present"""
    if hasattr(dataset, "source") and hasattr(dataset.source, "f"):
        dataset.source.f.close()  # type: ignore[attr-defined]


def load_aircrafts(dw: DW, dataset: TransformingSource):
    """
    Prec: dataset contains aircraft_dim data to load
    Post: loads aircraft_dim table into the DW
    """
    table = getattr(dw, "aircraft_dim")
    try:
        for row in tqdm(dataset, desc="Loading aircrafts"):
            try:
                table.ensure(row)
            except Exception as e:
                logging.critical(f"Error loading aircrafts dimension: {e}")
                raise e  # stop pipeline
        dw.conn_pygrametl.commit()
        logging.info("Finished loading aircrafts dimension.")
    finally:  # close the underlying source even if there is an error
        _close_csv_source(dataset)


def load_airports(dw: DW, dataset: PandasSource):
    """
    Prec: dataset contains airport_dim data to load
    Post: loads airport_dim table into the DW
    """
    table = getattr(dw, "airport_dim")
    try:
        for row in tqdm(dataset, desc="Loading airports"):
            try:
                table.ensure(row)
            except Exception as e:
                logging.critical(f"Error loading airports dimension: {e}")
                raise e  # stop pipeline
        dw.conn_pygrametl.commit()
        logging.info("Finished loading airports dimension.")
    finally:  # close the underlying source even if there is an error
        _close_csv_source(dataset)


def load_dates(dw: DW, dataset: PandasSource):
    """
    Prec: dataset contains date_dim data to load
    Post: loads date_dim table into the DW
    """
    table = getattr(dw, "date_dim")
    # Insert row by row using iterators
    for row in tqdm(dataset, desc="Loading dates"):
        try:
            table.ensure(row)
        except Exception as e:
            logging.critical(f"Error loading dates dimension: {e}")
            raise e  # stop pipeline in case of error!
    dw.conn_pygrametl.commit()
    logging.info("Finished loading dates dimension.")


def load_daily_aircraft(dw: DW, dataset: PandasSource):
    """
    Prec: dataset contains daily_aircraft_fact data to load
    Post: loads daily_aircraft_fact table into the DW
    """
    table = getattr(dw, "daily_aircraft_fact")
    # Insert row by row using iterators
    for row in tqdm(dataset, desc="Loading daily_aircraft"):
        aircraftid = dw.aircraft_dim.lookup(row)  # type: ignore
        dateid = dw.date_dim.lookup(row)  # type: ignore
        try:
            if aircraftid is not None and dateid is not None:
                row["aircraftid"] = aircraftid
                row["dateid"] = dateid
                table.insert(row)
        except Exception as e:
            logging.critical(f"Error loading daily_aircraft fact: {e}")
            raise e
    logging.info("Finished loading Daily Aircraft Stats fact table.")


def load_total_maintenance(dw: DW, dataset: PandasSource):
    """
    Prec: dataset contains total_maintenance_fact data to load
    Post: loads total_maintenance_fact table into the DW
    """
    table = getattr(dw, "total_maintenance_fact")
    # Insert row by row using iterators
    for row in tqdm(dataset, desc="Loading total_maintenance"):
        aircraftid = dw.aircraft_dim.lookup(row)  # type: ignore
        airportid = dw.airport_dim.lookup(row)  # type: ignore
        try:
            if aircraftid is not None and airportid is not None:
                row["aircraftid"] = aircraftid
                row["airportid"] = airportid
                table.insert(row)
        except Exception as e:
            raise RuntimeError(
                f"Error loading tuple into 'total_maintenance': {e}"
            ) from e
    logging.info("Finished loading Total Maintenance Reports fact table.")


def load_facts(dw: DW, facts: tuple[PandasSource, PandasSource]):
    """
    Prec: datasets contain fact data to load
    Post: loads fact tables into the DW
    """
    daily_flight_stats, total_maint_reports = facts
    try:
        load_daily_aircraft(dw, daily_flight_stats)
        load_total_maintenance(dw, total_maint_reports)
        dw.conn_pygrametl.commit()
        logging.info("Finished loading fact tables.")
    except Exception as e:
        logging.critical(f"Error loading fact tables: {e}")
        raise e  # stop pipeline
