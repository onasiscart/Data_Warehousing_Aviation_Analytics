import pygrametl
from tqdm import tqdm
import pandas as pd
from pygrametl.datasources import CSVSource
from pygrametl.tables import CachedDimension, FactTable
import duckdb

BATCH_SIZE = 1000


def establish_connection() -> duckdb.DuckDBPyConnection:
    """Establish a connection to the DuckDB database."""
    duckdb_filename = "dw.duckdb"
    try:
        # Connexió al fitxer dw.duckdb (la base de dades és el fitxer)
        conn_duckdb = duckdb.connect(duckdb_filename)
        print(f"Connected to DuckDB database '{duckdb_filename}' successfully.")
        conn_pygrametl = pygrametl.ConnectionWrapper(conn_duckdb)
        return conn_pygrametl

    except duckdb.Error as e:
        raise RuntimeError(
            f"Unable to connect to DuckDB database '{duckdb_filename}': {e}"
        ) from e


def load(data: dict[str, pd.DataFrame | CSVSource]) -> None:
    """
    Load the transformed data into the data warehouse.
    Prec: The dataframes must be transformed and ready to load, database tables created
    """
    conn = establish_connection()

    # potser s'haurà d'ajustar noms de les keys del diccionari
    # DailyFlightStats, Aircraft, Date, TotalMaintenanceReports, Airport
    # Definim les taules (mapa nom dataset -> Table)
    tables = {
        "Aircraft": Table(conn, table="Aircrafts", key="aircraftregistration"),
        "Date": Table(conn, table="Date", key="date"),
        "Airport": Table(conn, table="Airports", key="airportcode"),
        "TotalMaintenanceReports": Table(
            conn,
            table="TotalMaintenanceReports",
            key=("airportcode", "aircraftregistration"),
        ),
        "DailyFlightStats": Table(
            conn, table="DailyFlightStats", key=("date", "aircraftregistration")
        ),
    }

    for name, dataset in data.items():
        table = tables[name]
        batch = []

        if isinstance(dataset, pd.DataFrame):
            iterator = dataset.iterrows()
            total = len(dataset)

        elif isinstance(dataset, CSVSource):
            iterator = iter(dataset)
            total = None
        else:
            raise TypeError(f"Dataset '{name}' is not a DataFrame or CSVSource.")

        for item in tqdm(iterator, total=total, desc=f"Loading {name}"):
            row = item[1].to_dict() if isinstance(dataset, pd.DataFrame) else item
            batch.append(row)

            if len(batch) >= BATCH_SIZE:
                table.insertmany(batch)
                conn.commit()
                batch = []

        # Insert rest of batch
        if batch:
            table.insertmany(batch)
            conn.commit()

    conn.close()
    print("LOAD completed successfully.")
