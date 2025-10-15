from tqdm import tqdm
import pandas as pd
import duckdb
from pygrametl.tables import CachedDimension, FactTable
from pygrametl.datasources import CSVSource
from typing import Dict, Union

# Configuració general
BATCH_SIZE = 1000
DUCKDB_FILENAME = "dw.duckdb"


def establish_connection(create=False) -> pygrametl.ConnectionWrapper:
    """
    Retorna una connexió pygrametl a DuckDB.
    - Si create=True, crea un nou fitxer dw.duckdb (sobreescrivint si existeix)
    """

    try:
        conn_duckdb = duckdb.connect(DUCKDB_FILENAME)
        print(f"Connected to DuckDB database '{DUCKDB_FILENAME}' successfully.")
    except duckdb.Error as e:
        raise RuntimeError(
            f"Unable to connect to DuckDB database '{DUCKDB_FILENAME}': {e}"
        ) from e

    return pygrametl.ConnectionWrapper(conn_duckdb)


def load(data: Dict[str, Union[pd.DataFrame, CSVSource]]) -> None:
    """
    Carrega les dades transformades al data warehouse.
    Prec: Les taules ja han d'existir al DuckDB.
    """
    conn = establish_connection(create=False)

    # ================= Dimensions
    aircraft_dim = CachedDimension(
        name="Aircrafts",
        key="aircraftregistration",
        attributes=["model", "manufacturer"],
        targetconnection=conn,
    )

    date_dim = CachedDimension(
        name="Dates",
        key="date",
        attributes=["month", "year"],
        targetconnection=conn,
    )

    airport_dim = CachedDimension(
        name="Airports",
        key="airportcode",
        attributes=[],
        targetconnection=conn,
    )

    # ================= Fact tables
    daily_flight_fact = FactTable(
        name="DailyFlightStats",  # nom de la taula
        keyrefs=("date", "aircraftregistration"),  # clau composta (FK a dimensions)
        measures=(
            "flight_hours",
            "takeoffs",
            "ADOSS",
            "ADOSU",
            "delays",
            "cancellations",
            "delayed_minutes",
        ),
        targetconnection=conn,
    )

    total_maintenance_fact = FactTable(
        name="TotalMaintenanceReports",
        keyrefs=("airportcode", "aircraftregistration"),
        measures=("reports",),
        targetconnection=conn,
    )

    # ================= Mapatge dataset -> objecte ETL
    table_mapping = {
        "Aircraft": aircraft_dim,
        "Date": date_dim,
        "Airport": airport_dim,
        "DailyFlightStats": daily_flight_fact,
        "TotalMaintenanceReports": total_maintenance_fact,
    }

    for name, dataset in data.items():
        if name not in table_mapping:
            print(f"[WARN] No table defined for dataset '{name}', skipping...")
            continue

        table = table_mapping[name]
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

            # Dimensions: usar ensure() per evitar duplicats
            if isinstance(table, CachedDimension):
                table.ensure(row)
            # Fact tables: acumulem en batch
            else:
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    table.insertmany(batch)
                    conn.commit()
                    batch = []

        # Inserim la resta del batch per fact tables
        if isinstance(table, FactTable) and batch:
            table.insertmany(batch)
            conn.commit()

    conn.close()
    print("LOAD completed successfully.")
