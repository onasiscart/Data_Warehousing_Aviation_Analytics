import pandas as pd
from tqdm import tqdm
from pygrametl.datasources import CSVSource
from pygrametl.tables import CachedDimension
from dw import DW


def load_dimensions(
    dw: DW, data: dict[str, pd.DataFrame | CSVSource], dimension_tables: list[str]
):
    """
    Carrega les dimensions dins del DW.
    dw: objecte DW del dw.py
    data: dict de DataFrames o CSVSource
    dimension_tables: llista de noms de taules de dimensió
    """
    for name in dimension_tables:
        if name not in data:
            continue

        dataset = data[name]

        # Comprovar que hi ha taula associada al DW
        if not hasattr(dw, f"{name}_dim"):
            print(f"[WARN] Dimensió '{name}' no té taula associada al DW.")
            continue

        table = getattr(dw, f"{name}_dim")

        # Convertir dataset a iterable de dicts
        if isinstance(dataset, pd.DataFrame):
            iterator = dataset.to_dict("records")
            total = len(dataset)
        elif isinstance(dataset, CSVSource):
            iterator = iter(dataset)
            total = None
        else:
            raise TypeError(f"Dataset '{name}' no és DataFrame ni CSVSource.")

        # Inserció fila per fila
        error_count = 0
        for row in tqdm(iterator, total=total, desc=f"Loading {name}"):
            try:
                table.ensure(row)
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"[ERROR] Error carregant fila a '{name}': {e}")
                continue

        if error_count > 0:
            print(f"[WARN] {name}: {error_count} errors totals")


def load_daily_aircraft(dw: DW, dataset: pd.DataFrame):
    table = getattr(dw, "daily_aircraft_fact")
    iterator = dataset.to_dict("records")
    total = len(dataset)
    # Inserció fila per fila
    error_count = 0
    for row in tqdm(iterator, total=total, desc="Loading daily_aircraft"):
        aircraftid = dw.aircraft_dim.lookup(row)
        dateid = dw.date_dim.lookup(row)
        try:
            if aircraftid is not None and dateid is not None:
                row["aircraftid"] = aircraftid
                row["dateid"] = dateid
                table.insert(row)
        except Exception as e:
            error_count += 1
            if error_count <= 5:
                print(f"[ERROR] Error carregant fila a 'daily_aircraft': {e}")
            continue


def load_total_maintenance(dw: DW, dataset: pd.DataFrame):
    table = getattr(dw, "total_maintenance_fact")
    iterator = dataset.to_dict("records")
    total = len(dataset)
    # Inserció fila per fila
    error_count = 0
    for row in tqdm(iterator, total=total, desc="Loading total_maintenance"):
        aircraftid = dw.aircraft_dim.lookup(row)
        airportid = dw.airport_dim.lookup(row)
        try:
            if aircraftid is not None and airportid is not None:
                row["aircraftid"] = aircraftid
                row["airportid"] = airportid
                table.insert(row)
        except Exception as e:
            error_count += 1
            if error_count <= 5:
                print(f"[ERROR] Error carregant fila a 'total_maintenance': {e}")
            continue


def load(dw: DW, data: dict[str, pd.DataFrame | CSVSource]):
    """
    Carrega les dades transformades dins del DW.
    dw: objecte DW del dw.py
    data: dict de DataFrames o CSVSource
    """

    # load dimensions first
    dimension_tables = ["aircraft", "date", "airport"]
    load_dimensions(dw, data, dimension_tables)
    print("Committing dimensions...")
    dw.conn_pygrametl.commit()

    # load fact tables
    load_daily_aircraft(dw, data["daily_aircraft"])
    load_total_maintenance(dw, data["total_maintenance"])

    print("Committing fact tables...")
    dw.conn_pygrametl.commit()

    print("LOAD completed successfully")
