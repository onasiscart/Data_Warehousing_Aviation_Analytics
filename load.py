import pandas as pd
from tqdm import tqdm
from pygrametl.datasources import CSVSource, PandasSource
from pygrametl.tables import CachedDimension, FactTable
from dw import DW


def load(dw: DW, data: dict[str, pd.DataFrame | CSVSource]):
    """
    Carrega les dades transformades dins del DW.
    dw: objecte DW del dw.py
    data: dict de DataFrames o CSVSource
    """
    for name, dataset in data.items():
        # Comprovar que hi ha taula associada al DW
        if not any(hasattr(dw, f"{name}_{suffix}") for suffix in ("dim", "fact")):
            print(f"[WARN] Dataset '{name}' no té taula associada al DW.")
            continue

        # Trobar automàticament la taula (dim o fact)
        table = getattr(dw, f"{name}_dim", None) or getattr(dw, f"{name}_fact")

        # Convertir dataset a iterable de dicts
        if isinstance(dataset, pd.DataFrame):
            iterator = PandasSource(dataset)
            total = len(dataset)
        elif isinstance(dataset, CSVSource):
            iterator = iter(dataset)
            total = None
        else:
            raise TypeError(f"Dataset '{name}' no és DataFrame ni CSVSource.")

        # Inserció fila per fila
        for row in tqdm(iterator, total=total, desc=f"Loading {name}"):
            try:
                if isinstance(table, CachedDimension):
                    table.ensure(row)
                elif isinstance(table, FactTable):
                    table.insert(row)  # FactTable només suporta insert
                else:
                    raise TypeError(f"Taula '{name}' desconeguda.")
            except Exception as e:
                print(f"[ERROR] Error carregant fila a '{name}': {e}")
                continue

    # Commit final
    dw.conn_pygrametl.commit()
    print("LOAD completed successfully")
