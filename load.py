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
    # Definir ordre de càrrega: dimensions primer, fets després
    dimension_tables = ['date', 'aircraft', 'airport']
    fact_tables = ['daily_aircraft', 'total_maintenance']
    
    # Carregar dimensions primer
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
            iterator = dataset.to_dict('records')
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
    
    # COMMIT després de carregar totes les dimensions
    print("Committing dimensions...")
    dw.conn_pygrametl.commit()
    
    # Carregar taules de fets
    for name in fact_tables:
        if name not in data:
            continue
        
        dataset = data[name]
        
        # Comprovar que hi ha taula associada al DW
        if not hasattr(dw, f"{name}_fact"):
            print(f"[WARN] Fact table '{name}' no té taula associada al DW.")
            continue
        
        table = getattr(dw, f"{name}_fact")
        
        # Convertir dataset a iterable de dicts
        if isinstance(dataset, pd.DataFrame):
            iterator = dataset.to_dict('records')
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
                table.insert(row)
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"[ERROR] Error carregant fila a '{name}': {e}")
                continue
        
        if error_count > 0:
            print(f"[WARN] {name}: {error_count} errors totals")
    
    # Commit final
    print("Committing fact tables...")
    dw.conn_pygrametl.commit()
    print("LOAD completed successfully")