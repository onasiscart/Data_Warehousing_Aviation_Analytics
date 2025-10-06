import os
import sys
import duckdb  # https://duckdb.org
import pygrametl  # https://pygrametl.org
from pygrametl.tables import CachedDimension, SnowflakedDimension, FactTable


duckdb_filename = "dw.duckdb"


class DW:
    def __init__(self, create=False):
        if create and os.path.exists(duckdb_filename):
            os.remove(duckdb_filename)
        try:
            self.conn_duckdb = duckdb.connect(duckdb_filename)
            print("Connection to the DW created successfully")
        except duckdb.Error as e:
            print(f"Unable to connect to DuckDB database '{duckdb_filename}':", e)
            sys.exit(1)

        if create:
            try:
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE daily_flight_info (
                        flight_date DATE,
                        aircraftreg VARCHAR(6),
                        takeoffs INT NOT NULL,
                        flight_hours REAL NOT NULL,
                        CONSTRAINT pk_daily_flight_info PRIMARY KEY (flight_date, aircraftreg),
                        CONSTRAINT fk_aircraft FOREIGN KEY (aircraftreg) REFERENCES aircraft(aircraftreg)
                    );
                """
                )
                print("daily_flight_info created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE aircraft (
                        aircraftreg VARCHAR(6),
                        model VARCHAR(100) NOT NULL,
                        manufacturer VARCHAR(100) NOT NULL,
                        CONSTRAINT pk_aircraft PRIMARY KEY (aircraftreg)
                    );
                """
                )
                print("aircraft created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE monthly_aircraft_stats(
                        aircraftreg VARCHAR(6),
                        month INT NOT NULL,
                        ADOSS REAL NOT NULL DEFAULT 0,
                        ADOSU REAL NOT NULL DEFAULT 0,
                        DY INT NOT NULL DEFAULT 0,
                        CN INT NOT NULL DEFAULT 0,
                        DY_duration REAL NOT NULL DEFAULT 0,
                        Pilot_reports INT NOT NULL DEFAULT 0,
                        Maintenance_reports INT NOT NULL DEFAULT 0,
                        CONSTRAINT pk_monthly_aircraft_stats PRIMARY KEY (aircraftreg, month),
                        CONSTRAINT fk_aircraft FOREIGN KEY (aircraftreg) REFERENCES aircraft(aircraftreg)    
                    );
                """
                )
                print("monthly_aircraft_stats created successfully")

                self.conn_duckdb.commit()

            except duckdb.Error as e:
                print("Error creating the DW tables:", e)
                sys.exit(2)

        # Link DuckDB and pygrametl
        self.conn_pygrametl = pygrametl.ConnectionWrapper(self.conn_duckdb)

        # ======================================================================================================= Dimension and fact table objects
        # TODO: Declare the dimensions and facts for pygrametl

    # TODO: Rewrite the queries exemplified in "extract.py"
    def query_utilization(self):
        result = self.conn_duckdb.execute(
            """
            SELECT ...
            """
        ).fetchall()
        return result

    def query_reporting(self):
        result = self.conn_duckdb.execute(
            """
            SELECT ...
            """
        ).fetchall()
        return result

    def query_reporting_per_role(self):
        result = self.conn_duckdb.execute(
            """
            SELECT ...
            """
        ).fetchall()
        return result

    def close(self):
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()
