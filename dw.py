import os
import sys
import duckdb  # https://duckdb.org
import pygrametl  # https://pygrametl.org
from pygrametl.tables import CachedDimension, FactTable

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
                    CREATE TABLE Aircrafts (
                        aircraftregistration VARCHAR(6),
                        model VARCHAR(100) NOT NULL,
                        manufacturer VARCHAR(100) NOT NULL,
                        CONSTRAINT pk_aircraft PRIMARY KEY (aircraftregistration)
                    );
                """
                )
                print("aircraft created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE Date(
                        date DATE PRIMARY KEY,
                        month INT NOT NULL, --YYYYMM
                        year INT NOT NULL  --YYYY
                    );
                """
                )
                print("date created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE Airports(
                        airportcode VARCHAR(3) PRIMARY KEY,
                    );
                """
                )
                print("airports created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE DailyAircraftStats (
                        date DATE,
                        aircraftregistration VARCHAR(6),
                        takeoffs INT NOT NULL,
                        flighthours REAL NOT NULL,
                        ADOSS REAL NOT NULL DEFAULT 0,
                        ADOSU REAL NOT NULL DEFAULT 0,
                        delays INT NOT NULL DEFAULT 0,
                        cancellations INT NOT NULL DEFAULT 0,
                        delayduration REAL NOT NULL DEFAULT 0,
                        pilotreports INT NOT NULL DEFAULT 0,
                        maintenancereports INT NOT NULL DEFAULT 0,
                        CONSTRAINT pk_daily_flight_info PRIMARY KEY (date, aircraftregistration),
                        CONSTRAINT fk_aircraft FOREIGN KEY (aircraftregistration) REFERENCES Aircrafts(aircraftregistration),
                        CONSTRAINT fk_date FOREIGN KEY (date) REFERENCES Date(date)
                    );
                """
                )
                print("daily_flight_stats created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE TotalMaintenanceReports(
                        airportcode VARCHAR(3),
                        aircraftregistration VARCHAR(6),
                        reports INT,
                        PRIMARY KEY (airportcode, aircraftregistration),
                        FOREIGN KEY (airportcode) REFERENCES Airports(airportcode),
                        FOREIGN KEY (aircraftregistration) REFERENCES Aircrafts(aircraftregistration)
                    );
                """
                )
                print("total_maintenance_reports created successfully")
                self.conn_duckdb.commit()

            except duckdb.Error as e:
                print("Error creating the DW tables:", e)
                sys.exit(2)

        # Link DuckDB and pygrametl
        self.conn_pygrametl = pygrametl.ConnectionWrapper(self.conn_duckdb)

        # ======================================================================================================= Dimension and fact table objects
        # ================= Dimensions
        self.aircraft_dim = CachedDimension(
            name="Aircrafts",
            key="aircraftregistration",
            attributes=["model", "manufacturer"],
        )

        self.date_dim = CachedDimension(
            name="Date",
            key="date",
            attributes=["month", "year"],
        )

        self.airport_dim = CachedDimension(
            name="Airports",
            key="airportcode",
            attributes=[
                "airportcode"
            ],  # we add it as an attribute so pygrametl is happy
        )

        # ================= Fact tables
        self.daily_aircraft_fact = FactTable(
            name="DailyAircraftStats",  # nom de la taula
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
        )

        self.total_maintenance_fact = FactTable(
            name="TotalMaintenanceReports",
            keyrefs=("airportcode", "aircraftregistration"),
            measures=("reports",),
        )

    def query_utilization(self):
        result = self.conn_duckdb.execute(
            """SELECT 
                ac.manufacturer,
                d.year AS year,
                ROUND(SUM(f.flight_hours)/COUNT(DISTINCT f.aircraftregistration), 2) AS FH,
                ROUND(SUM(f.takeoffs)/COUNT(DISTINCT f.aircraftregistration), 2) AS TakeOff,
                ROUND(SUM(f.ADOSS)/COUNT(DISTINCT f.aircraftregistration), 2) AS ADOSS,
                ROUND(SUM(f.ADOSU)/COUNT(DISTINCT f.aircraftregistration), 2) AS ADOSU,
                ROUND((SUM(f.ADOSS)+SUM(f.ADOSU))/COUNT(DISTINCT f.aircraftregistration), 2) AS ADOS,
                365 - ROUND((SUM(f.ADOSS)+SUM(f.ADOSU))/COUNT(DISTINCT f.aircraftregistration), 2) AS ADIS,
                ROUND(
                    ROUND(SUM(f.flight_hours)/COUNT(DISTINCT f.aircraftregistration), 2) /
                    ((365 - ROUND((SUM(f.ADOSS)+SUM(f.ADOSU))/COUNT(DISTINCT f.aircraftregistration), 2)) * 24), 2
                ) AS DU,
                ROUND(
                    ROUND(SUM(f.takeoffs)/COUNT(DISTINCT f.aircraftregistration), 2) /
                    (365 - ROUND((SUM(f.ADOSS)+SUM(f.ADOSU))/COUNT(DISTINCT f.aircraftregistration), 2)), 2
                ) AS DC,
                100 * ROUND(SUM(f.delays)/ROUND(SUM(f.takeoffs), 2), 4) AS DYR,
                100 * ROUND(SUM(f.cancellations)/ROUND(SUM(f.takeoffs), 2), 4) AS CNR,
                100 - ROUND(100*(SUM(f.delays)+SUM(f.cancellations))/SUM(f.takeoffs), 2) AS TDR,
                100 * ROUND(SUM(f.delayduration)/SUM(f.delays),2) AS ADD
            FROM DailyAircraftStats f, Aircraft ac, Date d
            WHERE f.aircraftregistration = ac.aircraftregistration AND f.date = d.date
            GROUP BY ac.manufacturer, d.year
            ORDER BY ac.manufacturer, d.year;
            """
        ).fetchall()  # type: ignore
        return result

    def query_reporting(self):
        result = self.conn_duckdb.execute(
            """
            SELECT ac.manufacturer, d.year, 
                100*ROUND(SUM(f.pilotreports+f.maintenancereports)/SUM(f.flighthours), 3) as RRh,
                100*ROUND(SUM(f.pilotreports+f.maintenancereports)/SUM(f.takeoffs), 2) as RRc
            FROM DailyAircraftStats f, Aircrafts ac, Date d
            WHERE f.aircraftregistration = ac.aircraftregistration AND f.date = d.date
            GROUP BY ac.manufacturer, d.year
            ORDER BY ac.manufacturer, d.year;
            """
        ).fetchall()  # type: ignore
        return result

    def query_reporting_per_role(self):
        result = self.conn_duckdb.execute(
            """
            SELECT ac.manufacturer, d.year,
                100*ROUND( SUM(f.pilotreports)/SUM(f.flighthours), 3) as PRRh,
                100*ROUND( SUM(f.pilotreports)/SUM(f.takeoffs), 2) as PRRc,
                100*ROUND( SUM(f.maintenancereports)/SUM(f.flighthours), 3) as MRRh,
                100*ROUND( SUM(f.maintenancereports)/SUM(f.takeoffs), 2) as MRRc
            FROM DailyFlightStats f, Aircrafts ac, Date d
            WHERE f.aircraftregistration = ac.aircraftregistration AND f.date = d.date
            GROUP BY ac.manufacturer, d.year
            ORDER BY ac.manufacturer, d.year;
            """
        ).fetchall()  # type: ignore
        return result

    def close(self):
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()
