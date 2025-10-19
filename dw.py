import os
import sys
import duckdb  # https://duckdb.org
import pygrametl  # https://pygrametl.org
from pygrametl.tables import CachedDimension, FactTable

duckdb_filename = "dw.duckdb"


class DW:
    # Data Warehouse class for managing DuckDB connections and operations

    def __init__(self, create=False):
        """Initialize the DW object, creating or connecting to the DuckDB database."""
        if create and os.path.exists(duckdb_filename):
            os.remove(duckdb_filename)
        try:
            self.conn_duckdb = duckdb.connect(duckdb_filename)
            print("Connection to the DW created successfully")
        except duckdb.Error as e:
            print(f"Unable to connect to DuckDB database '{duckdb_filename}':", e)
            sys.exit(1)

        # Create tables in DuckDB if required
        if create:
            try:
                # dimensions tables first
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE Aircrafts (
                        aircraftregistration VARCHAR(6) PRIMARY KEY,
                        model VARCHAR(100) NOT NULL,
                        manufacturer VARCHAR(100) NOT NULL,
                    );
                """
                )
                print("Aircrafts created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE Date(
                        date DATE PRIMARY KEY,
                        month INT NOT NULL, --YYYYMM
                        year INT NOT NULL  --YYYY
                    );
                """
                )
                print("Date created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE Airports(
                        airportcode VARCHAR(3) PRIMARY KEY,
                        airportcode_attr VARCHAR(3),

                    );
                """
                )
                print("Airports created successfully")
                # fact tables next
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
                        PRIMARY KEY (date, aircraftregistration),
                        FOREIGN KEY (aircraftregistration) REFERENCES Aircrafts(aircraftregistration),
                        FOREIGN KEY (date) REFERENCES Date(date)
                    );
                """
                )
                print("DailyAircraftStats created successfully")
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
                print("TotalMaintenanceReports created successfully")
                self.conn_duckdb.commit()

            except duckdb.Error as e:
                print("Error creating the DW tables:", e)
                sys.exit(2)

        # Link DuckDB and pygrametl
        self.conn_pygrametl = pygrametl.ConnectionWrapper(self.conn_duckdb)

        # Create dimension and fact table pygrametl objects
        self.aircraft_dim = CachedDimension(
            name="Aircrafts",
            key="aircraftregistration",
            attributes=["model", "manufacturer"],
            lookupatts=["aircraftregistration"],
        )

        self.date_dim = CachedDimension(
            name="Date",
            key="date",
            attributes=["month", "year"],
            lookupatts=["date"],
        )

        self.airport_dim = CachedDimension(
            name="Airports",
            key="airportcode",
            attributes=[
                "airportcode_attr"
            ],  # we add it as an attribute so pygrametl is happy
            lookupatts=["airportcode"],
        )

        self.daily_aircraft_fact = FactTable(
            name="DailyAircraftStats",
            keyrefs=("date", "aircraftregistration"),  # foreign key to dimensions
            measures=(
                "flighthours",
                "takeoffs",
                "ADOSS",
                "ADOSU",
                "delays",
                "cancellations",
                "delayduration",
                "pilotreports",
                "maintenancereports",
            ),
        )

        self.total_maintenance_fact = FactTable(
            name="TotalMaintenanceReports",
            keyrefs=("airportcode", "aircraftregistration"),
            measures=("reports",),
        )

    # Test queries for analysis
    def query_utilization(self):
        """Query aircraft utilization statistics for each manufacturer and year."""
        result = self.conn_duckdb.execute(
            """SELECT 
                ac.manufacturer,
                d.year AS year,
                ROUND(SUM(f.flighthours)/COUNT(DISTINCT f.aircraftregistration), 2) AS FH,
                ROUND(SUM(f.takeoffs)/COUNT(DISTINCT f.aircraftregistration), 2) AS TakeOff,
                ROUND(SUM(f.ADOSS)/COUNT(DISTINCT f.aircraftregistration), 2) AS ADOSS,
                ROUND(SUM(f.ADOSU)/COUNT(DISTINCT f.aircraftregistration), 2) AS ADOSU,
                ROUND((SUM(f.ADOSS)+SUM(f.ADOSU))/COUNT(DISTINCT f.aircraftregistration), 2) AS ADOS,
                365 - ROUND((SUM(f.ADOSS)+SUM(f.ADOSU))/COUNT(DISTINCT f.aircraftregistration), 2) AS ADIS,
                ROUND(
                    ROUND(SUM(f.flighthours)/COUNT(DISTINCT f.aircraftregistration), 2) /
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
            FROM DailyAircraftStats f, Aircrafts ac, Date d
            WHERE f.aircraftregistration = ac.aircraftregistration AND f.date = d.date
            GROUP BY ac.manufacturer, d.year
            ORDER BY ac.manufacturer, d.year;
            """
        ).fetchall()  # type: ignore
        return result

    def query_reporting(self):
        """Query reporting rates for each manufacturer and year."""
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
        """Query reporting rates per role for each manufacturer and year."""
        result = self.conn_duckdb.execute(
            """
            SELECT ac.manufacturer, d.year,
            100*ROUND( SUM(f.pilotreports)/SUM(f.flighthours), 3) as PRRh,
            100*ROUND( SUM(f.pilotreports)/SUM(f.takeoffs), 2) as PRRc,
            100*ROUND( SUM(f.maintenancereports)/SUM(f.flighthours), 3) as MRRh,
            100*ROUND( SUM(f.maintenancereports)/SUM(f.takeoffs), 2) as MRRc
            FROM DailyAircraftStats f, Aircrafts ac, Date d
            WHERE f.aircraftregistration = ac.aircraftregistration AND f.date = d.date
            GROUP BY ac.manufacturer, d.year
            ORDER BY ac.manufacturer, d.year;
            """
        ).fetchall()  # type: ignore
        return result

    def close(self):
        """Close the DW connections."""
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()
