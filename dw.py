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
        # connection
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
                        aircraftid INT PRIMARY KEY,
                        aircraftregistration VARCHAR(6) UNIQUE NOT NULL,
                        model VARCHAR(100) NOT NULL,
                        manufacturer VARCHAR(100) NOT NULL
                    );
                """
                )
                print("Aircrafts created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE Date(
                        dateid INT PRIMARY KEY,
                        date DATE UNIQUE NOT NULL,
                        month INT NOT NULL, --YYYYMM
                        year INT NOT NULL  --YYYY
                    );
                """
                )
                print("Date created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE Airports(
                        airportid INT PRIMARY KEY,
                        airportcode VARCHAR(3) UNIQUE NOT NULL
                    );
                """
                )
                print("Airports created successfully")
                # fact tables next
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE DailyAircraftStats (
                        dateid INT,
                        aircraftid INT,
                        takeoffs INT NOT NULL,
                        flighthours REAL NOT NULL,
                        ADOSS REAL NOT NULL DEFAULT 0,
                        ADOSU REAL NOT NULL DEFAULT 0,
                        delays INT NOT NULL DEFAULT 0,
                        cancellations INT NOT NULL DEFAULT 0,
                        delayduration REAL NOT NULL DEFAULT 0,
                        pilotreports INT NOT NULL DEFAULT 0,
                        maintenancereports INT NOT NULL DEFAULT 0,
                        PRIMARY KEY (dateid, aircraftid),
                        FOREIGN KEY (aircraftid) REFERENCES Aircrafts(aircraftid),
                        FOREIGN KEY (dateid) REFERENCES Date(dateid)
                    );
                """
                )
                print("DailyAircraftStats created successfully")
                self.conn_duckdb.execute(
                    """
                    CREATE TABLE TotalMaintenanceReports(
                        airportid INT,
                        aircraftid INT,
                        takeoffs INT NOT NULL,
                        flighthours REAL NOT NULL,
                        reports INT NOT NULL,
                        PRIMARY KEY (airportid, aircraftid),
                        FOREIGN KEY (airportid) REFERENCES Airports(airportid),
                        FOREIGN KEY (aircraftid) REFERENCES Aircrafts(aircraftid)
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
            key="aircraftid",
            attributes=["aircraftregistration", "model", "manufacturer"],
            lookupatts=["aircraftregistration"],
        )

        self.date_dim = CachedDimension(
            name="Date",
            key="dateid",
            attributes=["date", "month", "year"],
            lookupatts=["date"],
        )

        self.airport_dim = CachedDimension(
            name="Airports",
            key="airportid",
            attributes=["airportcode"],
            lookupatts=["airportcode"],
        )

        self.daily_aircraft_fact = FactTable(
            name="DailyAircraftStats",
            keyrefs=("dateid", "aircraftid"),  # foreign key to dimensions
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
            keyrefs=("airportid", "aircraftid"),
            measures=("reports", "takeoffs", "flighthours"),
        )

    def query_utilization(self):
        """Query aircraft utilization statistics for each manufacturer and year."""
        result = self.conn_duckdb.execute(
            """SELECT 
                ac.manufacturer,
                d.year AS year,
                CAST(ROUND(SUM(f.flighthours)/COUNT(DISTINCT ac.aircraftregistration), 2) AS DECIMAL(10,2)) AS FH,
                CAST(ROUND((SUM(f.takeoffs) // COUNT(DISTINCT ac.aircraftregistration))::DOUBLE, 2) AS DECIMAL(10,2)) AS TakeOff,
                CAST(ROUND(SUM(f.ADOSS)/COUNT(DISTINCT ac.aircraftregistration), 2) AS DECIMAL(10,2)) AS ADOSS,
                CAST(ROUND(SUM(f.ADOSU)/COUNT(DISTINCT ac.aircraftregistration), 2) AS DECIMAL(10,2)) AS ADOSU,
                CAST(ROUND((SUM(f.ADOSS)+SUM(f.ADOSU))/COUNT(DISTINCT ac.aircraftregistration), 2) AS DECIMAL(10,2)) AS ADOS,
                CAST(365 - ROUND((SUM(f.ADOSS)+SUM(f.ADOSU))/COUNT(DISTINCT ac.aircraftregistration), 2) AS DECIMAL(10,2)) AS ADIS,
                CAST(ROUND(
                    ROUND(SUM(f.flighthours)/COUNT(DISTINCT ac.aircraftregistration), 2) /
                    ((365 - ROUND((SUM(f.ADOSS)+SUM(f.ADOSU))/COUNT(DISTINCT ac.aircraftregistration), 2)) * 24), 2
                ) AS DECIMAL(10,2)) AS DU,
                CAST(ROUND(
                    ROUND((SUM(f.takeoffs) // COUNT(DISTINCT ac.aircraftregistration))::DOUBLE, 2) /
                    (365 - ROUND((SUM(f.ADOSS)+SUM(f.ADOSU))/COUNT(DISTINCT ac.aircraftregistration), 2)), 2
                ) AS DECIMAL(10,2)) AS DC,
                CAST(100 * ROUND(SUM(f.delays)/ROUND(SUM(f.takeoffs), 2), 4) AS DECIMAL(10,2)) AS DYR,
                CAST(100 * ROUND(SUM(f.cancellations)/ROUND(SUM(f.takeoffs), 2), 4) AS DECIMAL(10,2)) AS CNR,
                CAST(100 - ROUND((100*(SUM(f.delays)+SUM(f.cancellations)) // SUM(f.takeoffs))::DOUBLE, 2) AS DECIMAL(10,2)) AS TDR,
                CAST(100 * ROUND(SUM(f.delayduration)/SUM(f.delays), 2) AS DECIMAL(10,2)) AS ADD
            FROM DailyAircraftStats f, Aircrafts ac, Date d
            WHERE f.aircraftid = ac.aircraftid AND f.dateid = d.dateid
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
                CAST(1000*ROUND(SUM(f.pilotreports+f.maintenancereports)/SUM(f.flighthours), 3) AS DECIMAL(10,3)) as RRh,
                CAST(100*ROUND(SUM(f.pilotreports+f.maintenancereports)/SUM(f.takeoffs), 2) AS DECIMAL(10,2)) as RRc
            FROM DailyAircraftStats f, Aircrafts ac, Date d
            WHERE f.aircraftid = ac.aircraftid AND f.dateid = d.dateid
            GROUP BY ac.manufacturer, d.year
            ORDER BY ac.manufacturer, d.year;
            """
        ).fetchall()
        return result

    def query_reporting_per_role(self):
        """Query reporting rates per role for each manufacturer and year."""
        result = self.conn_duckdb.execute(
            """
            SELECT ac.manufacturer, d.year, 'PIREP' as role,
                CAST(100*ROUND(SUM(f.pilotreports)/SUM(f.flighthours), 3) AS DECIMAL(10,3)) as RRh,
                CAST(100*ROUND(SUM(f.pilotreports)/SUM(f.takeoffs), 2) AS DECIMAL(10,2)) as RRc
            FROM DailyAircraftStats f, Aircrafts ac, Date d
            WHERE f.aircraftid = ac.aircraftid AND f.dateid = d.dateid
            GROUP BY ac.manufacturer, d.year
            
            UNION ALL
            
            SELECT ac.manufacturer, d.year, 'MAREP' as role,
                CAST(1000*ROUND(SUM(f.maintenancereports)/SUM(f.flighthours), 3) AS DECIMAL(10,3)) as RRh,
                CAST(100*ROUND(SUM(f.maintenancereports)/SUM(f.takeoffs), 2) AS DECIMAL(10,2)) as RRc
            FROM DailyAircraftStats f, Aircrafts ac, Date d
            WHERE f.aircraftid = ac.aircraftid AND f.dateid = d.dateid
            GROUP BY ac.manufacturer, d.year
            
            ORDER BY manufacturer, year, role;
            """
        ).fetchall()  # type: ignore
        return result

    def query_reporting_per_role_2(self):
        """Query reporting rates per role for each manufacturer and year."""
        result = self.conn_duckdb.execute(
            """
            SELECT ac.manufacturer, d.year,
            100*ROUND( SUM(f.pilotreports)/SUM(f.flighthours), 3) as PRRh,
            100*ROUND( SUM(f.pilotreports)/SUM(f.takeoffs), 2) as PRRc,
            100*ROUND( SUM(f.maintenancereports)/SUM(f.flighthours), 3) as MRRh,
            100*ROUND( SUM(f.maintenancereports)/SUM(f.takeoffs), 2) as MRRc
            FROM DailyAircraftStats f, Aircrafts ac, Date d
            WHERE f.aircraftid = ac.aircraftid AND f.dateid = d.dateid
            GROUP BY ac.manufacturer, d.year
            ORDER BY ac.manufacturer, d.year;
            """
        ).fetchall()  # type: ignore
        return result

    def debug_days_per_aircraft(self):
        """Compta quants dies diferents té cada avió al DW"""
        result = self.conn_duckdb.execute(
            """
            SELECT 
                ac.manufacturer,
                d.year,
                COUNT(*) as total_rows,
                COUNT(DISTINCT f.aircraftregistration) as num_aircraft,
                COUNT(DISTINCT f.date) as unique_dates,
                COUNT(*) / COUNT(DISTINCT f.aircraftregistration) as avg_rows_per_aircraft
            FROM DailyAircraftStats f, Aircrafts ac, Date d
            JOIN Aircrafts ac ON f.aircraftid = ac.aircraftid
            JOIN Date d ON f.dateid = d.dateid
            WHERE d.year = 2023 AND ac.manufacturer = 'Airbus'
            GROUP BY ac.manufacturer, d.year;
        """
        ).fetchall()
        return result

    def debug_tdr(self):
        result = self.conn_duckdb.execute(
            """
            SELECT 
                ac.manufacturer,
                d.year,
                SUM(f.takeoffs) as total_takeoffs,
                SUM(f.delays) as total_delays,
                SUM(f.cancellations) as total_cancellations,
                SUM(f.delays) + SUM(f.cancellations) as sum_delays_cancel,
                (SUM(f.delays) + SUM(f.cancellations))::DOUBLE / SUM(f.takeoffs) as ratio,
                100.0 * (SUM(f.delays) + SUM(f.cancellations)) / SUM(f.takeoffs) as percentage_bad,
                ROUND(100.0*(SUM(f.delays)+SUM(f.cancellations))/SUM(f.takeoffs), 2) as rounded_percentage,
                100 - ROUND(100.0*(SUM(f.delays)+SUM(f.cancellations))/SUM(f.takeoffs), 2) AS TDR
            FROM DailyAircraftStats f, Aircrafts ac, Date d
            WHERE f.aircraftregistration = ac.aircraftregistration AND f.date = d.date
            GROUP BY ac.manufacturer, d.year
            ORDER BY ac.manufacturer, d.year;
        """
        ).fetchall()
        return result

    def close(self):
        """Close the DW connections."""
        self.conn_pygrametl.commit()
        self.conn_pygrametl.close()
