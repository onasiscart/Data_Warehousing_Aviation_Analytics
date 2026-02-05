import logging
from pathlib import Path
import psycopg2
import pandas as pd
import csv
import warnings
from pygrametl.datasources import CSVSource, SQLSource

# ====================================================================================================================================
# Project paths configuration
PROJECT_ROOT = Path(__file__).parent.parent
CONFIG_DIR = PROJECT_ROOT / "config"
DATA_DIR = PROJECT_ROOT / "data" / "lookups"

# ====================================================================================================================================
# Connect to the PostgreSQL source
path = CONFIG_DIR / "db_conf.txt"
if not path.is_file():
    raise FileNotFoundError(
        f"Database configuration file '{path.absolute()}' not found."
    )
try:
    parameters = {}
    # Read the database configuration from the provided txt file, line by line
    with open(path, "r") as f:
        lines = f.readlines()
        for line in lines:
            parameters[line.split("=", 1)[0]] = line.split("=", 1)[1].strip()
    conn = psycopg2.connect(
        dbname=parameters["dbname"],
        user=parameters["user"],
        password=parameters["password"],
        host=parameters["ip"],
        port=parameters["port"],
    )
except psycopg2.Error as e:
    print(e)
    raise ValueError(f"Unable to connect to the database: {parameters}")
except Exception as e:
    print(e)
    raise ValueError(
        f"Database configuration file '{path.absolute()}' not properly formatted (check file 'config/db_conf.example.txt')."
    )

# Configure logging for information and errors
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
# Filter unwanted warnings out
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")


# ====================================================================================================================================
# extracting functions


def extract_flights() -> SQLSource:
    """
    Prec: connection to DBBDA established in conn
    Post: Extract flight data from AIMS.flights and return it as SQLSource
    """
    try:
        relevant_flight_cols = [
            "aircraftregistration",
            "cancelled",
            "actualdeparture",
            "actualarrival",
            "scheduleddeparture",
            "scheduledarrival",
        ]
        query = f'SELECT {", ".join(relevant_flight_cols)} FROM "AIMS"."flights"'
        return SQLSource(connection=conn, query=query)
    except Exception as e:
        logging.critical(f"Error creating flight data source: {e}")
        raise e


def extract_maint() -> SQLSource:
    """
    Prec: connection to DBBDA established in conn
    Post: Extract maintenance data from "AIMS.maintenance" and return it as SQLSource
    """
    try:
        relevant_maint_cols = [
            "aircraftregistration",
            "scheduledarrival",
            "scheduleddeparture",
            "programmed",
        ]
        query = f'SELECT {", ".join(relevant_maint_cols)} FROM "AIMS"."maintenance"'
        return SQLSource(connection=conn, query=query)
    except Exception as e:
        logging.critical(f"Error creating maintenance data source: {e}")
        raise e


def extract_reports() -> SQLSource:
    """
    Prec: connection to DBBDA established in conn
    Post: Extract report data from "AMOS.postflightreports" and return it as SQLSource
    """
    try:
        relevant_reports_cols = [
            "aircraftregistration",
            "reportingdate",
            "reporteurclass",
            "reporteurID",
        ]
        query = (
            f'SELECT {", ".join(relevant_reports_cols)} FROM "AMOS"."postflightreports"'
        )
        return SQLSource(connection=conn, query=query)
    except Exception as e:
        logging.critical(f"Error creating reports data source: {e}")
        raise e


def extract_reporterslookup() -> CSVSource:
    """
    Prec: maintenance_personnel.csv exists in data/lookups/
    Post: Extract reporter information from CSV file and store it in extracted_data
    """
    path = DATA_DIR / "maintenance_personnel.csv"
    try:
        f = open(path, "r", 16384, encoding="utf-8")  # recomanat per pygrametl
        return CSVSource(f=f, delimiter=",")  # crea la font
    except Exception as e:
        logging.critical(f"[extract_reporterslookup] Error reading {path}: {e}")
        raise e


def extract_aircraftlookup() -> CSVSource:
    """
    Prec: aircraft-manufacturerinfo-lookup.csv exists in data/lookups/
    Post: extracts aircraft manufacturer info from a CSV file as a CSVSource iterable
    """
    path = DATA_DIR / "aircraft-manufacturerinfo-lookup.csv"
    try:
        f = open(path, "r", 16384, encoding="utf-8")  # recomanat per pygrametl
        csv_source = CSVSource(f=f, delimiter=",")  # crea la font
        return csv_source
    except Exception as e:
        logging.critical(f"[extract_aircraftlookup] Error reading {path}: {e}")
        raise e


# ====================================================================================================================================
# Baseline queries


def get_aircrafts_per_manufacturer() -> dict[str, list[str]]:
    """
    Prec: aircraft-manufacturerinfo-lookup.csv exists in data/lookups/
    Post: Returns a dictionary with one entry per manufacturer and a list of aircraft identifiers as values.
    """
    path = DATA_DIR / "aircraft-manufacturerinfo-lookup.csv"
    aircrafts: dict[str, list[str]] = {
        "Airbus": [],
        "Boeing": [],
    }
    with open(path, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            manufacturer = row["aircraft_manufacturer"]
            registration = row["aircraft_reg_code"]
            if manufacturer in aircrafts:
                aircrafts[manufacturer].append(registration)
    return aircrafts


def query_utilization_baseline():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()
    cur.execute(
        f"""
        WITH atomic_data AS (
            SELECT f.aircraftregistration,
                CASE 
                    WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                    WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                    ELSE f.aircraftregistration
                    END AS manufacturer, 
                DATE_PART('year', f.scheduleddeparture)::text AS year,
                CASE WHEN f.cancelled 
                    THEN 0
                    ELSE EXTRACT(EPOCH FROM f.actualarrival-f.actualdeparture) / 3600
                    END AS flightHours,
                CASE WHEN f.cancelled 
                    THEN 0
                    ELSE 1
                    END AS flightCycles,
                CASE WHEN f.cancelled
                    THEN 1
                    ELSE 0
                    END AS cancellations,
                CASE WHEN f.cancelled
                    THEN 0
                    ELSE CASE WHEN EXTRACT(EPOCH FROM f.actualarrival - f.scheduledarrival) / 60 > 15
                        THEN 1
                        ELSE 0
                        END
                    END AS delays,
                CASE WHEN f.cancelled
                    THEN 0
                    ELSE CASE WHEN EXTRACT(EPOCH FROM f.actualarrival - f.scheduledarrival) / 60 > 15
                        THEN EXTRACT(EPOCH FROM f.actualarrival - f.scheduledarrival) / 60
                        ELSE 0
                        END
                    END AS delayedMinutes,
                0 AS scheduledOutOfService,
                0 AS unScheduledOutOfService
            FROM "AIMS".flights f
            UNION ALL
            SELECT m.aircraftregistration,           
                CASE 
                    WHEN m.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                    WHEN m.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                    ELSE m.aircraftregistration
                    END AS manufacturer, 
                DATE_PART('year', m.scheduleddeparture)::text AS year,
                0 AS flightHours,
                0 AS flightCycles,
                0 AS cancellations,
                0 AS delays,
                0 AS delayedMinutes,
                CASE WHEN m.programmed
                    THEN EXTRACT(EPOCH FROM m.scheduledarrival-m.scheduleddeparture)/(24*3600)
                    ELSE 0
                    END AS scheduledOutOfService,
                CASE WHEN m.programmed
                    THEN 0
                    ELSE EXTRACT(EPOCH FROM m.scheduledarrival-m.scheduleddeparture)/(24*3600)
                    END AS unScheduledOutOfService
            FROM "AIMS".maintenance m
            )
        SELECT a.manufacturer, a.year, 
            ROUND(SUM(a.flightHours)/COUNT(DISTINCT a.aircraftregistration), 2) AS FH,
            ROUND(SUM(a.flightCycles)/COUNT(DISTINCT a.aircraftregistration), 2) AS TakeOff,
            ROUND(SUM(a.scheduledOutOfService)/COUNT(DISTINCT a.aircraftregistration), 2) AS ADOSS,
            ROUND(SUM(a.unscheduledOutOfService)/COUNT(DISTINCT a.aircraftregistration), 2) AS ADOSU,
            ROUND((SUM(a.scheduledOutOfService)+SUM(a.unscheduledOutOfService))/COUNT(DISTINCT a.aircraftregistration), 2) AS ADOS,
            365-ROUND((SUM(a.scheduledOutOfService)+SUM(a.unscheduledOutOfService))/COUNT(DISTINCT a.aircraftregistration), 2) AS ADIS, -- This assumes a period of one year (as in the group by)
            ROUND(ROUND(SUM(a.flightHours)/COUNT(DISTINCT a.aircraftregistration), 2)/((365-ROUND((SUM(a.scheduledOutOfService)+SUM(a.unscheduledOutOfService))/COUNT(DISTINCT a.aircraftregistration), 2))*24), 2) AS DU,
            ROUND(ROUND(SUM(a.flightCycles)/COUNT(DISTINCT a.aircraftregistration), 2)/(365-ROUND((SUM(a.scheduledOutOfService)+SUM(a.unscheduledOutOfService))/COUNT(DISTINCT a.aircraftregistration), 2)), 2) AS DC,
            100*ROUND(SUM(delays)/ROUND(SUM(a.flightCycles), 2), 4) AS DYR,
            100*ROUND(SUM(a.cancellations)/ROUND(SUM(a.flightCycles), 2), 4) AS CNR,
            100-ROUND(100*(SUM(delays)+SUM(cancellations))/SUM(a.flightCycles), 2) AS TDR,
            100*ROUND(SUM(delayedMinutes)/SUM(delays),2) AS ADD
        FROM atomic_data a
        GROUP BY a.manufacturer, a.year
        ORDER BY a.manufacturer, a.year;
        """
    )
    result = cur.fetchall()
    cur.close()
    return result


def query_reporting_baseline():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()
    cur.execute(
        f"""
        WITH 
            atomic_data_utilization AS (
                SELECT
                    CASE 
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                        ELSE f.aircraftregistration
                        END AS manufacturer, 
                    DATE_PART('year', f.scheduleddeparture)::text AS year,
                    CAST(SUM(CASE WHEN f.cancelled 
                        THEN 0
                        ELSE EXTRACT(EPOCH FROM f.actualarrival-f.actualdeparture) / 3600
                        END) AS numeric) AS flightHours,
                    CAST(SUM(CASE WHEN f.cancelled 
                        THEN 0
                        ELSE 1
                        END) AS numeric) AS flightCycles
                FROM "AIMS".flights f
                GROUP BY manufacturer, YEAR
                ),
            atomic_data_reporting AS (
                SELECT
                    CASE 
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                        ELSE f.aircraftregistration
                        END AS manufacturer, 
                    DATE_PART('year', f.reportingdate)::text AS year,
                    COUNT(*) AS counter
                FROM "AMOS".postflightreports f
                GROUP BY manufacturer, YEAR
                )
        SELECT f1.manufacturer, f1.year,
            1000*ROUND(f1.counter/f2.flightHours, 3) AS RRh,
            100*ROUND(f1.counter/f2.flightCycles, 2) AS RRc               
        FROM atomic_data_reporting f1
            JOIN atomic_data_utilization f2 ON f2.manufacturer = f1.manufacturer AND f1.year = f2.year
        ORDER BY f1.manufacturer, f1.YEAR;
        """
    )
    result = cur.fetchall()
    cur.close()
    return result


def query_reporting_per_role_baseline():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()
    cur.execute(
        f"""
        WITH 
            atomic_data_utilization AS (
                SELECT
                    CASE 
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                        ELSE f.aircraftregistration
                        END AS manufacturer, 
                    DATE_PART('year', f.scheduleddeparture)::text AS year,
                    CAST(SUM(CASE WHEN f.cancelled 
                        THEN 0
                        ELSE EXTRACT(EPOCH FROM f.actualarrival-f.actualdeparture) / 3600
                        END) AS numeric) AS flightHours,
                    CAST(SUM(CASE WHEN f.cancelled 
                        THEN 0
                        ELSE 1
                        END) AS numeric) AS flightCycles
                FROM "AIMS".flights f
                GROUP BY manufacturer, YEAR
                ),
            atomic_data_reporting AS (
                SELECT
                    CASE 
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                        WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                        ELSE f.aircraftregistration
                        END AS manufacturer, 
                    DATE_PART('year', f.reportingdate)::text AS year,
                    f.reporteurclass AS role,
                    COUNT(*) AS counter
                FROM "AMOS".postflightreports f
                GROUP BY manufacturer, year, role
                )
        SELECT f1.manufacturer, f1.year, f1.role,
            1000*ROUND(f1.counter/f2.flightHours, 3) AS RRh,
            100*ROUND(f1.counter/f2.flightCycles, 2) AS RRc              
        FROM atomic_data_reporting f1
            JOIN atomic_data_utilization f2 ON f2.manufacturer = f1.manufacturer AND f1.year = f2.year
        ORDER BY f1.manufacturer, f1.year, f1.role;
        """
    )
    result = cur.fetchall()
    cur.close()
    return result
