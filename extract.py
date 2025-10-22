import logging
from pathlib import Path
import psycopg2
import pandas as pd

# https://pygrametl.org
from pygrametl.datasources import CSVSource


# ====================================================================================================================================
# Connect to the PostgreSQL source
path = Path("dbconf2.txt")
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
        f"Database configuration file '{path.absolute()}' not properly formatted (check file 'db_conf.example.txt'."
    )

# ====================================================================================================================================


def extract_flights(extracted_data: dict[str, pd.DataFrame | CSVSource]) -> None:
    """
    Extract flight data from the database and store it in the provided dictionary.
    Prec: connection to DBBDA established in conn
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
        extracted_data["flights"] = pd.read_sql(
            f'SELECT {", ".join(relevant_flight_cols)} FROM "AIMS"."flights"', conn
        )
    except Exception as e:
        raise RuntimeError(f"Error reading flight data: {e}") from e


def extract_maint(extracted_data: dict[str, pd.DataFrame | CSVSource]) -> None:
    """""" ""
    try:
        relevant_maint_cols = [
            "aircraftregistration",
            "scheduledarrival",
            "scheduleddeparture",
            "programmed",
        ]
        extracted_data["maintenance"] = pd.read_sql(
            f'SELECT {", ".join(relevant_maint_cols)} FROM "AIMS"."maintenance"', conn
        )
    except Exception as e:
        raise RuntimeError(f"Error reading maintenance data: {e}") from e


def extract_reports(extracted_data: dict[str, pd.DataFrame | CSVSource]) -> None:
    """"""
    try:
        relevant_reports_cols = [
            "aircraftregistration",
            "reportingdate",
            "reporteurclass",
            "reporteurID",
        ]
        extracted_data["reports"] = pd.read_sql(
            f'SELECT {", ".join(relevant_reports_cols)} FROM "AMOS"."postflightreports"',
            conn,
        )
    except Exception as e:
        raise RuntimeError(f"Error reading reports data: {e}") from e


def extract_reporterslookup(
    extracted_data: dict[str, pd.DataFrame | CSVSource],
) -> None:
    """Extract reporter information from a CSV file."""
    path = "maintenance_personnel.csv"
    try:
        extracted_data["lookup_reporters"] = pd.read_csv(path)

    except FileNotFoundError:
        raise FileNotFoundError(f"[extract_reporterslookup] File {path} not found.")
    except Exception as e:
        raise RuntimeError(
            f"[extract_reporterslookup] Error reading {path}: {e}"
        ) from e


def extract_aircraftlookup(extracted_data: dict[str, pd.DataFrame | CSVSource]) -> None:
    """"""
    path = "aircraft-manufacturerinfo-lookup.csv"
    try:
        # Llegir com DataFrame per normalitzar columnes
        df = pd.read_csv(path)

        # Renombrar columnes per consistència amb el DW
        df.rename(
            columns={
                "aircraft_reg_code": "aircraftregistration",
                "aircraft_manufacturer": "manufacturer",
                "aircraft_model": "model",
                "manufacturer_serial_number": "serialnumber",
            },
            inplace=True,
        )

        # Retornar com DataFrame (més fiable que CSVSource)
        extracted_data["lookup_aircrafts"] = df

    except FileNotFoundError:
        raise FileNotFoundError(f"[extract_aircraftlookup] File {path} not found.")
    except Exception as e:
        raise RuntimeError(f"[extract_aircraftlookup] Error reading {path}: {e}") from e


# function to extract all necessary files
def extract() -> dict[str, pd.DataFrame | CSVSource]:
    """
    Prec: connection to DBBDA established
    Returns: dictionary with extracted tables (flights, maintenance, techlog, lookup_reporters) as dataframes
    and an aircraft lookup pygrametl iterable
    """

    extracted_data: dict[str, pd.DataFrame | CSVSource] = {}

    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    # actions that extract data from sources and save them in the extracted_data dictionary
    extract_funcs = [
        extract_flights,
        extract_maint,
        extract_reports,
        extract_reporterslookup,
        extract_aircraftlookup,
    ]

    for func in extract_funcs:
        logging.info(f"Executing {func.__name__}...")
        try:
            func(extracted_data)
            logging.info(f"{func.__name__} completed successfully.")
        except Exception as e:
            logging.critical(f"{func.__name__} failed: {e}")
            # stop pipeline in case of an error
            raise

    logging.info("Extraction completed successfully.")
    return extracted_data


# ====================================================================================================================================
# Baseline queries
def get_aircrafts_per_manufacturer() -> dict[str, list[str]]:
    """
    Returns a dictionary with one entry per manufacturer
    and a list of aircraft identifiers as values.
    """
    path = "aircraft-manufacturerinfo-lookup.csv"
    aircrafts = {
        "Airbus": [],
        "Boeing": [],
    }

    # Obrim el fitxer amb encoding i passem només el fitxer a CSVSource
    with open(path, encoding="utf-8") as f:
        source = CSVSource(f, delimiter=",")

        for row in source:
            manufacturer = row["aircraft_manufacturer"]
            registration = row["aircraft_reg_code"]
            if manufacturer in aircrafts:
                aircrafts[manufacturer].append(registration)
            else:
                # Opcional: si hi ha altres fabricants
                aircrafts[manufacturer] = [registration]

    return dict(aircrafts)


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
            100*ROUND(f1.counter/f2.flightHours, 3) AS RRh,
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
            100*ROUND(f1.counter/f2.flightHours, 3) AS RRh,
            100*ROUND(f1.counter/f2.flightCycles, 2) AS RRc              
        FROM atomic_data_reporting f1
            JOIN atomic_data_utilization f2 ON f2.manufacturer = f1.manufacturer AND f1.year = f2.year
        ORDER BY f1.manufacturer, f1.year, f1.role;
        """
    )
    result = cur.fetchall()
    cur.close()
    return result


def debug_baseline_days():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()
    cur.execute(
        f"""
        WITH all_data AS (
            SELECT aircraftregistration, scheduleddeparture::date as date
            FROM "AIMS".flights
            WHERE aircraftregistration IN ('{"','".join(aircrafts["Airbus"])}')
              AND DATE_PART('year', scheduleddeparture) = 2023
            UNION
            SELECT aircraftregistration, scheduleddeparture::date as date
            FROM "AIMS".maintenance  
            WHERE aircraftregistration IN ('{"','".join(aircrafts["Airbus"])}')
              AND DATE_PART('year', scheduleddeparture) = 2023
        )
        SELECT 
            COUNT(*) as unique_combinations,
            COUNT(DISTINCT aircraftregistration) as num_aircraft,
            COUNT(DISTINCT date) as unique_dates
        FROM all_data;
    """
    )
    return cur.fetchall()


def debug_baseline_exact_calculation():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()
    cur.execute(
        f"""
        SELECT 
            SUM(CASE WHEN cancelled THEN 0 ELSE 1 END)::numeric as total_takeoffs,
            COUNT(DISTINCT aircraftregistration) as num_aircraft,
            SUM(CASE WHEN cancelled THEN 0 ELSE 1 END)::numeric / COUNT(DISTINCT aircraftregistration) as division,
            ROUND(SUM(CASE WHEN cancelled THEN 0 ELSE 1 END)::numeric / COUNT(DISTINCT aircraftregistration), 2) as rounded
        FROM "AIMS".flights
        WHERE aircraftregistration IN ('{"','".join(aircrafts["Airbus"])}')
          AND DATE_PART('year', scheduleddeparture) = 2023;
    """
    )
    return cur.fetchall()


def debug_baseline_atomic():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()
    cur.execute(
        f"""
        WITH atomic_data AS (
            SELECT 
                aircraftregistration,
                CASE 
                    WHEN aircraftregistration IN ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                    WHEN aircraftregistration IN ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                END AS manufacturer, 
                DATE_PART('year', scheduleddeparture)::text AS year,
                CASE WHEN cancelled THEN 0 ELSE 1 END AS flightCycles
            FROM "AIMS".flights
            WHERE DATE_PART('year', scheduleddeparture) = 2023
            UNION ALL
            SELECT 
                aircraftregistration,
                CASE 
                    WHEN aircraftregistration IN ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                    WHEN aircraftregistration IN ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                END AS manufacturer, 
                DATE_PART('year', scheduleddeparture)::text AS year,
                0 AS flightCycles
            FROM "AIMS".maintenance
            WHERE DATE_PART('year', scheduleddeparture) = 2023
        )
        SELECT 
            manufacturer,
            SUM(flightCycles) as total_cycles,
            COUNT(DISTINCT aircraftregistration) as num_aircraft,
            SUM(flightCycles)::numeric / COUNT(DISTINCT aircraftregistration) as raw_avg,
            ROUND(SUM(flightCycles)::numeric / COUNT(DISTINCT aircraftregistration), 2) as rounded_avg
        FROM atomic_data
        WHERE manufacturer = 'Airbus'
        GROUP BY manufacturer;
    """
    )
    return cur.fetchall()


def debug_utilization_baseline():
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
            -- DEBUG: Valors intermedis per TakeOff
            SUM(a.flightCycles) as sum_cycles,
            COUNT(DISTINCT a.aircraftregistration) as count_aircraft,
            SUM(a.flightCycles)::numeric / COUNT(DISTINCT a.aircraftregistration) as raw_division,
            -- Valors originals
            ROUND(SUM(a.flightHours)/COUNT(DISTINCT a.aircraftregistration), 2) AS FH,
            ROUND(SUM(a.flightCycles)/COUNT(DISTINCT a.aircraftregistration), 2) AS TakeOff,
            ROUND(SUM(a.scheduledOutOfService)/COUNT(DISTINCT a.aircraftregistration), 2) AS ADOSS,
            ROUND(SUM(a.unscheduledOutOfService)/COUNT(DISTINCT a.aircraftregistration), 2) AS ADOSU,
            ROUND((SUM(a.scheduledOutOfService)+SUM(a.unscheduledOutOfService))/COUNT(DISTINCT a.aircraftregistration), 2) AS ADOS,
            365-ROUND((SUM(a.scheduledOutOfService)+SUM(a.unscheduledOutOfService))/COUNT(DISTINCT a.aircraftregistration), 2) AS ADIS,
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


def debug_baseline_tdr():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()
    cur.execute(
        f"""
        WITH atomic_data AS (
            SELECT f.aircraftregistration,
                CASE 
                    WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                    WHEN f.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                END AS manufacturer, 
                DATE_PART('year', f.scheduleddeparture)::text AS year,
                CASE WHEN f.cancelled THEN 0 ELSE 1 END AS flightCycles,
                CASE WHEN f.cancelled THEN 1 ELSE 0 END AS cancellations,
                CASE WHEN f.cancelled THEN 0
                    ELSE CASE WHEN EXTRACT(EPOCH FROM f.actualarrival - f.scheduledarrival) / 60 > 15
                        THEN 1 ELSE 0 END
                END AS delays
            FROM "AIMS".flights f
            UNION ALL
            SELECT m.aircraftregistration,
                CASE 
                    WHEN m.aircraftregistration in ('{"','".join(aircrafts.get("Airbus", []))}') THEN 'Airbus'
                    WHEN m.aircraftregistration in ('{"','".join(aircrafts.get("Boeing", []))}') THEN 'Boeing'
                END AS manufacturer, 
                DATE_PART('year', m.scheduleddeparture)::text AS year,
                0 AS flightCycles,
                0 AS cancellations,
                0 AS delays
            FROM "AIMS".maintenance m
        )
        SELECT 
            a.manufacturer, 
            a.year,
            SUM(a.flightCycles) as total_takeoffs,
            SUM(delays) as total_delays,
            SUM(cancellations) as total_cancellations,
            SUM(delays) + SUM(cancellations) as sum_delays_cancel,
            (SUM(delays) + SUM(cancellations))::numeric / SUM(a.flightCycles) as ratio,
            100.0 * (SUM(delays) + SUM(cancellations)) / SUM(a.flightCycles) as percentage_bad,
            ROUND(100*(SUM(delays)+SUM(cancellations))/SUM(a.flightCycles), 2) as rounded_percentage,
            100 - ROUND(100*(SUM(delays)+SUM(cancellations))/SUM(a.flightCycles), 2) AS TDR
        FROM atomic_data a
        GROUP BY a.manufacturer, a.year
        ORDER BY a.manufacturer, a.year;
    """
    )
    result = cur.fetchall()
    cur.close()
    return result


def debug_baseline_aircraft_count():
    aircrafts = get_aircrafts_per_manufacturer()
    cur = conn.cursor()

    # Avions a flights
    cur.execute(
        f"""
        SELECT COUNT(DISTINCT aircraftregistration)
        FROM "AIMS".flights
        WHERE aircraftregistration IN ('{"','".join(aircrafts["Airbus"])}')
          AND DATE_PART('year', scheduleddeparture) = 2023;
    """
    )
    flights_count = cur.fetchone()[0]

    # Avions a maintenance
    cur.execute(
        f"""
        SELECT COUNT(DISTINCT aircraftregistration)
        FROM "AIMS".maintenance
        WHERE aircraftregistration IN ('{"','".join(aircrafts["Airbus"])}')
          AND DATE_PART('year', scheduleddeparture) = 2023;
    """
    )
    maint_count = cur.fetchone()[0]

    # Avions en UNION
    cur.execute(
        f"""
        SELECT COUNT(DISTINCT aircraftregistration)
        FROM (
            SELECT aircraftregistration FROM "AIMS".flights
            WHERE aircraftregistration IN ('{"','".join(aircrafts["Airbus"])}')
              AND DATE_PART('year', scheduleddeparture) = 2023
            UNION
            SELECT aircraftregistration FROM "AIMS".maintenance
            WHERE aircraftregistration IN ('{"','".join(aircrafts["Airbus"])}')
              AND DATE_PART('year', scheduleddeparture) = 2023
        ) t;
    """
    )
    union_count = cur.fetchone()[0]

    cur.close()
    return flights_count, maint_count, union_count


# ====================================================================================================================================

# ====================================================================================================================================ç

if __name__ == "__main__":
    main()
    conn.close()
