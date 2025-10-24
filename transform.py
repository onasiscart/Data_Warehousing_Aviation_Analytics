from duckdb import df
from tqdm import tqdm
import logging
import pandas as pd
from typing import Dict
import numpy as np

# Configure logging
logging.basicConfig(
    filename="cleaning.log",  # Log file name
    level=logging.INFO,  # Logging level
    format="%(message)s",  # Log message format
)


# ====================================================================================================================================
# Utility functions for date codes
def build_dateCode(date: pd.Timestamp) -> str:
    """build a dateCode string 'YYYY-MM-DD' from a datetime object"""
    return f"{date.year}-{date.month}-{date.day}"


def build_monthCode(date: pd.Timestamp) -> str:
    """build a monthCode string 'YYYYMM' from a datetime object"""
    return f"{date.year}{str(date.month).zfill(2)}"


# ====================================================================================================================================
# transformation functions


def transform_aircrafts(lookup_aircrafts: pd.DataFrame) -> None:
    """
    Prec: lookup_aircrafts DataFrame with raw data extracted from CSV
    Post: modifies lookup_aircrafts to have column names and columns consistent with the DW schema
    """
    # Rename columns for consistency with DW
    lookup_aircrafts.rename(
        columns={
            "aircraft_reg_code": "aircraftregistration",
            "aircraft_manufacturer": "manufacturer",
            "aircraft_model": "model",
        },
        inplace=True,
    )
    # Drop serial number
    lookup_aircrafts.drop(columns=["manufacturer_serial_number"], inplace=True)


def transform_reporter_lookup(lookup_reporters_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prec: lookup_reporters_df DataFrame with data (reporteurID, airport) extracted from CSV
    Post: returns Dataframe with column "airportcode"
    """
    airports = lookup_reporters_df[["airport"]].drop_duplicates().reset_index(drop=True)
    airports.rename(columns={"airport": "airportcode"}, inplace=True)
    return airports


def check_rule_1_actualarrival_after_departure(flights_df: pd.DataFrame) -> None:
    """
    BR-1: actualArrival debe ser posterior a actualDeparture
    Fix: Swap their values
    Modifies flights_df in place
    """
    logging.info("Checking Rule 1: actualArrival > actualDeparture...")

    # 1. BUSCAR ERRORS
    # Filtrar vols no cancel·lats amb dates vàlides
    mask = (
        (~flights_df["cancelled"])
        & (flights_df["actualarrival"].notna())
        & (flights_df["actualdeparture"].notna())
    )

    valid_flights = flights_df[mask]

    # Detectar violacions
    violations = valid_flights[
        valid_flights["actualarrival"] <= valid_flights["actualdeparture"]
    ]

    # 2. SOLUCIONAR ERRORS
    if len(violations) > 0:
        logging.warning(
            f"Rule 1 violated: {len(violations)} flights with actualArrival <= actualDeparture"
        )

        # FIX: Swap values IN PLACE
        for idx in violations.index:
            temp = flights_df.at[idx, "actualarrival"]
            flights_df.at[idx, "actualarrival"] = flights_df.at[idx, "actualdeparture"]
            flights_df.at[idx, "actualdeparture"] = temp

        logging.info(
            f"Rule 1 fixed: Swapped {len(violations)} actualArrival/actualDeparture pairs"
        )
    else:
        logging.info("Rule 1 passed: All flights have correct arrival/departure times")


def check_rule_2_no_overlapping_flights(flights_df: pd.DataFrame) -> None:
    """
    BR-2: Two non-cancelled flights of the same aircraft cannot overlap
    Fix: Ignore the first flight, but record the row in a log file
    Modifies flights_df in place by dropping overlapping flights
    """
    logging.info("Checking Rule 2: No overlapping flights for same aircraft...")

    LOG_FILE = "overlapping_flights.csv"

    # 1. BUSCAR ERRORS
    # Filtrar vols no cancel·lats
    non_cancelled = flights_df[~flights_df["cancelled"]].copy()

    # Ordenar per aircraft i departure
    non_cancelled = non_cancelled.sort_values(
        ["aircraftregistration", "actualdeparture"]
    )

    indices_to_remove = []
    overlapping_rows = []

    # Agrupar per aircraft
    for aircraft, group in non_cancelled.groupby("aircraftregistration"):
        group = group.sort_values("actualdeparture").reset_index()

        for i in range(len(group) - 1):
            current_idx = group.at[i, "index"]
            current_arrival = group.at[i, "actualarrival"]
            next_departure = group.at[i + 1, "actualdeparture"]

            # Comprovar si hi ha overlap
            if pd.notna(current_arrival) and pd.notna(next_departure):
                if current_arrival > next_departure:
                    # El vol actual overlaps amb el següent
                    indices_to_remove.append(current_idx)
                    overlapping_rows.append(flights_df.loc[current_idx].to_dict())

    # 2. SOLUCIONAR ERRORS i 3. LOGGING
    if overlapping_rows:
        logging.warning(
            f"Rule 2 violated: {len(overlapping_rows)} overlapping flights detected"
        )

        # 3. LOGGING: Guardar a CSV
        overlapping_df = pd.DataFrame(overlapping_rows)

        # Append o crear nou fitxer
        try:
            overlapping_df.to_csv(
                LOG_FILE,
                mode="a",
                index=False,
                header=not pd.io.common.file_exists(LOG_FILE),
            )
        except:
            overlapping_df.to_csv(LOG_FILE, mode="w", index=False)

        # 2. FIX: Eliminar les files overlapping IN PLACE
        flights_df.drop(indices_to_remove, inplace=True)

        logging.info(
            f"Rule 2 fixed: Removed {len(overlapping_rows)} overlapping flights (logged to {LOG_FILE})"
        )
    else:
        logging.info("Rule 2 passed: No overlapping flights detected")


def check_rule_3_aircraft_exists(
    reports_df: pd.DataFrame, aircrafts_df: pd.DataFrame
) -> None:
    """
    BR-3: The aircraft registration in a post flight report must be an aircraft
    Fix: Ignore the report, but record the row in a log file
    Modifies reports_df in place by dropping invalid reports
    """
    logging.info("Checking Rule 3: All reports reference valid aircraft...")

    LOG_FILE = "invalid_reports.csv"

    # 1. BUSCAR ERRORS
    # Obtenir set d'aircrafts vàlids
    valid_aircrafts = set(aircrafts_df["aircraftregistration"].unique())

    # Detectar reports amb aircraft invàlids
    invalid_mask = ~reports_df["aircraftregistration"].isin(valid_aircrafts)
    invalid_reports = reports_df[invalid_mask]

    # 2. SOLUCIONAR ERRORS i 3. LOGGING
    if len(invalid_reports) > 0:
        logging.warning(
            f"Rule 3 violated: {len(invalid_reports)} reports with invalid aircraft"
        )

        # 3. LOGGING: Guardar a CSV
        try:
            invalid_reports.to_csv(
                LOG_FILE,
                mode="a",
                index=False,
                header=not pd.io.common.file_exists(LOG_FILE),
            )
        except:
            invalid_reports.to_csv(LOG_FILE, mode="w", index=False)

        # 2. FIX: Eliminar les files invàlides IN PLACE
        reports_df.drop(invalid_reports.index, inplace=True)

        logging.info(
            f"Rule 3 fixed: Removed {len(invalid_reports)} invalid reports (logged to {LOG_FILE})"
        )
    else:
        logging.info("Rule 3 passed: All reports reference valid aircraft")


def transform(
    data: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """
    Prec: data (dict{str, pd.DataFrame}): Dictionary of dataframes to transform.
    Post: returns dictionary of transformed data ready to load as dataframes
    """

    # Retrieve dataframes from the input dictionary
    flights_df = data["flights"]
    maint_df = data["maintenance"]
    reports_df = data["reports"]
    lookup_reporters_df = data["lookup_reporters"]
    aircrafts = data["lookup_aircrafts"]

    # rename and project lookups
    transform_aircrafts(aircrafts)
    airports = transform_reporter_lookup(lookup_reporters_df)

    # quality checks
    check_rule_1_actualarrival_after_departure(flights_df)
    check_rule_2_no_overlapping_flights(flights_df)
    check_rule_3_aircraft_exists(reports_df, aircrafts)

    # calculate aggregate data (GROUP BY)
    agg_flights = transform_flights(flights_df)
    agg_maint = transform_maint(maint_df)
    transform_reports(reports_df)

    # JOIN data
    time, daily_flight_stats = merge_flights_maint_log(
        agg_flights, agg_maint, reports_df
    )
    total_maint_reports = create_total_maint_reports(
        agg_flights, reports_df, lookup_reporters_df, time
    )
    # return tables ready for loading
    return {
        "date": time,
        "aircraft": aircrafts,
        "airport": airports,
        "daily_aircraft": daily_flight_stats,
        "total_maintenance": total_maint_reports,  # type: ignore
    }


def to_timestamps(df: pd.DataFrame, columns: list[str]) -> None:
    """
    Converteix les columnes especificades del DataFrame a timestamps (datetime64[ns]).
    Modifica el DataFrame original per referència.
    """
    for col in columns:
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")


def transform_flights(flights_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the flights dataframe: calculate attributes and optionally aggregate.
    Returns a new dataframe aggregated by date or other columns if needed.
    """
    # Step 1: calculate derived attributes
    calculate_flight_attributes(flights_df)
    # Step 2: groupby aggregation
    flights_df["sumdelay"] = flights_df["DELAY"]
    agg_flights = flights_df.groupby(
        ["date", "aircraftregistration"], as_index=False
    ).agg(
        flighthours=("flighthours", "sum"),
        takeoffs=("takeoffs", "sum"),
        delays=("DY", "sum"),
        cancellations=("CN", "sum"),
        delayduration=("sumdelay", "sum"),
    )
    return agg_flights


def calculate_flight_attributes(flights_df: pd.DataFrame) -> None:
    """
    Calculate derived attributes for the flights dataframe.
    Modifies flights_df in place.
    """
    # Convert relevant columns to timestamps
    to_timestamps(
        flights_df,
        ["actualdeparture", "actualarrival", "scheduleddeparture", "scheduledarrival"],
    )
    # Create additional columns: date, flighthours, takeoffs, delay
    flights_df["date"] = flights_df["scheduleddeparture"].apply(build_dateCode)
    flights_df["flighthours"] = np.where(
        ~flights_df["cancelled"],
        (
            (
                flights_df["actualarrival"] - flights_df["actualdeparture"]
            ).dt.total_seconds()
            / 3600
        ).fillna(0),
        0,
    )
    flights_df["takeoffs"] = (~flights_df["cancelled"]).astype(int)
    # Calculate delay if applicable
    calc_delay(flights_df)  # assigns "DELAY" in place
    # Canceled and delay in binaries
    flights_df.rename(columns={"cancelled": "CN"}, inplace=True)
    flights_df["DY"] = (flights_df["DELAY"] > 0).astype(int)
    # Keep only relevant columns
    flights_df.drop(
        columns=[
            "actualdeparture",
            "actualarrival",
            "scheduleddeparture",
            "scheduledarrival",
        ],
        inplace=True,
    )


def calc_delay(flights_df: pd.DataFrame) -> pd.Series:
    """
    Calculate the delay in minutes if applicable, for the entire DataFrame in a vectorized way.

    Args:
        flights_df (pd.DataFrame)

    Returns:
        pd.Series: Applicable delay in minutes for each row.
    """
    # Compute delay in seconds
    delay_secs = (
        flights_df["actualarrival"] - flights_df["scheduledarrival"]
    ).dt.total_seconds()
    delay_mins = delay_secs / 60

    # Mask: not cancelled and delay between 15 minutes and 6 hours
    mask = (~flights_df["cancelled"]) & (delay_mins > 15) & (delay_mins < 60 * 6)

    # Assign in place
    flights_df["DELAY"] = delay_mins.where(mask, 0.0)


def transform_maint(maint_df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform the maintenance dataframe.
    """

    # Step 1: calculate derived attributes
    calculate_maintenance_attributes(maint_df)

    # Step 2: groupby and aggregate
    agg_maint = maint_df.groupby(["date", "aircraftregistration"], as_index=False).agg(
        ADOSS=("TOSS", "sum"), ADOSU=("TOSU", "sum")
    )
    return agg_maint


def calculate_maintenance_attributes(maint_df: pd.DataFrame) -> None:
    """
    Calculate derived attributes for the maintenance dataframe.
    Modifies maint_df in place.
    """

    # Time columns formatting
    to_timestamps(maint_df, ["scheduledarrival", "scheduleddeparture"])
    maint_df["date"] = maint_df["scheduleddeparture"].apply(build_dateCode)

    calc_maintenance_time(maint_df)

    # Project to keep only relevant cols
    maint_df.drop(
        columns=["scheduledarrival", "scheduleddeparture", "programmed"], inplace=True
    )


def calc_maintenance_time(maint_df: pd.DataFrame) -> None:
    """
    Adds columns TOSS and TOSU to the maintenance dataframe based in wether the maintenance is scheduled.
    """
    # Compute the difference in days once
    diff_days = (
        maint_df["scheduledarrival"] - maint_df["scheduleddeparture"]
    ).dt.total_seconds() / (24 * 3600)

    # Assign TOSS and TOSU based on 'programmed'
    maint_df["TOSS"] = diff_days.where(maint_df["programmed"], 0.0)
    maint_df["TOSU"] = diff_days.where(~maint_df["programmed"], 0.0)


def transform_reports(reports_df: pd.DataFrame) -> None:
    """
    Transform the reports dataframe.
    """
    # Convert relevant columns to timestamps
    to_timestamps(reports_df, ["reportingdate"])
    print(reports_df["reportingdate"].sort_values().head())
    reports_df["date"] = reports_df["reportingdate"].apply(build_dateCode)

    reports_df.drop(columns=["reportingdate"], inplace=True)

    # Create pilotreports and maintenancereports flags
    reports_df["pilotreports"] = (reports_df["reporteurclass"] == "PIREP").astype(int)
    reports_df["maintenancereports"] = (reports_df["reporteurclass"] == "MAREP").astype(
        int
    )


def create_total_maint_reports(
    agg_flights_df: pd.DataFrame,
    maint_df: pd.DataFrame,
    lookup_reporters_df: pd.DataFrame,
    time_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Create the total_maint_reports dataframe: for each aircraft and airport,
    count the number of maintenance reports from MAREP reporters.

    The function performs the following steps:
      1. Aggregates flight data by aircraft (summing take-offs and flight hours).
      2. Filters maintenance data to include only MAREP reporters.
      3. Counts maintenance reports per reporter and aircraft.
      4. Joins maintenance records with airport data from the reporter lookup table.
      5. Aggregates the total number of maintenance reports per aircraft and airport.

    Args:
        agg_flights_df (pd.DataFrame): Aggregated flight data per date, aircraft
        maint_df (pd.DataFrame): Maintenance data report-wise
        lookup_reporters_df (pd.DataFrame): Reporter lookup table.

    Returns:
        pd.DataFrame: DataFrame with the total number of maintenance reports,
            take-offs, and flight hours per aircraft and airport. Columns:
                - 'aircraftregistration'
                - 'airport'
                - 'count'
                - 'takeoffs'
                - 'flighthours
    """

    def join_airports_to_maint(
        maint_df: pd.DataFrame, lookup_df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Join airport information to the maintenance dataframe using the reporter lookup table.

        Args:
            maint_df (pd.DataFrame): Maintenance dataframe containing 'reporteurid'.
            lookup_df (pd.DataFrame): Reporter lookup dataframe containing 'reporteurid' and 'airport'.

        Returns:
            pd.DataFrame: Maintenance dataframe with the 'airport' column added.
        """
        return maint_df.merge(
            lookup_df[["reporteurid", "airport"]], on="reporteurid", how="left"
        )

    # Step 1: Aggregate flight data by aircraft
    grouped_flights = agg_flights_df.groupby(
        "aircraftregistration", as_index=False
    ).agg(takeoffs=("takeoffs", "sum"), flighthours=("flighthours", "sum"))

    # Step 2: Filter only MAREP reporters
    maint_df = maint_df[maint_df["reporteurclass"] == "MAREP"].copy()
    maint_df.drop(columns=["reporteurclass"], inplace=True)

    maint_df = maint_df[maint_df["date"].isin(time_df["date"])].copy()

    # Step 3: GROUPBY before reporter, aircraft BEFORE THE JOIN
    counts = maint_df.groupby(
        ["reporteurid", "aircraftregistration"], as_index=False
    ).size()
    counts.rename(columns={"size": "count"}, inplace=True)

    # Step 4: Add airport information
    counts = join_airports_to_maint(counts, lookup_reporters_df)

    # Step 5: Aggregate total counts per aircraft and airport
    total_maint_reports = counts.groupby(
        ["aircraftregistration", "airport"], as_index=False
    ).agg(count=("count", "sum"))

    # Step 6: Merge with aggregated flight data
    total_maint_reports = total_maint_reports.merge(
        grouped_flights, on="aircraftregistration", how="left"
    )

    total_maint_reports.rename(
        columns={"airport": "airportcode", "count": "reports"}, inplace=True
    )

    return total_maint_reports


def merge_flights_maint_log(
    agg_flights_df: pd.DataFrame, agg_maint_df: pd.DataFrame, techlog_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Merge aggregated flights and maintenance data with the techlog dataframe.
    Uses OUTER JOIN to keep all data from all sources, but only for years present in flights.

    Args:
        agg_flights_df (pd.DataFrame): Aggregated flights dataframe.
        agg_maint_df (pd.DataFrame): Aggregated maintenance dataframe.
        techlog_df (pd.DataFrame): Techlog dataframe.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]:
            - time dataframe with unique dates and corresponding month codes.
            - daily_flight_stats dataframe with merged flight and maintenance data.
    """

    # Step 1: Convert all dates to datetime
    agg_flights_df["date"] = pd.to_datetime(agg_flights_df["date"], errors="coerce")
    agg_maint_df["date"] = pd.to_datetime(agg_maint_df["date"], errors="coerce")
    techlog_df["date"] = pd.to_datetime(techlog_df["date"], errors="coerce")

    # Step 2: Get valid years from flights
    valid_years = set(agg_flights_df["date"].dt.year.dropna().unique())
    print(f"Valid years from flights: {sorted(valid_years)}")

    # Step 3: Filter all dataframes by valid years BEFORE merging
    agg_flights_filtered = agg_flights_df[
        agg_flights_df["date"].dt.year.isin(valid_years)
    ].copy()

    agg_maint_filtered = agg_maint_df[
        agg_maint_df["date"].dt.year.isin(valid_years)
    ].copy()

    techlog_filtered = techlog_df[techlog_df["date"].dt.year.isin(valid_years)].copy()

    print(f"Rows after year filtering:")
    print(f"  - Flights: {len(agg_flights_filtered)}")
    print(f"  - Maintenance: {len(agg_maint_filtered)}")
    print(f"  - Reports: {len(techlog_filtered)}")

    # Step 4: Build the time dimension from filtered dates
    all_dates = (
        pd.concat(
            [
                agg_flights_filtered["date"],
                agg_maint_filtered["date"],
                techlog_filtered["date"],
            ]
        )
        .dropna()
        .unique()
    )

    time_df = pd.DataFrame(sorted(all_dates), columns=["date"])
    time_df["month"] = time_df["date"].apply(build_monthCode)
    time_df["year"] = time_df["date"].dt.year

    print(f"Years included in time_df: {sorted(time_df['year'].unique())}")
    print(f"Total dates in time_df: {len(time_df)}")

    # Step 5: Prepare reports dataframe (drop unnecessary columns and aggregate)
    techlog_proj = techlog_filtered.drop(
        columns=[
            col
            for col in ["reporteurclass", "reporteurid"]
            if col in techlog_filtered.columns
        ],
        errors="ignore",
    )

    # Group techlog data by aircraft and date
    techlog_proj = techlog_proj.groupby(
        ["date", "aircraftregistration"], as_index=False
    ).agg({"pilotreports": "sum", "maintenancereports": "sum"})

    print(
        f"Total reports after grouping: PIREP={techlog_proj['pilotreports'].sum():.0f}, MAREP={techlog_proj['maintenancereports'].sum():.0f}"
    )

    # Step 6: OUTER MERGE to keep all combinations of (date, aircraft)
    # First merge: flights + reports (OUTER)
    daily_flight_stats = agg_flights_filtered.merge(
        techlog_proj, on=["date", "aircraftregistration"], how="outer"
    )

    # Second merge: + maintenance (OUTER)
    daily_flight_stats = daily_flight_stats.merge(
        agg_maint_filtered, on=["date", "aircraftregistration"], how="outer"
    )

    print(f"Total rows after merges: {len(daily_flight_stats)}")

    # Step 7: Fill NaN values with 0 for numeric columns
    numeric_cols = daily_flight_stats.select_dtypes(include="number").columns
    daily_flight_stats[numeric_cols] = daily_flight_stats[numeric_cols].fillna(0)

    # Després del fillna, converteix a int les columnes que haurien de ser enteres
    int_cols = [
        "takeoffs",
        "delays",
        "cancellations",
        "pilotreports",
        "maintenancereports",
    ]
    for col in int_cols:
        if col in daily_flight_stats.columns:
            daily_flight_stats[col] = daily_flight_stats[col].astype(int)

    # Step 8: Verify final data
    print(f"\nFinal daily_flight_stats summary:")
    print(f"  - Total rows: {len(daily_flight_stats)}")
    print(f"  - Total flighthours: {daily_flight_stats['flighthours'].sum():.2f}")
    print(f"  - Total takeoffs: {daily_flight_stats['takeoffs'].sum():.0f}")
    print(f"  - Total ADOSS: {daily_flight_stats['ADOSS'].sum():.2f}")
    print(f"  - Total ADOSU: {daily_flight_stats['ADOSU'].sum():.2f}")
    print(f"  - Total pilotreports: {daily_flight_stats['pilotreports'].sum():.0f}")
    print(
        f"  - Total maintenancereports: {daily_flight_stats['maintenancereports'].sum():.0f}"
    )

    print(f"\nSample of daily_flight_stats:")
    print(daily_flight_stats.head(10))

    return time_df, daily_flight_stats
