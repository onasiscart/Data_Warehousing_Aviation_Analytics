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


# Utility functions for date codes
def build_dateCode(date) -> str:
    """build a dateCode string 'YYYY-MM-DD' from a datetime object"""
    return f"{date.year}-{date.month}-{date.day}"


def build_monthCode(date) -> str:
    """build a monthCode string 'YYYYMM' from a datetime object"""
    return f"{date.year}{str(date.month).zfill(2)}"


def transform(data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Apply all transformations to the dataframes in the input dictionary.

    Args:
        data (dict{str, pd.DataFrame}): Dictionary of dataframes to transform.

    Returns:
        dict{str, pd.DataFrame}: Dictionary of transformed dataframes ready to load.
    """

    # Retrieve dataframes from the input dictionary
    flights_df = data["flights"]
    maint_df = data["maintenance"]
    reports_df = data["reports"]
    lookup_reporters_df = data["lookup_reporters"]
    lookup_aircrafts = data["lookup_aircrafts"]  # CSVSource

    agg_flights = transform_flights(flights_df)

    agg_maint = transform_maint(maint_df)

    transform_reports(reports_df)

    time, daily_flight_stats = merge_flights_maint_log(
        agg_flights, agg_maint, reports_df
    )
    total_maint_reports = create_total_maint_reports(
        agg_flights, reports_df, lookup_reporters_df, time
    )

    airports = lookup_reporters_df[["airport"]].drop_duplicates().reset_index(drop=True)
    airports["airportcode_attr"] = airports["airport"]
    airports.rename(columns={"airport": "airportcode"}, inplace=True)

    return {
        "date": time,
        "aircraft": lookup_aircrafts,
        "airport": airports,
        "daily_aircraft": daily_flight_stats,
        "total_maintenance": total_maint_reports,
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

    # Step 2: example groupby aggregation

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

    # Create additional columns
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
