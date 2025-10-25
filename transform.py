import logging
import pandas as pd
from typing import Dict
import numpy as np


# Configure logging for information and errors
logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")


# ====================================================================================================================================
# Utility functions for date codes
def build_dateCode(date: pd.Timestamp) -> str:
    """
    Pre: datetime object date
    Post: build a dateCode string 'YYYY-MM-DD' from date
    """
    return f"{date.year}-{date.month}-{date.day}"


def build_monthCode(date: pd.Timestamp) -> str:
    """
    Prec: datetime object date
    Post: build a monthCode string 'YYYYMM' from date
    """
    return f"{date.year}{str(date.month).zfill(2)}"


# ====================================================================================================================================
# transformation functions


def to_timestamps(df: pd.DataFrame, columns: list[str]) -> None:
    """
    Prec: dataframe df with temporal columns to convert
    Post: modifies df in place, converting columns to datetime64[ns] format
    """
    for col in columns:
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%d", errors="coerce")


def transform_aircrafts(lookup_aircrafts: pd.DataFrame) -> None:
    """
    Prec: lookup_aircrafts DataFrame with raw data extracted from CSV
    Post: modifies lookup_aircrafts to have column names and columns consistent with the final DW schema
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


def check_actualarrival_after_departure(flights_df: pd.DataFrame) -> None:
    """
    Prec: flights_df must contain columns 'actualArrival' and 'actualDeparture'
    Post: flights_df will have corrected 'actualArrival' and 'actualDeparture' values (swap values if needed)
    Modifies flights_df in place
    """
    # Filter flights that are not cancelled and have actual times
    valid_flights = flights_df[
        (
            (~flights_df["cancelled"])
            & (flights_df["actualarrival"].notna())
            & (flights_df["actualdeparture"].notna())
        )
    ]
    # Detect violations
    violations = valid_flights[
        valid_flights["actualarrival"] <= valid_flights["actualdeparture"]
    ]
    # fix
    if len(violations) > 0:
        # Swap values in place
        for idx in violations.index:
            temp = flights_df.at[idx, "actualarrival"]
            flights_df.at[idx, "actualarrival"] = flights_df.at[idx, "actualdeparture"]
            flights_df.at[idx, "actualdeparture"] = temp
        logging.info(
            f"BR-1 fixed: Swapped {len(violations)} actualArrival/actualDeparture pairs"
        )
    else:
        logging.info("BR-1 passed: All flights have correct arrival/departure times")


def check_no_overlapping_flights(flights_df: pd.DataFrame) -> None:
    """
    Prec: flights_df must contain columns 'actualArrival' and 'actualDeparture'
    Post: No two non-cancelled flights of the same aircraft overlap (discard flights if needed)
    Modifies flights_df in place
    """
    LOG_FILE = "overlapping_flights.csv"
    # filter non-cancelled flights
    non_cancelled = flights_df[~flights_df["cancelled"]].copy()
    # Sort by aircraft and departure
    non_cancelled = non_cancelled.sort_values(
        ["aircraftregistration", "actualdeparture"]
    )
    indices_to_remove = []
    overlapping_rows = []
    # Group by aircraft
    for _, group in non_cancelled.groupby("aircraftregistration"):
        group = group.sort_values("actualdeparture").reset_index()
        for i in range(len(group) - 1):
            current_idx = group.at[i, "index"]
            current_arrival = group.at[i, "actualarrival"]
            next_departure = group.at[i + 1, "actualdeparture"]
            # Check for overlap
            if pd.notna(current_arrival) and pd.notna(next_departure):
                if current_arrival > next_departure:
                    # Current flight overlaps with next
                    indices_to_remove.append(current_idx)
                    overlapping_rows.append(flights_df.loc[current_idx].to_dict())
    # Fix
    if overlapping_rows:
        # logging in a csv file
        overlapping_df = pd.DataFrame(overlapping_rows)
        try:
            overlapping_df.to_csv(
                LOG_FILE,
                mode="a",
                index=False,
                header=not pd.io.common.file_exists(LOG_FILE),
            )
        except:
            overlapping_df.to_csv(LOG_FILE, mode="w", index=False)
        # drop overlapping rows IN PLACE
        flights_df.drop(indices_to_remove, inplace=True)
        logging.info(
            f"BR-2 fixed: Removed {len(overlapping_rows)} overlapping flights (logged to {LOG_FILE})"
        )
    else:
        logging.info("BR-2 passed: No overlapping flights detected")


def check_aircraft_exists(reports_df: pd.DataFrame, aircrafts_df: pd.DataFrame) -> None:
    """
    Prec: reports_df must contain column 'aircraftregistration'
    Post: all aircrafts in reports_df exist in aircrafts_df
    Modifies reports_df in place by dropping invalid reports
    """
    LOG_FILE = "invalid_reports.csv"
    # obtain valid aircrafts
    valid_aircrafts = set(aircrafts_df["aircraftregistration"].unique())
    # find violations
    invalid_mask = ~reports_df["aircraftregistration"].isin(valid_aircrafts)
    invalid_reports = reports_df[invalid_mask]
    # Fix
    if len(invalid_reports) > 0:
        # logging to csv file
        try:
            invalid_reports.to_csv(
                LOG_FILE,
                mode="a",
                index=False,
                header=not pd.io.common.file_exists(LOG_FILE),
            )
        except:
            invalid_reports.to_csv(LOG_FILE, mode="w", index=False)
        # fix: drop invalid reports in place
        reports_df.drop(invalid_reports.index, inplace=True)
        logging.info(
            f"BR-3 fixed: Removed {len(invalid_reports)} invalid reports (logged to {LOG_FILE})"
        )
    else:
        logging.info("BR-3 passed: All reports reference valid aircraft")


def calc_delay(flights_df: pd.DataFrame) -> None:
    """
    Prec: flights_df must contain columns 'actualarrival', 'scheduledarrival', 'cancelled'
    Post: flights_df will contain a new column 'DELAY' with the calculated delay in minutes.
    Returns applicable delay in minutes for each row.
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


def calculate_flight_attributes(flights_df: pd.DataFrame) -> None:
    """
    Prec: flights_df must contain columns 'actualarrival', 'scheduledarrival', 'cancelled'
    Post: flights_df will contain new columns with calculated flight attributes.
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
        ).fillna(  # type:ignore
            0
        ),
        0,
    )
    flights_df["takeoffs"] = (~flights_df["cancelled"]).astype(int)
    # Calculate delay if applicable
    calc_delay(flights_df)  # assigns "DELAY" in place
    # Canceled and delay as binary flags
    flights_df.rename(columns={"cancelled": "CN"}, inplace=True)
    flights_df["DY"] = (flights_df["DELAY"] > 0).astype(int)
    # Drop unneeded columns
    flights_df.drop(
        columns=[
            "actualdeparture",
            "actualarrival",
            "scheduleddeparture",
            "scheduledarrival",
        ],
        inplace=True,
    )


def transform_flights(flights_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prec: flights_df DataFrame with raw flight data extracted from the source
    Post: returns dataframe with derived attributes and aggregated by date and aircraftregistrations
    """
    # Step 1: derive attributes
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


def calculate_maintenance_time(maint_df: pd.DataFrame) -> None:
    """
    Prec: maint_df must contain columns 'scheduledarrival', 'scheduleddeparture', 'programmed'
    Post: adds columns TOSS and TOSU to maint_df based in wether the maintenance is scheduled.
    """
    # Compute difference in days once
    diff_days = (
        maint_df["scheduledarrival"] - maint_df["scheduleddeparture"]
    ).dt.total_seconds() / (24 * 3600)
    # Assign Time on service based on 'programmed'
    maint_df["TOSS"] = diff_days.where(maint_df["programmed"], 0.0)
    maint_df["TOSU"] = diff_days.where(~maint_df["programmed"], 0.0)


def calculate_maintenance_attributes(maint_df: pd.DataFrame) -> None:
    """
    Prec: maint_df must contain columns 'scheduledarrival', 'scheduleddeparture', 'programmed'
    Post: maint_df will contain new columns with calculated maintenance attributes.
    """
    # Impute NaN values with 0
    maint_df.fillna(0, inplace=True)
    # Date conversions
    to_timestamps(maint_df, ["scheduledarrival", "scheduleddeparture"])
    maint_df["date"] = maint_df["scheduleddeparture"].apply(build_dateCode)
    calculate_maintenance_time(maint_df)
    # Projection to drop unneeded columns
    maint_df.drop(
        columns=["scheduledarrival", "scheduleddeparture", "programmed"], inplace=True
    )


def transform_maint(maint_df: pd.DataFrame) -> pd.DataFrame:
    """
    Prec: maint_df must contain all AIMS maintenance extracted data
    Post: maint_df will contain new columns with derived and aggregated data
    """
    # Step 1: calculate derived attributes
    calculate_maintenance_attributes(maint_df)
    # Step 2: groupby and aggregate
    agg_maint = maint_df.groupby(["date", "aircraftregistration"], as_index=False).agg(
        ADOSS=("TOSS", "sum"), ADOSU=("TOSU", "sum")
    )
    return agg_maint


def transform_reports(reports_df: pd.DataFrame) -> None:
    """
    Prec: reports_df must contain all AMOS postflight reports extracted data
    Post: reports_df will contain new columns with derived data
    """
    # Convert relevant columns to timestamps
    to_timestamps(reports_df, ["reportingdate"])
    reports_df["date"] = reports_df["reportingdate"].apply(build_dateCode)
    # Projection to drop unneeded columns
    reports_df.drop(columns=["reportingdate"], inplace=True)
    # Derive pilotreports and maintenancereports flags
    reports_df["pilotreports"] = (reports_df["reporteurclass"] == "PIREP").astype(int)
    reports_df["maintenancereports"] = (reports_df["reporteurclass"] == "MAREP").astype(
        int
    )


def merge_flights_maint_log(
    agg_flights_df: pd.DataFrame, agg_maint_df: pd.DataFrame, reports_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Prec: agg_flights, agg_maint, reports_df dataframes with aggregated and cleaned data
    Post: returns daily_flight_stats dataframe with merged data from the three dataframes
    and time dataframe with unique dates and month codes (for years in agg_flights).
    """
    # Step 1: Convert all dates to datetime
    agg_flights_df["date"] = pd.to_datetime(agg_flights_df["date"], errors="coerce")
    agg_maint_df["date"] = pd.to_datetime(agg_maint_df["date"], errors="coerce")
    reports_df["date"] = pd.to_datetime(reports_df["date"], errors="coerce")

    # Step 2: Get valid years from flights to match baseline queries years
    valid_years = set(agg_flights_df["date"].dt.year.dropna().unique())

    # Step 3: Filter all dataframes by valid years BEFORE merging
    agg_flights_filtered = agg_flights_df[
        agg_flights_df["date"].dt.year.isin(valid_years)
    ].copy()
    agg_maint_filtered = agg_maint_df[
        agg_maint_df["date"].dt.year.isin(valid_years)
    ].copy()
    reports_filtered = reports_df[reports_df["date"].dt.year.isin(valid_years)].copy()

    # Step 4: Build the time dimension from filtered dates
    all_dates = (
        pd.concat(
            [
                agg_flights_filtered["date"],
                agg_maint_filtered["date"],
                reports_filtered["date"],
            ]
        )
        .dropna()
        .unique()  # type: ignore
    )
    time_df = pd.DataFrame(sorted(all_dates), columns=["date"])
    time_df["month"] = time_df["date"].apply(build_monthCode)
    time_df["year"] = time_df["date"].dt.year

    # Step 5: Prepare reports dataframe (drop unnecessary columns and aggregate)
    reports_proj = reports_filtered.drop(
        columns=[
            col
            for col in ["reporteurclass", "reporteurid"]
            if col in reports_filtered.columns
        ],
        errors="ignore",
    )
    reports_proj = reports_proj.groupby(
        ["date", "aircraftregistration"], as_index=False
    ).agg({"pilotreports": "sum", "maintenancereports": "sum"})

    # Step 6: MERGE three dataframes to keep all combinations of (date, aircraft)
    daily_flight_stats = agg_flights_filtered.merge(
        reports_proj, on=["date", "aircraftregistration"], how="outer"
    )
    daily_flight_stats = daily_flight_stats.merge(
        agg_maint_filtered, on=["date", "aircraftregistration"], how="outer"
    )

    # Step 7: Impute missing values and ensure types
    numeric_cols = daily_flight_stats.select_dtypes(include="number").columns
    daily_flight_stats[numeric_cols] = daily_flight_stats[numeric_cols].fillna(0)
    # Ensure type of integer columns
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
    return time_df, daily_flight_stats


def join_airports_to_maint(
    maint_df: pd.DataFrame, lookup_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Prec: maint_df and lookup_df must have same 'reporteurid' column
    Post: returns maintenance dataframe with 'airport' column added using reporteur lookup table
    """
    return maint_df.merge(
        lookup_df[["reporteurid", "airport"]], on="reporteurid", how="left"
    )


def create_total_maint_reports(
    agg_flights_df: pd.DataFrame,
    maint_df: pd.DataFrame,
    lookup_reporters_df: pd.DataFrame,
    time_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Prec: agg_flights_df, maint_df, lookup_reporters_df dataframes with cleaned and aggregated data
    Post: returns total_maint_reports dataframe: for each aircraft and airport, number of maintenance reports from MAREP reporters.
    """

    # Step 1: Get sum of flight cycles and takeoffs by aircraft
    grouped_flights = agg_flights_df.groupby(
        "aircraftregistration", as_index=False  # type: ignore
    ).agg(takeoffs=("takeoffs", "sum"), flighthours=("flighthours", "sum"))

    # Step 2: Filter only MAREP reporters and dates in "time_df"
    maint_df = maint_df[maint_df["reporteurclass"] == "MAREP"].copy()
    maint_df.drop(columns=["reporteurclass"], inplace=True)
    maint_df = maint_df[maint_df["date"].isin(time_df["date"])].copy()

    # Step 3: count reports for eeach reporteur and aircraft
    counts = maint_df.groupby(
        ["reporteurid", "aircraftregistration"], as_index=False
    ).size()
    counts.rename(columns={"size": "count"}, inplace=True)

    # Step 4: Join maintenance records with airport data from the reporter lookup table.
    counts = join_airports_to_maint(counts, lookup_reporters_df)

    # Step 5: Aggregates the total number of maintenance reports per aircraft and airport
    total_maint_reports = counts.groupby(
        ["aircraftregistration", "airport"], as_index=False
    ).agg(  # type: ignore
        count=("count", "sum")
    )

    # Step 6: Merge with grouped_flights to add total takeoffs and flighthours
    total_maint_reports = total_maint_reports.merge(
        grouped_flights, on="aircraftregistration", how="left"
    )
    total_maint_reports.rename(
        columns={"airport": "airportcode", "count": "reports"}, inplace=True
    )
    return total_maint_reports


def transform(
    data: Dict[str, pd.DataFrame],
) -> Dict[str, pd.DataFrame]:
    """
    Prec: data (dict{str, pd.DataFrame}): Dictionary of extracted dataframes to transform.
    Post: returns dictionary of transformed dataframes ready to load into DW schema
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
    check_actualarrival_after_departure(flights_df)
    check_no_overlapping_flights(flights_df)
    check_aircraft_exists(reports_df, aircrafts)

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
    logging.info("Transformation completed successfully.")
    # return tables ready for loading
    return {
        "date": time,
        "aircraft": aircrafts,
        "airport": airports,
        "daily_aircraft": daily_flight_stats,
        "total_maintenance": total_maint_reports,  # type: ignore
    }
