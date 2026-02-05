import logging
import pandas as pd
from typing import Dict
import numpy as np
from dw import DW
from pygrametl.datasources import PandasSource, CSVSource, TransformingSource, SQLSource


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


def transform_aircrafts(lookup_aircrafts: CSVSource) -> TransformingSource:
    """
    Prec: lookup_aircrafts és un CSVSource amb les columnes brutes del CSV
    Post: retorna un TransformingSource amb noms de columna coherents amb l'esquema del DW
    """

    def transform(row):  # type: ignore
        # Map raw columns to DW schema columns
        row["aircraftregistration"] = row["aircraft_reg_code"]
        row["manufacturer"] = row["aircraft_manufacturer"]
        row["model"] = row["aircraft_model"]
        # ignore serial number to "drop it"
        # Eliminar claus originals
        del row["aircraft_reg_code"]
        del row["manufacturer_serial_number"]
        del row["aircraft_model"]
        del row["aircraft_manufacturer"]

    return TransformingSource(lookup_aircrafts, transform)


def transform_reporter_lookup(lookup_reporters_src: CSVSource) -> PandasSource:
    """
    Prec: lookup_reporters_src és un CSVSource amb almenys la columna 'airport'
    Post: retorna un TransformingSource amb columnes únicament ['airportcode'], sense duplicats
    """
    lookup_df = pd.DataFrame(lookup_reporters_src)  # blocking operation
    lookup_df.rename(columns={"airport": "airportcode"}, inplace=True)
    lookup_df = (
        lookup_df[["reporteurid", "airportcode"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    return PandasSource(lookup_df)


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


def clean_flights(flights_source: SQLSource) -> pd.DataFrame:
    """
    Prec: flights_source is an iterator with raw flight data extracted from the source
    Post: returns dataframe where all business rules are enforced"""
    flights_df = pd.DataFrame(flights_source)
    check_actualarrival_after_departure(flights_df)
    check_no_overlapping_flights(flights_df)
    return flights_df


def clean_reports(reports_it: SQLSource, dw: DW) -> pd.DataFrame:
    """
    Prec: reports_it must contain column 'aircraftregistration'
    Post: returns dataframe where all aircrafts in reports_df exist in aircraft_dim
    """
    LOG_FILE = "invalid_reports.csv"
    reports_df = pd.DataFrame(list(reports_it))
    if reports_df.empty:
        logging.warning("No reports found in source.")
        return reports_df
    # find invalid aircraftregistrations
    valid_idx = []
    invalid_rows = []
    for idx, row in reports_df.iterrows():
        reg = row.get("aircraftregistration")
        # make sure attribute exists
        assert reg is not None
        # look for valid
        if dw.aircraft_dim.lookup({"aircraftregistration": reg}) is not None:
            valid_idx.append(idx)
        else:
            invalid_rows.append(row.to_dict())
    # log invalid rows in CSV
    if invalid_rows:
        invalid_df = pd.DataFrame(invalid_rows)
        invalid_df.to_csv(
            LOG_FILE,
            mode="a",
            index=False,
            header=not pd.io.common.file_exists(LOG_FILE),
        )
        logging.info(
            f"BR-3 fixed: Removed {len(invalid_rows)} invalid reports (logged to {LOG_FILE})"
        )
    else:
        logging.info("BR-3 passed: All reports reference valid aircrafts.")
    return reports_df.loc[valid_idx].reset_index(drop=True)


def valid_dates(
    flights_df: pd.DataFrame, reports_df: pd.DataFrame, maint_it: SQLSource, dw: DW
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Prec: flights_df, reports_df dataframes and maint_it iterator with maintenance data
    Post: modifies in place the dataframes and iterator to only contain rows with valid dates in date_dim
    """
    maint_df = pd.DataFrame(list(maint_it))

    # Step 1: Convert all dates to datetime
    def safe_to_datetime(series) -> pd.Series:
        return pd.to_datetime(series, errors="coerce")  # type: ignore

    flights_df["date"] = safe_to_datetime(
        flights_df["scheduleddeparture"].apply(build_dateCode)  # type: ignore
    )
    maint_df["date"] = safe_to_datetime(
        maint_df["scheduleddeparture"].apply(build_dateCode)  # type: ignore
    )
    reports_df["date"] = safe_to_datetime(reports_df["reportingdate"])
    # Step 2: Get valid years from flights to match baseline queries years
    valid_years = set(flights_df["date"].dt.year.dropna().unique())  # type: ignore
    # Step 3: Filter all dataframes by valid years BEFORE merging
    flights_filtered = flights_df[flights_df["date"].dt.year.isin(valid_years)].copy()  # type: ignore
    maint_filtered = maint_df[maint_df["date"].dt.year.isin(valid_years)].copy()  # type: ignore
    reports_filtered = reports_df[reports_df["date"].dt.year.isin(valid_years)].copy()  # type: ignore
    return flights_filtered, reports_filtered, maint_filtered


def get_date_dim(
    flights_df: pd.DataFrame, reports_df: pd.DataFrame, maint_df: pd.DataFrame
) -> PandasSource:
    """
    Prec: flights_df, reports_df dataframes and maint_df dataframe with maintenance data
    Post: returns date_dim dataframe with unique dates from all three sources
    """
    # Build the time dimension from filtered dates
    all_dates = set(flights_df["date"].dropna())
    all_dates.update(maint_df["date"].dropna())
    all_dates.update(reports_df["date"].dropna())
    time_df = pd.DataFrame(sorted(all_dates), columns=["date"])
    time_df["month"] = time_df["date"].apply(build_monthCode)
    time_df["year"] = time_df["date"].dt.year
    return PandasSource(time_df)


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


def get_facts(
    flights_df: pd.DataFrame,
    reports_df: pd.DataFrame,
    maint_df: pd.DataFrame,
    lookup_reporters_it: CSVSource,
) -> tuple[PandasSource, PandasSource]:
    """
    Prec: flights_df, reports_df dataframes and maint_df dataframe with maintenance data
    Post: returns daily_aircraft_fact dataframe with merged and aggregated data
    """
    # Step 1: Transform and aggregate
    agg_flights = transform_flights(flights_df)
    agg_maint = transform_maint(maint_df)
    transform_reports(reports_df)
    # Step 3: JOINS
    daily_flight_stats = merge_flights_maint_log(agg_flights, agg_maint, reports_df)
    total_maint_reports = create_total_maint_reports(
        agg_flights, reports_df, lookup_reporters_it
    )
    return PandasSource(daily_flight_stats), PandasSource(total_maint_reports)


def merge_flights_maint_log(
    agg_flights_df: pd.DataFrame,
    agg_maint_df: pd.DataFrame,
    reports_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Prec: agg_flights_df, agg_maint_df, reports_df dataframes with aggregated data
    Post: returns daily_flight_stats with all valid (date, aircraft) combinations
    """
    # Step 2: Prepare reports_df
    reports_proj = reports_df.drop(
        columns=[
            c for c in ["reporteurclass", "reporteurid"] if c in reports_df.columns
        ],
        errors="ignore",
    )
    reports_proj = reports_proj.groupby(
        ["date", "aircraftregistration"], as_index=False  # type: ignore
    ).agg({"pilotreports": "sum", "maintenancereports": "sum"})
    # Step 3: MERGE the three DataFrames
    daily_flight_stats = agg_flights_df.merge(
        reports_proj, on=["date", "aircraftregistration"], how="outer"
    )
    daily_flight_stats = daily_flight_stats.merge(
        agg_maint_df, on=["date", "aircraftregistration"], how="outer"
    )
    # Step 4: Impute missing values and ensure types
    numeric_cols = daily_flight_stats.select_dtypes(include="number").columns
    daily_flight_stats[numeric_cols] = daily_flight_stats[numeric_cols].fillna(0)
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
    return daily_flight_stats


def join_airports_to_maint(
    maint_df: pd.DataFrame, lookup_df: pd.DataFrame
) -> pd.DataFrame:
    """
    Prec: maint_df and lookup_df must have same 'reporteurid' column
    Post: returns maintenance dataframe with 'airport' column added using reporteur lookup table
    """
    maint_df["reporteurid"] = pd.to_numeric(maint_df["reporteurid"], errors="coerce")
    lookup_df["reporteurid"] = pd.to_numeric(lookup_df["reporteurid"], errors="coerce")
    return maint_df.merge(
        lookup_df[["reporteurid", "airport"]], on="reporteurid", how="left"
    )


def create_total_maint_reports(
    agg_flights_df: pd.DataFrame,
    maint_df: pd.DataFrame,
    lookup_reporters_it: CSVSource,
) -> pd.DataFrame:
    """
    Prec: agg_flights_df, maint_df, lookup_reporters_df dataframes with cleaned and aggregated data
    Post: returns total_maint_reports dataframe: for each aircraft and airport, number of maintenance reports from MAREP reporters.
    """
    lookup_reporters_df = pd.DataFrame(lookup_reporters_it)  # blocking operation
    # Step 1: Get sum of flight cycles and takeoffs by aircraft
    grouped_flights = agg_flights_df.groupby(
        "aircraftregistration", as_index=False  # type: ignore
    ).agg(takeoffs=("takeoffs", "sum"), flighthours=("flighthours", "sum"))

    # Step 2: Filter only MAREP reporters and dates in "time_df"
    maint_df = maint_df[maint_df["reporteurclass"] == "MAREP"].copy()
    maint_df.drop(columns=["reporteurclass"], inplace=True)

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
