from tqdm import tqdm
import logging
import pandas as pd


# Configure logging
logging.basicConfig(
    filename='cleaning.log',           # Log file name
    level=logging.INFO,           # Logging level
    format='%(message)s'  # Log message format
)


def build_dateCode(date) -> str:
    return f"{date.year}-{date.month}-{date.day}"


def build_monthCode(date) -> str:
    return f"{date.year}{str(date.month).zfill(2)}"


# TODO: Implement here all transforming functions

from typing import Dict

def transform(data: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    """
    Apply all transformations to the dataframes in the input dictionary.

    Args:
        data (dict{str, pd.DataFrame}): Dictionary of dataframes to transform.

    Returns:
        dict{str, pd.DataFrame}: Dictionary of transformed dataframes ready to load.
    """

    # Retrieve dataframes from the input dictionary
    flights_df = data['flights']
    maint_df = data['maintenance']
    techlog_df = data['techlog']
    lookup_reporters_df = data['lookup_reporters']
    lookup_airports_df = data['lookup_airports']

    #TIMESTAAMPS

    
    agg_flights = transform_flights(flights_df)

    agg_maint = transform_maint(maint_df)

    transform_techlog(techlog_df)


    total_maint_reports = create_total_maint_reports(maint_df, lookup_reporters_df)



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
        ["date", "aircraftreg"], as_index=False
    ).agg(
        flight_hours=("flight_hours", "sum"),
        takeoffs=("takeoffs", "sum"),
        DY=("DY", "sum"),
        CN=("CN", "sum"),
        sumdelay=("sumdelay", "sum")
    )

    return agg_flights




  
def calculate_flight_attributes(flights_df: pd.DataFrame) -> None:
    """
    Calculate derived attributes for the flights dataframe.
    Modifies flights_df in place.
    """

    # Convert relevant columns to timestamps
    to_timestamps(flights_df, ["actualdeparture", "actualarrival", "scheduleddeparture", "scheduledarrival"])
    
    # Create additional columns
    flights_df["date"] = flights_df["actualdeparture"].apply(build_dateCode)
    flights_df["flight_hours"] = (flights_df["actualarrival"] - flights_df["actualdeparture"]).dt.total_seconds() / 3600
    flights_df["takeoffs"] = (~flights_df["cancelled"]).astype(int)

    # Calculate delay if applicable
    calc_delay(flights_df)  # assigns "DELAY" in place

    # Canceled and delay in binaries
    flights_df.rename(columns={"cancelled": "CN"}, inplace=True)
    flights_df["DY"] = flights_df["DELAY"].where(flights_df["DELAY"] > 0, 0)

    # Keep only relevant columns
    flights_df.drop(columns=["actualdeparture", "actualarrival", "scheduleddeparture", "scheduledarrival"], inplace=True)


def to_timestamps(df: pd.DataFrame, columns: list[str]) -> None:
    """
    Converteix les columnes especificades del DataFrame a timestamps (datetime64[ns]).
    Modifica el DataFrame original per referència.
    """
    for col in columns:
        df[col] = pd.to_datetime(df[col])


def calc_delay(flights_df: pd.DataFrame) -> pd.Series:
    """
    Calculate the delay in minutes if applicable, for the entire DataFrame in a vectorized way.

    Args:
        flights_df (pd.DataFrame)

    Returns:
        pd.Series: Applicable delay in minutes for each row.
    """
     # Compute delay in seconds
    delay_secs = (flights_df["actualarrival"] - flights_df["scheduledarrival"]).dt.total_seconds()
    delay_mins = delay_secs / 60

    # Mask: not cancelled and delay between 15 minutes and 6 hours
    mask = (~flights_df["cancelled"]) & (15 < delay_mins < 60*6)

    # Assign in place
    flights_df["DELAY"] = delay_mins.where(mask, 0.0)



def transform_maint(maint_df: pd.DataFrame) -> pd.DataFrame:
    """ 
    Transform the maintenance dataframe.
    """

    # Step 1: calculate derived attributes
    calculate_maintenance_attributes(maint_df)

    # Step 2: groupby
    agg_maint = maint_df.groupby(
        ["date", "aircraftreg"], as_index=False
    ).agg(
        ADOSS=("TOSS", "sum"),
        ADOSU=("TOSU", "sum")
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
    maint_df.drop(columns = ["aircraftreg", "scheduledarrival", "scheduleddeparture", "programmed"]
)

def calc_maintenance_time(maint_df: pd.DataFrame) -> None:
    """
    Adds columns TOSS and TOSU to the maintenance dataframe based in wether the maintenance is scheduled.
    """
    # Compute the difference in days once
    diff_days = (maint_df["scheduleddeparture"] - maint_df["scheduledarrival"]).dt.total_seconds() / (24*3600)

    # Assign TOSS and TOSU based on 'programmed'
    maint_df["TOSS"] = diff_days.where(maint_df["programmed"], 0.0)
    maint_df["TOSU"] = diff_days.where(~maint_df["programmed"], 0.0)


def transform_techlog(techlog_df: pd.DataFrame) -> None:
    """ 
    Transform the techlog dataframe.
    """
    # Convert relevant columns to timestamps
    to_timestamps(techlog_df, ["executiondate"])
    techlog_df["date"] = techlog_df["date"].apply(build_dateCode)


def create_total_maint_reports(maint_df: pd.DataFrame, lookup_reporters_df: pd.DataFrame) -> pd.DataFrame:
    """
    Create the total_maint_reports dataframe: for each aircraft and airport,
    count the number of maintenance reports from MAREP reporters.

    Args:
        maint_df (pd.DataFrame): Maintenance dataframe.
        lookup_reporters_df (pd.DataFrame): Lookup reporters dataframe.

    Returns:
        pd.DataFrame: Aggregated total maintenance reports dataframe with columns:
                      'aircraftregistration', 'airport', 'count'
    """

    # Filter only MAREP reporters
    maint_df = maint_df[maint_df["reporteurclass"] == "MAREP"].copy()
    maint_df.drop(columns=["reporteurclass"], inplace=True)

    # Count per reporteurID and aircraftregistration
    counts = maint_df.groupby(["reporteurID", "aircraftregistration"], as_index=False).size().reset_index(name="count")

    # Join with lookup to get airport
    counts = counts.merge(
        lookup_reporters_df[["reporteurID", "airport"]],
        on="reporteurID",
        how="left"
    )

    # Aggregate by aircraft and airport (sum counts)
    total_maint_reports = counts.groupby(
        ["aircraftregistration", "airport"], as_index=False
    ).agg(count=("count", "sum"))

    return total_maint_reports

