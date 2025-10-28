from dw import DW
import extract
import transform
import load

if __name__ == "__main__":
    # create a data warehouse object
    dw = DW(create=True)
    # ====================================================================================================================================
    # load aircraft dimension
    load.load_aircrafts(
        dw, transform.transform_aircrafts(extract.extract_aircraftlookup())  # type: ignore
    )
    load.load_airports(
        dw, transform.transform_reporter_lookup(extract.extract_reporterslookup())  # type: ignore
    )
    # extract and clean data (qc and BR) needed for date_dim and fact tables
    clean_flights_df = transform.clean_flights(extract.extract_flights())  # type:ignore
    clean_reports_df = transform.clean_reports(
        extract.extract_reports(), dw
    )  # type:ignore
    maint_it = extract.extract_maint()  # type: ignore
    flights_df, reports_df, maint_df = transform.valid_dates(clean_flights_df, clean_reports_df, maint_it, dw)  # type: ignore
    # load date dimension
    load.load_dates(
        dw,
        transform.get_date_dim(flights_df, reports_df, maint_df),  # type: ignore
    )
    # load fact tables
    load.load_facts(dw, transform.get_facts(flights_df, reports_df, maint_df, extract.extract_reporterslookup()))  # type: ignore
    # ====================================================================================================================================
    # done
    dw.close()
