import marimo

__generated_with = "0.21.0"
app = marimo.App(width="medium")


@app.cell
def _():
    import duckdb
    import marimo as mo
    import polars as pl

    USERS_TABLE = "users_without_pii"

    def run_query(query: str) -> pl.DataFrame:
        with duckdb.connect("../datalake/gold/users.db") as connection:
            return connection.sql(query).pl()

    return USERS_TABLE, mo, run_query


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## How many distinct users are in your active snapshot (i.e., not deleted)?
    """)
    return


@app.cell
def _(USERS_TABLE, run_query):
    unique_users_query = f"""
    SELECT
        COUNT(DISTINCT _id)
    FROM {USERS_TABLE}
    WHERE
        is_current = true -- deleted rows are is_current = false, see ../data_pipeline/data_pipeline/queries.py@93
    """
    unique_users = run_query(unique_users_query)
    return (unique_users,)


@app.cell
def _(unique_users):
    unique_users  # 18.997
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## What percentage of active users use Gmail (@gmail.com) as their email provider?
    """)
    return


@app.cell
def _(USERS_TABLE, run_query):
    email_distribution_query = f"""
    WITH
        email_distribution AS (
            SELECT
                email_domain,
                COUNT(*) AS number_of_users,
                SUM(COUNT(*)) OVER () AS total_users,
                COUNT(*) / SUM(COUNT(*)) OVER () AS percentage
            FROM
                {USERS_TABLE}
            WHERE
                is_current = true
            GROUP BY
                email_domain
            ORDER BY
                email_domain ASC
        )
    SELECT
        email_domain,
        ROUND(percentage * 100, 2) AS active_users_percentage,
    FROM
        email_distribution
    WHERE
        email_domain = 'gmail.com'
    """
    email_distribution = run_query(email_distribution_query)
    return (email_distribution,)


@app.cell
def _(email_distribution):
    email_distribution  # gmail.com: 30.38
    return


@app.cell
def _(USERS_TABLE, run_query):
    total_email_distribution_query = f"""
    SELECT
        email_domain,
        COUNT(*) AS number_of_users,
        SUM(COUNT(*)) OVER () AS total_users,
        COUNT(*) / SUM(COUNT(*)) OVER () AS percentage
    FROM
        {USERS_TABLE}
    WHERE
        is_current = true
    GROUP BY
        email_domain
    ORDER BY
        email_domain ASC
    """
    total_email_distribution = run_query(total_email_distribution_query)
    return (total_email_distribution,)


@app.cell
def _(total_email_distribution):
    total_email_distribution
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Which are the top 3 countries by number of Gmail users
    """)
    return


@app.cell
def _(USERS_TABLE, run_query):
    total_email_country_distribution_query = f"""
    SELECT
        country,
        COUNT(*) AS number_of_users,
    FROM
        {USERS_TABLE}
    WHERE
        email_domain = 'gmail.com'
        AND is_current = true
    GROUP BY
        country
    ORDER BY
        COUNT(*) DESC
    LIMIT 3
    """
    total_email_country_distribution = run_query(total_email_country_distribution_query)
    return (total_email_country_distribution,)


@app.cell
def _(total_email_country_distribution):
    total_email_country_distribution  # Germany: 1172, Switzerland: 235, Spain: 228
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## How many users changed their email address at least once during the captured period
    """)
    return


@app.cell
def _(USERS_TABLE, run_query):
    changed_email_at_least_once_query = f"""
    WITH
        users_changed_email AS (
            SELECT
                _id
            FROM
                {USERS_TABLE}
            WHERE
                email_domain IS NOT NULL
                AND is_deleted = false
            GROUP BY
                _id
            HAVING COUNT(DISTINCT email_domain) > 1
        )
    SELECT COUNT(*) AS total_number_of_users FROM users_changed_email
    """
    changed_email_at_least_once = run_query(changed_email_at_least_once_query)
    return (changed_email_at_least_once,)


@app.cell
def _(changed_email_at_least_once):
    changed_email_at_least_once  # 78
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## What are the top 5 email domain transitions
    """)
    return


@app.cell
def _(USERS_TABLE, run_query):
    top_5_email_domain_transitions_query = f"""
    WITH
        transitions AS (
            SELECT
                _id,
                email_domain AS from_domain,
                LEAD(email_domain) OVER (PARTITION BY _id ORDER BY surrogate_key) AS to_domain
            FROM
                {USERS_TABLE}
            WHERE
                is_deleted = false
                AND email_domain IS NOT NULL
        )
    SELECT
        from_domain,
        to_domain,
        COUNT(*) AS transition_count
        --*
    FROM
        transitions
    WHERE
        from_domain IS NOT NULL
        AND to_domain IS NOT NULL
        AND from_domain != to_domain
    GROUP BY
        from_domain,
        to_domain
    ORDER BY
        COUNT(*) DESC,
        from_domain ASC,
        to_domain ASC
    LIMIT 5
    """
    top_5_email_domain_transitions = run_query(top_5_email_domain_transitions_query)
    return (top_5_email_domain_transitions,)


@app.cell
def _(top_5_email_domain_transitions):
    top_5_email_domain_transitions  # 1. gmail.com -> icloud.com 4, 2. yahoo.com -> outlook.com 4, 3. example.com -> gmail.com 3, 4. example.com -> outlook.com 3, 5. gmail.com -> example.io 3
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## What is the average time span (in minutes) between the first and last CDC event for users who have more than one event
    """)
    return


@app.cell
def _(USERS_TABLE, run_query):
    average_time_span_between_events_query = f"""
    WITH
        user_event_timestamps AS (
            SELECT
                _id,
                MIN(valid_from) AS first_event_ts,
                MAX(valid_from) AS last_event_ts
            FROM
                {USERS_TABLE}
            GROUP BY
                _id
            HAVING
                COUNT(*) > 1
        )
    SELECT
        ROUND(AVG(date_diff('minute', first_event_ts, last_event_ts)), 2) as average_time_span_in_minutes
    FROM
        user_event_timestamps
    """
    average_time_span_between_events = run_query(average_time_span_between_events_query)
    return (average_time_span_between_events,)


@app.cell
def _(average_time_span_between_events):
    average_time_span_between_events  # 2.86
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
