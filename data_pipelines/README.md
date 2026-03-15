# data-pipeline

A _Python_ service that orchestrates and runs data pipelines using Airflow 3 build with uv.

## Prerequisites

* Python >= 3.12 and < 3.14, you can use for example [uv](https://docs.astral.sh/uv/guides/install-python/) to manage
  that
* [uv](https://docs.astral.sh/uv/getting-started/installation/)

## Installing dependencies

```bash
make install
```

## Tests and checks

To run all tests and checks:

```bash
make check
```

To run all tests (unit and integration):

```bash
make test
```

### Unit-tests

To just run unit-tests:

```bash
make unit-test
```

### Integration-tests

To just run integration-tests:

```bash
make integration-test
```

### Auto-formatting

```bash
make auto-format
```

### Linting

```bash
make lint
```

### Check types

```bash
make type-check
```

## Running locally

To run locally:

```bash
make run
```

This will start airflow on http://localhost:8080. You can find the login credentials in
`./airflow/simple_auth_manager_passwords.json.generated`.

## Issues that occurred

1. Not per se a big issue but all *.jsonl files that contained the cdc events were thought to include the date they were
   captured in their file name. Unfortunately all files had the same filename: `events-20260223_143220.jsonl`

## Design decisions and trade-offs

1. A medaillon architecture was used where the gold layer contains the data that can be used for reports.
    * good, because it adds structure to the data lake and makes it easier to find assets
    * good, because data is being isolated, one could introduce different security measures to the different layers of
      the medaillon architecture
    * good, because duckdb does not support parallel write and read access to files, therefore the isolation of the
      layers can help manage data access
    * bad, because it adds additional complexity, although it is rather small in this case
2. Taskflow API, Assets and AssetAlias were used to design the data-pipelines and their dependencies and triggers
    * good, because it uses Airflow internal abstractions and the lineage can be viewed in the UI
    * good, because AssetAlias can be used to trigger asset events, only when an asset was actually changed
    * good, because DAGs can be triggered only if a certain Asset has changed they depend on
    * good, because Taskflow API is more functional and its functions can be tested
    * bad, because AssetAliases exist only for the sake of wrapping an Asset properly and triggering downstream jobs
3. There are two DAGs responsible for creating the data for the user reports: one runs every minute (ingesting cdc
   events), the other only when the ingested cdc events change (creating a snapshot of the cdc data).
    * good, because we have cdc data on a minute resolution
    * good, because the cdc data is already partitioned by minute, we can use the partitions to determine the logical
      date for the cdc ingestion DAG
    * good, because the DAG responsible for creating the snapshot of the data only runs when there were actual changes,
      instead of running also every minute
    * bad, because it could happen that both DAGs run in parallel at the same time, causing lock errors in duckdb
    * bad, because it adds additional complexity
4. The snapshot of the users table follows a type 2 slow changing dimension approach, adding valid_from, valid_until,
   is_current columns to each row
    * good, because historical changes in the data don't get lost
    * bad, because it adds additional complexity in queries
5. Instead of applying each delta of cdc events, the DAG that creates the snapshot takes all available cdc events and
   replaces previous state completely
    * good, because low complexity in the logic, less python code
    * good, because even in updates the payload contains the complete updated state leading to a relatively simple SQL
      query
    * bad, because might not scale well as data increases to certain volume
6. The primary key of the users table is not applied ("_id" as defined in the events) and no indices are created
    * good, because not that much time was available
    * good, because complexity of existing solution is lower
    * bad, because as data grows, certain queries could perform worse, especially when this dimension table is used in
      star schema
7. No data validations were included
    * good, because it reduces the time necessary for implementation
    * bad, because one schema change can make the whole pipeline fail (especially dangerous considering the source is a
      mongodb, which has flexible schemas)
8. This is not a production grade deployment
    * good, because it reduces the time necessary for implementation
    * bad, because this should only be run locally

## Assumptions

1. There are no data contracts and no sla/slo definitions so no data validations were created for this. An assumption
   was made that the cdc data that is partitioned by minute, can also be ingested by minute. The DAG creating the
   snapshot is taking all available cdc events at the time and applies the snapshot instead of taking a batch of deltas
   or something similar, as there are no slo/sla definitions that can steer the operational behavior.
2. There are two fields that signal deletion, the change_type = "DELETE" in the metadata and the is_deleted boolean flag
   in the payload. An assumption was made that there are no synchronization issues when deleting data in the mongodb (
   tombstones set, but inserts still happening for not sync state).
