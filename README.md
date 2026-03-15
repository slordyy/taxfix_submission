# TaxFix Submission

This is a [monorepo](https://en.wikipedia.org/wiki/Monorepo) consisting of three components:

1. datalake
2. data_pipelines
3. reports

## datalake

A folder that mimics a data lake.
Provides [medaillon architecture](https://www.databricks.com/blog/what-is-medallion-architecture) subfolders in which
data assets are stored including the raw cdc events (`./datalake/bronze/users`).

## data_pipelines

A python service that provides data pipelines and orchestrations. This solves part 1 of the exercise. Please refer to
the README.md in `./data_pipelines/README.md` for more information.

## reports

A python service that provides an interactive notebook environment. This solves part 2 of the exercise. Please refer to
the README.md in `./reports/README.md` for more information.

## Running everything locally

You can run everything using [docker compose](https://docs.docker.com/compose/install/):

```bash
docker compose up # or docker-compose up - depends on how it was installed
```

This will start the data_pipelines at: http://localhost:8080, check the login credentials at:
`./data_pipelines/airflow/simple_auth_manager_passwords.json.generated`.

The reports can be opened by following the link that is findable in the logs of the service, e.g.:

```bash
docker logs taxfix_submission-reports-1
```

Then you can open the users_report.py file within marimo.

## Design decisions and trade-offs, assumptions and more

Please refer to the data_pipelines README.md in `./data_pipelines/README.md`

## Using AI assistance

I used https://claude.ai/ when search engines did not resolve wanted results regarding implementation issues. This
included for example Asset logic caveats in Airflow 3.
