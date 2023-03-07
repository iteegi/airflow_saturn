import datetime as dt

from airflow import DAG
from custom.operators import MovielensFetchRatingsOperator

with DAG(
    dag_id="movie_rating_operator",
    start_date=dt.datetime(2023, 1, 1),
    end_date=dt.datetime(2023, 1, 10),
    schedule_interval="@daily",
) as dag:
    MovielensFetchRatingsOperator(
        task_id="fetch_ratings",
        conn_id="movielens",
        start_date="{{ds}}",
        end_date="{{next_ds}}",
        output_path="/data/custom_operator/{{ds}}.json",
    )
