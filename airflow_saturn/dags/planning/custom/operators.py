import json
import os

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from custom.hooks import MovielensHook


class MovielensFetchRatingsOperator(BaseOperator):
    """
    Operator that fetches ratings from the Movielens API.
    Parameters
    ----------
    :param conn_id: ID of the connection to use to connect to the
        Movielens API. Connection is expected to include
        authentication details (login/password) and the
        host that is serving the API.
    :type conn_id : str

    :param output_path : Path to write the fetched ratings to.
    :type output_path : str

    :param start_date : Start date to start fetching ratings from (inclusive).
        Expected format is YYYY-MM-DD (equal to Airflow's ds formats).
    :type start_date : str

    :param end_date : End date to fetching ratings up to (exclusive).
        Expected format is YYYY-MM-DD (equal to Airflow's ds formats).
    :type end_date : str

    :param batch_size : Size of the batches (pages) to fetch from the API.
        Larger values mean less requests, but more data transferred
        per request.
    :type batch_size : int

    """

    template_fields = ("_start_date", "_end_date", "_output_path")

    @apply_defaults
    def __init__(
        self,
        conn_id,
        output_path,
        start_date="{{ds}}",
        end_date="{{next_ds}}",
        batch_size=1000,
        **kwargs,
    ):
        super(MovielensFetchRatingsOperator, self).__init__(**kwargs)

        self._conn_id = conn_id
        self._output_path = output_path
        self._start_date = start_date
        self._end_date = end_date
        self._batch_size = batch_size

    def execute(self, context):
        hook = MovielensHook(self._conn_id)

        try:
            self.log.info(
                f"Fetching ratings for {self._start_date} to {self._end_date}"
            )
            ratings = list(
                hook.get_ratings(
                    start_date=self._start_date,
                    end_date=self._end_date,
                    batch_size=self._batch_size,
                )
            )
            self.log.info(f"Fetched {len(ratings)} ratings")
        finally:
            hook.close()

        self.log.info(f"Writing ratings to {self._output_path}")

        output_dir = os.path.dirname(self._output_path)
        os.makedirs(output_dir, exist_ok=True)

        with open(self._output_path, "w") as file_:
            json.dump(ratings, fp=file_)
