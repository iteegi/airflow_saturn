from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults

from custom.hooks import MovielensHook


class MovielensRatingsSensor(BaseSensorOperator):
    """
    Sensor that waits for the Movielens API to have ratings for a time period.

    :param start_date : Start date to start fetching ratings from (inclusive).
        Expected format is YYYY-MM-DD (equal to Airflow's ds formats).
    :type start_date : str

    :param end_date : End date to fetching ratings up to (exclusive).
        Expected format is YYYY-MM-DD (equal to Airflow's ds formats).
    :type end_date : str
    """

    template_fields = ("_start_date", "_end_date")

    @apply_defaults
    def __init__(self, conn_id,
                 start_date="{{ds}}",
                 end_date="{{next_ds}}",
                 **kwargs):
        super().__init__(**kwargs)
        self._conn_id = conn_id
        self._start_date = start_date
        self._end_date = end_date

    def poke(self, context):
        hook = MovielensHook(self._conn_id)

        try:
            next(
                hook.get_ratings(
                    start_date=self._start_date,
                    end_date=self._end_date,
                    batch_size=1
                )
            )

            self.log.info(
                f"Found ratings for {self._start_date} to "
                f"{self._end_date}, continuing!"
            )
            return True
        except StopIteration:
            self.log.info(
                f"Didn't find any ratings for {self._start_date} "
                f"to {self._end_date}, waiting..."
            )
            return False
        finally:
            hook.close()
