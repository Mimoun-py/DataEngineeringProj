import pandas as pd
import logging

logger = logging.getLogger(__name__)

# Columns removed per project spec 
COLUMNS_TO_DROP = ['VendorID', 'store_and_fwd_flag', 'RatecodeID']


class Processor:
    """
    Transforms validated NYC Yellow Taxi trip records.

    Responsibilities:
      1. Remove columns that are no longer needed downstream.
      2. Derive new analytical columns from the raw fields.

    Assumes the DataFrame has already been through Validator, so no defensive
    null-checks are needed for fields the validator guarantees are clean.
    """

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply all transformations in order.
        Returns a new DataFrame; input is not mutated.
        """
        df = df.copy()

        df = self._drop_columns(df)
        df = self._add_trip_duration(df)
        df = self._add_average_speed(df)
        df = self._add_pickup_date_parts(df)
        df = self._add_revenue_per_mile(df)
        df = self._add_trip_distance_category(df)
        df = self._add_fare_category(df)
        df = self._add_trip_time_of_day(df)

        logger.info(
            f"Processing complete. Output shape: {df.shape[0]:,} rows × {df.shape[1]} columns."
        )
        return df

    # ── transformations ──────────────────────────────────────────────────────────

    def _drop_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        # Drop columns that are no longer needed downstream
        existing = [c for c in COLUMNS_TO_DROP if c in df.columns]
        df = df.drop(columns=existing)
        logger.info(f"Dropped columns: {existing}")
        return df

    def _add_trip_duration(self, df: pd.DataFrame) -> pd.DataFrame:
        # Total seconds → minutes as a float keeps sub-minute precision for speed calcs.
        df['trip_duration_minutes'] = (
            (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime'])
            .dt.total_seconds() / 60
        )
        return df

    def _add_average_speed(self, df: pd.DataFrame) -> pd.DataFrame:
        # Only compute where duration > 0 to avoid division by zero.
        # Rows with duration == 0 were already dropped by the validator, but the
        # guard is kept here so the processor is safe if used standalone.
        valid = df['trip_duration_minutes'] > 0
        df['average_speed_mph'] = pd.NA
        df.loc[valid, 'average_speed_mph'] = (
            df.loc[valid, 'trip_distance'] / (df.loc[valid, 'trip_duration_minutes'] / 60)
        )
        df['average_speed_mph'] = df['average_speed_mph'].astype('Float64')
        return df

    def _add_pickup_date_parts(self, df: pd.DataFrame) -> pd.DataFrame:
        df['pickup_year']  = df['tpep_pickup_datetime'].dt.year
        df['pickup_month'] = df['tpep_pickup_datetime'].dt.month
        return df

    def _add_revenue_per_mile(self, df: pd.DataFrame) -> pd.DataFrame:
        # Only compute where distance > 0; validator already removed zero-distance rows
        # but the guard makes the processor safe if used standalone.
        valid = df['trip_distance'] > 0
        df['revenue_per_mile'] = pd.NA
        df.loc[valid, 'revenue_per_mile'] = (
            df.loc[valid, 'total_amount'] / df.loc[valid, 'trip_distance']
        )
        df['revenue_per_mile'] = df['revenue_per_mile'].astype('Float64')
        return df

    def _add_trip_distance_category(self, df: pd.DataFrame) -> pd.DataFrame:
        # Thresholds from project spec: Short < 2, Medium 2–10, Long > 10
        df['trip_distance_category'] = pd.cut(
            df['trip_distance'],
            bins=[0, 2, 10, float('inf')],
            labels=['Short', 'Medium', 'Long'],
            right=False,       # intervals are [left, right)  →  [0,2), [2,10), [10,∞)
            include_lowest=True
        )
        return df

    def _add_fare_category(self, df: pd.DataFrame) -> pd.DataFrame:
        # Thresholds from project spec: Low < 20, Medium 20–50, High > 50
        df['fare_category'] = pd.cut(
            df['fare_amount'],
            bins=[float('-inf'), 20, 50, float('inf')],
            labels=['Low', 'Medium', 'High'],
            right=False        # intervals are (-∞,20), [20,50), [50,∞)
        )
        return df

    def _add_trip_time_of_day(self, df: pd.DataFrame) -> pd.DataFrame:
        # Standard meteorological time-of-day bands based on pickup hour
        # Night: 00–05, Morning: 06–11, Afternoon: 12–17, Evening: 18–23
        hour = df['tpep_pickup_datetime'].dt.hour
        df['trip_time_of_day'] = pd.cut(
            hour,
            bins=[-1, 5, 11, 17, 23],
            labels=['Night', 'Morning', 'Afternoon', 'Evening'],
            right=True         # intervals are (-1,5], (5,11], (11,17], (17,23]
        )
        return df
