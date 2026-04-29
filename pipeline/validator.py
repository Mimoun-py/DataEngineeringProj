import pandas as pd
import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ValidationReport:
    initial_rows: int = 0
    final_rows: int = 0
    issues: list = field(default_factory=list)

    def record(self, column: str, rule: str, count: int, action: str):
        self.issues.append({"column": column, "rule": rule, "count": count, "action": action})
        logger.warning(f"[{action.upper():6}] {column:<25} | {rule}: {count:,} rows")

    def summary(self) -> str:
        dropped = self.initial_rows - self.final_rows
        pct = 100 * dropped / max(self.initial_rows, 1)
        lines = [
            "=== Validation Summary ===",
            f"Initial rows : {self.initial_rows:,}",
            f"Final rows   : {self.final_rows:,}",
            f"Rows dropped : {dropped:,} ({pct:.2f}%)",
            "--- Issues ---",
        ]
        for issue in self.issues:
            lines.append(
                f"  [{issue['action'].upper():6}] {issue['column']:<25} "
                f"{issue['rule']}: {issue['count']:,}"
            )
        lines.append("==========================")
        return "\n".join(lines)


class Validator:
    """
    Validates NYC Yellow Taxi trip records against the official TLC data dictionary
    and domain rules derived from exploratory analysis.

    Two levels of validation:
      - DROP  : row is fundamentally unusable (corrupt key fields, impossible values)
      - FLAG  : row is suspicious but recoverable; a 'validation_flags' column records why
    """

    # ── domain constants (sourced from official TLC data dictionary) ─────────────

    VALID_VENDOR_IDS      = {1, 2, 6, 7}
    VALID_RATECODE_IDS    = {1, 2, 3, 4, 5, 6, 99}   # 99 = Null/unknown per dict
    VALID_PAYMENT_TYPES   = {0, 1, 2, 3, 4, 5, 6}
    VALID_STORE_FWD_FLAGS = {'Y', 'N'}

    LOCATION_MIN = 1
    LOCATION_MAX = 265          # TLC publishes exactly 263 zones; 265 is the documented max ID

    # Empirical cap: real NYC taxi trips cannot exceed ~200 miles.
    # The raw data contains outliers up to 276,000 miles (clearly sensor errors).
    TRIP_DISTANCE_MAX = 200.0

    # Date window for this file (yellow_tripdata_2025-01).
    # A few late-Dec pickups and early-Feb drop-offs exist legitimately.
    DATE_MIN = pd.Timestamp('2024-12-31')
    DATE_MAX = pd.Timestamp('2025-02-01')

    # Payment types that legitimately produce negative monetary values (refunds / disputes)
    REFUND_PAYMENT_TYPES = {3, 4}   # 3 = No charge, 4 = Dispute

    # ── public interface ─────────────────────────────────────────────────────────

    def validate(self, df: pd.DataFrame) -> tuple:
        """
        Run all validation rules in order.
        Returns (cleaned_df, report).  Input df is never mutated.
        """
        report = ValidationReport(initial_rows=len(df))
        df = df.copy()
        df['validation_flags'] = ''

        df = self._validate_vendor_id(df, report)
        df = self._validate_datetimes(df, report)
        df = self._validate_passenger_count(df, report)
        df = self._validate_trip_distance(df, report)
        df = self._validate_ratecode_id(df, report)
        df = self._validate_store_fwd_flag(df, report)
        df = self._validate_location_ids(df, report)
        df = self._validate_payment_type(df, report)
        df = self._validate_fare_amount(df, report)
        df = self._validate_tip_amount(df, report)
        df = self._validate_tolls_amount(df, report)
        df = self._validate_improvement_surcharge(df, report)
        df = self._validate_refund_monetary_fields(df, report)

        report.final_rows = len(df)
        logger.info(report.summary())
        return df, report

    # ── private helpers ──────────────────────────────────────────────────────────

    def _drop(self, df, mask, column, rule, report):
        count = int(mask.sum())
        if count:
            report.record(column, rule, count, 'drop')
        return df[~mask].reset_index(drop=True)

    def _flag(self, df, mask, column, rule, report):
        count = int(mask.sum())
        if count:
            report.record(column, rule, count, 'flag')
            tag = f"{column}:{rule}"
            df.loc[mask, 'validation_flags'] = df.loc[mask, 'validation_flags'].apply(
                lambda x: tag if x == '' else f"{x},{tag}"
            )
        return df

    # ── validation rules ─────────────────────────────────────────────────────────

    def _validate_vendor_id(self, df, report):
        # Only the four documented providers are valid; anything else is a bad record.
        mask = ~df['VendorID'].isin(self.VALID_VENDOR_IDS)
        return self._drop(df, mask, 'VendorID', 'invalid_value', report)

    def _validate_datetimes(self, df, report):
        # Pickup outside the expected month window
        mask = (df['tpep_pickup_datetime'] < self.DATE_MIN) | \
               (df['tpep_pickup_datetime'] > self.DATE_MAX)
        df = self._drop(df, mask, 'tpep_pickup_datetime', 'out_of_range', report)

        # Dropoff outside the expected month window
        mask = (df['tpep_dropoff_datetime'] < self.DATE_MIN) | \
               (df['tpep_dropoff_datetime'] > self.DATE_MAX)
        df = self._drop(df, mask, 'tpep_dropoff_datetime', 'out_of_range', report)

        # Dropoff at or before pickup makes duration <= 0, which breaks downstream
        # speed/revenue calculations.
        mask = df['tpep_dropoff_datetime'] <= df['tpep_pickup_datetime']
        df = self._drop(df, mask, 'tpep_dropoff_datetime', 'not_after_pickup', report)

        # Trip duration longer than 24 hours is unrealistic for taxi trips; drop.
        mask = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']) > pd.Timedelta(hours=24)
        df = self._drop(df, mask, 'tpep_dropoff_datetime', 'duration_exceeds_24h', report)

        return df

    def _validate_passenger_count(self, df, report):
        # Exploration showed ~540k rows with NaN passenger_count (a separate batch of
        # records with different field population). These cannot be trusted.
        mask_null = df['passenger_count'].isna()
        df = self._drop(df, mask_null, 'passenger_count', 'null', report)

        # Zero-passenger trips exist in the data (vehicle moving without a fare).
        # Keep them but flag and impute with mean so downstream stats remain sensible.
        mask_zero = df['passenger_count'] == 0
        if mask_zero.sum():
            mean_val = round(df.loc[df['passenger_count'] > 0, 'passenger_count'].median())
            df = self._flag(df, mask_zero, 'passenger_count', 'zero_imputed_to_mean', report)
            df.loc[mask_zero, 'passenger_count'] = mean_val

        return df

    def _validate_trip_distance(self, df, report):
        # Distance of 0 or less is physically impossible for a completed trip.
        mask = df['trip_distance'] <= 0
        df = self._drop(df, mask, 'trip_distance', 'non_positive', report)

        # Values above 200 miles are sensor/odometer faults (raw data contains values
        # up to 276,000 miles).
        mask = df['trip_distance'] > self.TRIP_DISTANCE_MAX
        df = self._drop(df, mask, 'trip_distance',
                        f'exceeds_{int(self.TRIP_DISTANCE_MAX)}_miles', report)
        return df

    def _validate_ratecode_id(self, df, report):
        # Per the data dictionary, 99 means Null/unknown — keep but flag.
        mask_99 = df['RatecodeID'] == 99
        df = self._flag(df, mask_99, 'RatecodeID', 'unknown_99', report)

        # NaN also means unknown — flag (same NaN batch as passenger_count).
        mask_null = df['RatecodeID'].isna()
        df = self._flag(df, mask_null, 'RatecodeID', 'null', report)

        # Any non-null value outside the documented set is an encoding error.
        mask_invalid = ~df['RatecodeID'].isna() & ~df['RatecodeID'].isin(self.VALID_RATECODE_IDS)
        df = self._drop(df, mask_invalid, 'RatecodeID', 'invalid_value', report)

        return df

    def _validate_store_fwd_flag(self, df, report):
        # NaN in this column coincides with the same batch as passenger_count NaN — flag.
        mask_null = df['store_and_fwd_flag'].isna()
        df = self._flag(df, mask_null, 'store_and_fwd_flag', 'null', report)

        # Any non-null value other than 'Y'/'N' is malformed.
        mask_invalid = ~df['store_and_fwd_flag'].isna() & \
                       ~df['store_and_fwd_flag'].isin(self.VALID_STORE_FWD_FLAGS)
        df = self._drop(df, mask_invalid, 'store_and_fwd_flag', 'invalid_value', report)

        return df

    def _validate_location_ids(self, df, report):
        # TLC publishes zones 1–265; anything outside that range is unmappable.
        for col in ('PULocationID', 'DOLocationID'):
            mask = (df[col] < self.LOCATION_MIN) | (df[col] > self.LOCATION_MAX)
            df = self._drop(df, mask, col,
                            f'out_of_range_{self.LOCATION_MIN}_{self.LOCATION_MAX}', report)
        return df

    def _validate_payment_type(self, df, report):
        mask = ~df['payment_type'].isin(self.VALID_PAYMENT_TYPES)
        return self._drop(df, mask, 'payment_type', 'invalid_value', report)

    def _validate_fare_amount(self, df, report):
        # Negative fare on a refund/dispute payment type is documented behaviour — flag.
        mask_neg_refund = (df['fare_amount'] < 0) & \
                           df['payment_type'].isin(self.REFUND_PAYMENT_TYPES)
        df = self._flag(df, mask_neg_refund, 'fare_amount', 'negative_refund', report)

        # Negative fare on any other payment type is corrupt data.
        mask_neg_other = (df['fare_amount'] < 0) & \
                         ~df['payment_type'].isin(self.REFUND_PAYMENT_TYPES)
        df = self._drop(df, mask_neg_other, 'fare_amount', 'negative_non_refund', report)

        # Zero fare is only valid for "No charge" (payment_type 3).
        mask_zero = (df['fare_amount'] == 0) & (df['payment_type'] != 3)
        df = self._drop(df, mask_zero, 'fare_amount', 'zero_non_no_charge', report)

        return df

    def _validate_tip_amount(self, df, report):
        # Negative tips cannot exist (field is auto-populated from card processors).
        mask_neg = df['tip_amount'] < 0
        df = self._drop(df, mask_neg, 'tip_amount', 'negative', report)

        # The data dictionary states cash tips (payment_type 2) are NOT recorded in
        # this field, so a non-zero value here for cash is a data inconsistency.
        mask_cash_tip = (df['payment_type'] == 2) & (df['tip_amount'] != 0)
        df = self._flag(df, mask_cash_tip, 'tip_amount', 'nonzero_on_cash_payment', report)

        return df

    def _validate_tolls_amount(self, df, report):
        # Negative tolls on non-refund transactions are suspicious but not impossible
        # (billing corrections). Flag rather than drop.
        mask = (df['tolls_amount'] < 0) & ~df['payment_type'].isin(self.REFUND_PAYMENT_TYPES)
        return self._flag(df, mask, 'tolls_amount', 'negative_non_refund', report)

    def _validate_improvement_surcharge(self, df, report):
        # Observed values: -1.0 (refund), 0.0, 0.3, 1.0 (normal).
        # Anything outside [-1, 1] indicates encoding corruption.
        mask = (df['improvement_surcharge'] < -1.0) | (df['improvement_surcharge'] > 1.0)
        return self._flag(df, mask, 'improvement_surcharge', 'out_of_range', report)

    def _validate_refund_monetary_fields(self, df, report):
        """
        Surcharges and fees should only be negative on refund/dispute transactions.
        Flag the rest rather than drop — they are minor fields and the trip data is
        otherwise valid.
        """
        cols = ['congestion_surcharge', 'Airport_fee', 'cbd_congestion_fee', 'extra', 'mta_tax']
        for col in cols:
            if col not in df.columns:
                continue
            mask = (df[col] < 0) & ~df['payment_type'].isin(self.REFUND_PAYMENT_TYPES)
            df = self._flag(df, mask, col, 'negative_non_refund', report)
        return df
