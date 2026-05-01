import pandas as pd
import logging
from dataclasses import dataclass, field


VALID_STATUSES = {'Delivered', 'Shipped', 'Processing', 'Cancelled'}
VALID_PAYMENTS = {'Credit Card', 'PayPal', 'Debit Card', 'Bank Transfer'}


@dataclass
class ValidationReport:
    initial_rows: int = 0
    final_rows:   int = 0
    issues: list = field(default_factory=list)

    def record(self, column: str, rule: str, count: int, action: str):
        if count > 0:
            self.issues.append({
                'column': column,
                'rule':   rule,
                'count':  count,
                'action': action.upper()
            })
            logging.warning(f"[{action.upper():<5}] {column:<20} | {rule}: {count:,} rows")

    def summary(self) -> str:
        dropped = self.initial_rows - self.final_rows
        pct     = (dropped / self.initial_rows * 100) if self.initial_rows else 0
        lines   = [
            '=== Validation Summary ===',
            f'Initial rows : {self.initial_rows:,}',
            f'Final rows   : {self.final_rows:,}',
            f'Rows dropped : {dropped:,} ({pct:.2f}%)',
            '--- Issues ---',
        ]
        for issue in self.issues:
            lines.append(f"  [{issue['action']:<5}] {issue['column']:<20} {issue['rule']}: {issue['count']:,}")
        lines.append('==========================')
        return '\n'.join(lines)


class Validator:

    # ── domain constants ──────────────────────────────────────────────────────
    ORDER_ID_LEN     = 8                          # e.g. ORD00073
    CUSTOMER_ID_LEN  = 8                          # e.g. CUST1119
    MIN_ORDER_DATE   = pd.Timestamp('2020-01-01')
    MAX_ORDER_DATE   = pd.Timestamp('2027-12-31') # notebook rule: can't be past 2027
    VALID_STATUSES   = VALID_STATUSES
    VALID_PAYMENTS   = VALID_PAYMENTS

    # ── public entry point ────────────────────────────────────────────────────

    def validate(self, df: pd.DataFrame) -> tuple[pd.DataFrame, ValidationReport]:
        report = ValidationReport(initial_rows=len(df))
        df = df.copy()
        df['validation_flags'] = ''

        df = self._drop_duplicates(df, report)
        df = self._validate_order_id(df, report)
        df = self._validate_customer_id(df, report)
        df = self._validate_order_date(df, report)
        df = self._validate_product_name(df, report)
        df = self._validate_quantity(df, report)
        df = self._validate_unit_price(df, report)
        df = self._validate_discount(df, report)
        df = self._validate_total_amount(df, report)
        df = self._validate_status(df, report)
        df = self._validate_payment_method(df, report)
        df = self._validate_country(df, report)
        df = self._validate_customer_rating(df, report)

        report.final_rows = len(df)
        logging.info('\n' + report.summary())
        return df, report

    # ── validation methods ────────────────────────────────────────────────────

    def _validate_order_id(self, df, report):
        df = self._drop(df, df['order_id'].isnull(), 'order_id', 'null', report)
        df = self._drop(df, df['order_id'].str.len() != self.ORDER_ID_LEN,
                        'order_id', f'not_{self.ORDER_ID_LEN}_chars', report)
        return df

    def _validate_customer_id(self, df, report):
        df = self._drop(df, df['customer_id'].isnull(), 'customer_id', 'null', report)
        df = self._drop(df, df['customer_id'].str.len() != self.CUSTOMER_ID_LEN,
                        'customer_id', f'not_{self.CUSTOMER_ID_LEN}_chars', report)
        return df

    def _validate_order_date(self, df, report):
        df = self._drop(df, df['order_date'].isnull(), 'order_date', 'null', report)
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
        df = self._drop(df, df['order_date'].isnull(), 'order_date', 'unparseable', report)
        df = self._drop(df, df['order_date'] < self.MIN_ORDER_DATE, 'order_date', 'too_old', report)
        df = self._drop(df, df['order_date'] > self.MAX_ORDER_DATE, 'order_date', 'future_date', report)
        return df

    def _validate_product_name(self, df, report):
        # mandatory — null product names cannot be processed downstream
        df = self._drop(df, df['product_name'].isnull(), 'product_name', 'null', report)
        return df

    def _validate_quantity(self, df, report):
        df = self._drop(df, df['quantity'].isnull(), 'quantity', 'null', report)
        df = self._drop(df, df['quantity'] <= 0, 'quantity', 'zero_or_negative', report)
        return df

    def _validate_unit_price(self, df, report):
        df = self._drop(df, df['unit_price'].isnull(), 'unit_price', 'null', report)
        df = self._drop(df, df['unit_price'] <= 0, 'unit_price', 'zero_or_negative', report)
        return df

    def _validate_discount(self, df, report):
        # discount is a ratio: 0.0 = no discount, 1.0 = 100% off
        df = self._drop(df, df['discount'] < 0, 'discount', 'negative', report)
        df = self._drop(df, df['discount'] > 1, 'discount', 'above_100_pct', report)
        return df

    def _validate_total_amount(self, df, report):
        df = self._drop(df, df['total_amount'].isnull(), 'total_amount', 'null', report)
        df = self._drop(df, df['total_amount'] <= 0, 'total_amount', 'zero_or_negative', report)
        return df

    def _validate_status(self, df, report):
        df['status'] = df['status'].str.strip().str.title()
        df = self._drop(df, ~df['status'].isin(self.VALID_STATUSES), 'status', 'invalid_value', report)
        return df

    def _validate_payment_method(self, df, report):
        df['payment_method'] = df['payment_method'].str.strip().str.title()
        df = self._flag(df, ~df['payment_method'].isin(self.VALID_PAYMENTS),
                        'payment_method', 'unexpected_value', report)
        return df

    def _validate_country(self, df, report):
        # normalize casing only — no strict allowlist for country
        df['country'] = df['country'].str.strip().str.title()
        return df

    def _validate_customer_rating(self, df, report):
        # optional field: flag if missing, drop if out of the 1–5 scale
        df = self._flag(df, df['customer_rating'].isnull(), 'customer_rating', 'null', report)
        invalid = df['customer_rating'].notna() & (
            (df['customer_rating'] < 1) | (df['customer_rating'] > 5)
        )
        df = self._drop(df, invalid, 'customer_rating', 'out_of_range_1_5', report)
        return df

    # ── helpers ───────────────────────────────────────────────────────────────

    def _drop_duplicates(self, df, report):
        before = len(df)
        df = df.drop_duplicates().reset_index(drop=True)
        report.record('ALL', 'exact_duplicate', before - len(df), 'drop')
        return df

    def _drop(self, df, mask, column, rule, report):
        report.record(column, rule, int(mask.sum()), 'drop')
        return df[~mask].reset_index(drop=True)

    def _flag(self, df, mask, column, rule, report):
        count = int(mask.sum())
        report.record(column, rule, count, 'flag')
        if count > 0:
            df.loc[mask, 'validation_flags'] = (
                df.loc[mask, 'validation_flags'].fillna('') + f'[{column}:{rule}]'
            )
        return df
