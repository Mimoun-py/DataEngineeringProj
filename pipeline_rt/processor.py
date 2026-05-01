import pandas as pd
import logging


class Processor:

    def process(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()

        # ── New column 1: revenue_after_discount ─────────────────────────────
        # Recalculate cleanly from source columns so we know it's correct,
        # regardless of what total_amount said in the raw file.
        df['revenue_after_discount'] = (
            df['unit_price'] * df['quantity'] * (1 - df['discount'])
        ).round(2)
        logging.info("Added column: revenue_after_discount")

        # ── New column 2: discount_percentage ────────────────────────────────
        # Human-readable version: 0.15 → 15.0
        df['discount_percentage'] = (df['discount'] * 100).round(1)
        logging.info("Added column: discount_percentage")

        # ── New column 3: price_category ─────────────────────────────────────
        # Categorise the order by unit price
        df['price_category'] = pd.cut(
            df['unit_price'],
            bins=[0, 30, 150, float('inf')],
            labels=['Budget', 'Mid-range', 'Premium'],
            right=True
        )
        logging.info("Added column: price_category")

        # ── New column 4: order_year ──────────────────────────────────────────
        df['order_year'] = df['order_date'].dt.year
        logging.info("Added column: order_year")

        # ── New column 5: order_month ─────────────────────────────────────────
        df['order_month'] = df['order_date'].dt.month
        logging.info("Added column: order_month")

        logging.info(f"Processor complete. Final shape: {df.shape[0]} rows × {df.shape[1]} columns")
        return df
