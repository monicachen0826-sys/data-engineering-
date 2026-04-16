#!/usr/bin/env python3
import requests
import psycopg2
import os
from datetime import datetime

# Database connection
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5432')
DB_NAME = os.getenv('DB_NAME', 'issdb')
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASS', 'postgres')

# SILVER API endpoint (override with env var if needed)
SILVER_API_URL = os.getenv('SILVER_API_URL', 'https://www.alphavantage.co/query?function=GOLD_SILVER_SPOT&symbol=SILVER&apikey=demo')

def get_silver_price():
    """Fetch latest silver price from SILVER API and normalize output.

    Expected simple response format (example provided by user):
    {
        "nominal": "XAGUSD",
        "timestamp": "2026-04-02 01:04:38",
        "price": "75.1361265624"
    }

    The function attempts to handle that format and a few common fallbacks.
    Returns a dict: {'nominal': str, 'price': float, 'timestamp': datetime}
    """
    try:
        resp = requests.get(SILVER_API_URL, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        # Direct simple format
        if isinstance(data, dict) and 'price' in data:
            price = float(data['price'])
            ts = data.get('timestamp')
            # Parse timestamp string or epoch
            if isinstance(ts, (int, float)):
                timestamp = datetime.utcfromtimestamp(int(ts))
            elif isinstance(ts, str):
                try:
                    timestamp = datetime.fromisoformat(ts)
                except Exception:
                    try:
                        timestamp = datetime.strptime(ts, '%Y-%m-%d %H:%M:%S')
                    except Exception:
                        timestamp = datetime.utcnow()
            else:
                timestamp = datetime.utcnow()

            return {
                'nominal': data.get('nominal'),
                'price': price,
                'timestamp': timestamp
            }

        # Fallback for nested / vendor-specific formats: try to find a numeric value
        def find_number(d):
            if isinstance(d, (int, float)):
                return float(d)
            if isinstance(d, str):
                try:
                    return float(d)
                except Exception:
                    return None
            if isinstance(d, dict):
                for v in d.values():
                    n = find_number(v)
                    if n is not None:
                        return n
            return None

        price_val = find_number(data)
        if price_val is not None:
            return {'nominal': None, 'price': price_val, 'timestamp': datetime.utcnow()}

        raise ValueError('Unexpected SILVER API response format')

    except requests.exceptions.Timeout:
        print('Error: Request to SILVER API timed out after 30 seconds')
        raise
    except requests.exceptions.RequestException as e:
        print(f'Error: Failed to fetch silver price: {e}')
        raise

def store_silver_price(entry):
    """Store silver price entry in PostgreSQL database."""
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

    cursor = None
    try:
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS silver_prices (
                id SERIAL PRIMARY KEY,
                nominal VARCHAR(32),
                price NUMERIC NOT NULL,
                ts TIMESTAMP NOT NULL,
                recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
            )
            """
        )

        # Indexes
        cursor.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_silver_ts ON silver_prices(ts)
            """
        )

        # Insert the price data
        cursor.execute(
            """
            INSERT INTO silver_prices (nominal, price, ts, recorded_at)
            VALUES (%s, %s, %s, %s)
            """,
            (entry.get('nominal'), entry['price'], entry['timestamp'], datetime.utcnow())
        )
        conn.commit()
        print(f"Stored silver price: {entry.get('nominal')} {entry['price']} at {entry['timestamp']}")
    except Exception:
        conn.rollback()
        raise
    finally:
        if cursor is not None:
            cursor.close()
        conn.close()

if __name__ == '__main__':
    try:
        entry = get_silver_price()
        store_silver_price(entry)
    except Exception as e:
        print(f'Fatal error: {e}')
        exit(1)
