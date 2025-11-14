import csv
import sqlite3
from pathlib import Path
from typing import Callable, Dict, List, Sequence, Tuple


BASE_DIR = Path(__file__).parent
DB_PATH = BASE_DIR / "ecommerce.db"


TABLE_DEFINITIONS: Dict[str, str] = {
    "users": """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TEXT NOT NULL
        )
    """,
    "categories": """
        CREATE TABLE IF NOT EXISTS categories (
            category_id INTEGER PRIMARY KEY,
            category_name TEXT NOT NULL
        )
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS products (
            product_id INTEGER PRIMARY KEY,
            product_name TEXT NOT NULL,
            category_id INTEGER NOT NULL,
            price REAL NOT NULL,
            FOREIGN KEY (category_id) REFERENCES categories(category_id)
        )
    """,
    "orders": """
        CREATE TABLE IF NOT EXISTS orders (
            order_id INTEGER PRIMARY KEY,
            user_id INTEGER NOT NULL,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    """,
    "order_items": """
        CREATE TABLE IF NOT EXISTS order_items (
            item_id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders(order_id),
            FOREIGN KEY (product_id) REFERENCES products(product_id)
        )
    """,
}


class IngestionConfig:
    def __init__(
        self,
        table: str,
        csv_file: str,
        columns: Sequence[str],
        converters: Dict[str, Callable[[str], object]],
    ) -> None:
        placeholders = ", ".join(["?"] * len(columns))
        column_list = ", ".join(columns)
        self.table = table
        self.csv_file = BASE_DIR / csv_file
        self.columns = columns
        self.converters = converters
        self.insert_sql = f"INSERT INTO {table} ({column_list}) VALUES ({placeholders})"


INGESTION_CONFIGS: List[IngestionConfig] = [
    IngestionConfig(
        table="users",
        csv_file="users.csv",
        columns=("user_id", "username", "email", "created_at"),
        converters={
            "user_id": int,
        },
    ),
    IngestionConfig(
        table="categories",
        csv_file="categories.csv",
        columns=("category_id", "category_name"),
        converters={
            "category_id": int,
        },
    ),
    IngestionConfig(
        table="products",
        csv_file="products.csv",
        columns=("product_id", "product_name", "category_id", "price"),
        converters={
            "product_id": int,
            "category_id": int,
            "price": float,
        },
    ),
    IngestionConfig(
        table="orders",
        csv_file="orders.csv",
        columns=("order_id", "user_id", "order_date", "status"),
        converters={
            "order_id": int,
            "user_id": int,
        },
    ),
    IngestionConfig(
        table="order_items",
        csv_file="order_items.csv",
        columns=("item_id", "order_id", "product_id", "quantity"),
        converters={
            "item_id": int,
            "order_id": int,
            "product_id": int,
            "quantity": int,
        },
    ),
]


def ensure_csv_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Required CSV file not found: {path}")


def create_tables(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    for name, ddl in TABLE_DEFINITIONS.items():
        cursor.execute(ddl)
    connection.commit()


def clear_tables(connection: sqlite3.Connection) -> None:
    cursor = connection.cursor()
    deletion_order = ["order_items", "orders", "products", "categories", "users"]
    for table_name in deletion_order:
        cursor.execute(f"DELETE FROM {table_name}")
    connection.commit()


def parse_row(row: Dict[str, str], config: IngestionConfig) -> Tuple[object, ...]:
    parsed: List[object] = []
    for column in config.columns:
        value = row[column]
        converter = config.converters.get(column, lambda x: x)
        parsed.append(converter(value))
    return tuple(parsed)


def ingest_table(connection: sqlite3.Connection, config: IngestionConfig) -> None:
    ensure_csv_exists(config.csv_file)
    cursor = connection.cursor()
    with config.csv_file.open("r", newline="", encoding="utf-8") as file_handle:
        reader = csv.DictReader(file_handle)
        rows = [parse_row(row, config) for row in reader]
    cursor.executemany(config.insert_sql, rows)
    connection.commit()
    print(f"Successfully ingested {len(rows)} rows from {config.csv_file.name} into {config.table}.")


def main() -> None:
    connection = sqlite3.connect(DB_PATH)
    connection.execute("PRAGMA foreign_keys = ON;")
    try:
        create_tables(connection)
        clear_tables(connection)
        for config in INGESTION_CONFIGS:
            try:
                ingest_table(connection, config)
            except Exception as error:
                print(f"Failed to ingest {config.csv_file.name}: {error}")
                raise
    finally:
        connection.close()


if __name__ == "__main__":
    main()

