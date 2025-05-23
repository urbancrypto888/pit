from datetime import datetime
from point_in_time_data import PointInTimeData  # adjust import if using notebook/module
import pandas as pd

# Initialize PIT store with composite key and multi-column value fields
pit = PointInTimeData(
    key_columns=["symbol", "source"],
    value_columns=["price", "volume"],
    default_overlap_mode="replace"
)

# Insert initial rows
pit.batch_upsert([
    {"symbol": "AAPL", "source": "vendor1", "from_time": "2024-01-01", "price": 150, "volume": 10000},
    {"symbol": "AAPL", "source": "vendor2", "from_time": "2024-01-01", "price": 151, "volume": 11000},
    {"symbol": "MSFT", "source": "vendor1", "from_time": "2024-01-01", "price": 300, "volume": 20000}
])

# Upsert a new version for AAPL-vendor1
pit.upsert({
    "symbol": "AAPL", "source": "vendor1", "from_time": "2024-03-01", "price": 155, "volume": 10500
})

# Perform a logical delete on MSFT-vendor1
pit.delete({"symbol": "MSFT", "source": "vendor1"}, delete_time="2024-04-15")

# Get snapshot at specific time
print("Snapshot on 2024-03-15:")
print(pit.get_active("2024-03-15"))

# Show full version history for AAPL
print("\nFull history for AAPL:")
print(pit.full_history({"symbol": "AAPL"}))

# Display latest active records
print("\nLatest active rows:")
print(pit.latest())
