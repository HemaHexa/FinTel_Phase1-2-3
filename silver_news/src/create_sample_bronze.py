import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
 
import pyarrow as pa
import pyarrow.parquet as pq
 
 
# --------------------------------------------------
# 1. Define project paths
# --------------------------------------------------
 
PROJECT_ROOT = Path(__file__).resolve().parents[2]
 
OUTPUT_PATH = (
    PROJECT_ROOT
    / "silver_news"
    / "data"
    / "simulated_bronze"
    / "news"
)
 
OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
 
 
# --------------------------------------------------
# 2. Create sample news articles
# --------------------------------------------------
 
articles = [
    {
        "article_id": "NEWS-001",
        "published_at": "2026-11-10T08:30:00Z",
        "source": "Business Standard",
        "headline": "Reliance Industries shares rise after strong quarterly results",
        "full_text": "<p>Reliance Industries reported strong quarterly profits.</p>",
        "url": "https://example.com/reliance-results",
        "ingested_at": "2026-11-10T08:35:00Z"
    },
    {
        "article_id": "NEWS-002",
        "published_at": "2026-11-10T08:32:00Z",
        "source": "Moneycontrol",
        "headline": "SBI shares decline amid banking sector pressure",
        "full_text": "<p>State Bank of India shares declined during trading.</p>",
        "url": "https://example.com/sbi-decline",
        "ingested_at": "2026-11-10T08:36:00Z"
    },
    {
        "article_id": "NEWS-003",
        "published_at": "2026-11-10T08:34:00Z",
        "source": "CNBC-TV18",
        "headline": "Infosys announces expansion in artificial intelligence services",
        "full_text": "<p>Infosys Limited announced new AI service initiatives.</p>",
        "url": "https://example.com/infosys-ai",
        "ingested_at": "2026-11-10T08:37:00Z"
    },
    {
        "article_id": "NEWS-004",
        "published_at": "2026-11-10T08:36:00Z",
        "source": "Business Standard",
        "headline": "HDFC Bank reports steady growth in quarterly deposits",
        "full_text": "<p>HDFC Bank reported steady deposit growth.</p>",
        "url": "https://example.com/hdfc-deposits",
        "ingested_at": "2026-11-10T08:38:00Z"
    },
    {
        "article_id": "NEWS-005",
        "published_at": "2026-11-10T08:38:00Z",
        "source": "Economic Times",
        "headline": "Reliance and TCS attract investor attention",
        "full_text": "<p>Reliance Industries and Tata Consultancy Services attracted investors.</p>",
        "url": "https://example.com/reliance-tcs",
        "ingested_at": "2026-11-10T08:40:00Z"
    },
    {
        "article_id": "NEWS-006",
        "published_at": None,
        "source": "Moneycontrol",
        "headline": "ICICI Bank announces new banking initiatives",
        "full_text": "<p>ICICI Bank announced new initiatives.</p>",
        "url": "https://example.com/icici-news",
        "ingested_at": "2026-11-10T08:42:00Z"
    },
    {
        "article_id": "NEWS-007",
        "published_at": "2026-11-10T08:42:00Z",
        "source": "Business Standard",
        "headline": "Bharti Airtel expands its 5G network",
        "full_text": "<p>Bharti Airtel announced its network expansion.</p>",
        "url": "https://example.com/airtel-5g",
        "ingested_at": "2026-11-10T08:44:00Z"
    },
    {
        "article_id": "NEWS-008",
        "published_at": "2026-11-10T08:45:00Z",
        "source": "Economic Times",
        "headline": "Tata Steel faces demand pressure",
        "full_text": None,
        "url": "https://example.com/tata-steel",
        "ingested_at": "2026-11-10T08:47:00Z"
    }
]
 
 
# --------------------------------------------------
# 3. Create Bronze records
# --------------------------------------------------
 
bronze_records = []
 
base_time = datetime(
    2026, 11, 10, 8, 35,
    tzinfo=timezone.utc
)
 
for index, article in enumerate(articles):
 
    kafka_timestamp = base_time + timedelta(minutes=index)
 
    record = {
        "kafka_key": article["article_id"],
        "raw_json": json.dumps(article),
        "topic": "raw-news-events",
        "partition": index % 3,
        "offset": 125 + index,
        "kafka_ts": kafka_timestamp
    }
 
    bronze_records.append(record)
 
 
# --------------------------------------------------
# 4. Define the Bronze schema
# --------------------------------------------------
 
bronze_schema = pa.schema([
    pa.field("kafka_key", pa.string()),
    pa.field("raw_json", pa.string(), nullable=False),
    pa.field("topic", pa.string(), nullable=False),
    pa.field("partition", pa.int32(), nullable=False),
    pa.field("offset", pa.int64(), nullable=False),
    pa.field(
        "kafka_ts",
        pa.timestamp("us", tz="UTC"),
        nullable=False
    )
])
 
 
# --------------------------------------------------
# 5. Convert records into a PyArrow table
# --------------------------------------------------
 
table = pa.Table.from_pylist(
    bronze_records,
    schema=bronze_schema
)
 
 
# --------------------------------------------------
# 6. Write Parquet file
# --------------------------------------------------
 
output_file = OUTPUT_PATH / "part-00000.parquet"
 
pq.write_table(
    table,
    output_file,
    compression="snappy"
)
 
 
# --------------------------------------------------
# 7. Verify the created Parquet file
# --------------------------------------------------
 
print("\nSIMULATED BRONZE SCHEMA")
print(table.schema)
 
print("\nSIMULATED BRONZE RECORDS")
for record in table.to_pylist():
    print(
        record["kafka_key"],
        record["topic"],
        record["partition"],
        record["offset"],
        record["kafka_ts"]
    )
 
print("\nTOTAL RECORDS:", table.num_rows)
 
print("\nPARQUET FILE CREATED SUCCESSFULLY!")
print("Output location:", output_file)
 
# Read it back to verify
verification_table = pq.read_table(output_file)
 
print("\nPARQUET VERIFICATION")
print("Number of records:", verification_table.num_rows)
print("Schema matches:", verification_table.schema == bronze_schema)
