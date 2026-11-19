import os
import sys
import time
from pathlib import Path
 
import pandas as pd
import torch
 
from bs4 import BeautifulSoup
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification
)
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, concat_ws
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType
)
 
 
# --------------------------------------------------
# 1. CONFIGURATION
# --------------------------------------------------
 
# Use the same Python interpreter for Spark workers
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
 
PROJECT_FOLDER = Path(__file__).resolve().parents[1]
 
INPUT_FILE = PROJECT_FOLDER / "data" / "sample_bronze_news.json"
 
MODEL_PATH = PROJECT_FOLDER / "models" / "finbert"
 
print("Input file:", INPUT_FILE)
print("FinBERT model:", MODEL_PATH)
 
 
# --------------------------------------------------
# 2. CREATE SPARK SESSION
# --------------------------------------------------
 
spark = (
    SparkSession.builder
    .appName("FinBERT_PySpark_Integration")
    .master("local[1]")
    .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    .config("spark.sql.execution.arrow.maxRecordsPerBatch", "8")
    .getOrCreate()
)
 
spark.sparkContext.setLogLevel("ERROR")
 
 
# --------------------------------------------------
# 3. CLEAN HTML
# --------------------------------------------------
 
def clean_html(html):
 
    if html is None:
        return ""
 
    soup = BeautifulSoup(str(html), "html.parser")
 
    text = soup.get_text(separator=" ", strip=True)
 
    return " ".join(text.split())
 
 
# --------------------------------------------------
# 4. DEFINE SENTIMENT OUTPUT SCHEMA
# --------------------------------------------------
 
sentiment_schema = StructType([
 
    StructField("sentiment", StringType(), True),
 
    StructField("positive_probability", DoubleType(), True),
 
    StructField("negative_probability", DoubleType(), True),
 
    StructField("neutral_probability", DoubleType(), True),
 
    StructField("sentiment_score", DoubleType(), True)
 
])
 
 
# --------------------------------------------------
# 5. CREATE PANDAS UDF
# --------------------------------------------------
 
@pandas_udf(sentiment_schema)
def finbert_sentiment_udf(news_series: pd.Series) -> pd.DataFrame:
 
    # Load the model once per Python worker process
    # Cache it for subsequent batches handled by that worker.
 
    if not hasattr(finbert_sentiment_udf, "model"):
 
        print("Loading FinBERT inside Spark worker...")
 
        tokenizer = AutoTokenizer.from_pretrained(
            str(MODEL_PATH),
            local_files_only=True
        )
 
        model = AutoModelForSequenceClassification.from_pretrained(
            str(MODEL_PATH),
            local_files_only=True
        )
 
        device = torch.device(
            "cuda" if torch.cuda.is_available() else "cpu"
        )
 
        model.to(device)
        model.eval()
 
        finbert_sentiment_udf.tokenizer = tokenizer
        finbert_sentiment_udf.model = model
        finbert_sentiment_udf.device = device
        finbert_sentiment_udf.id2label = model.config.id2label
 
        print("FinBERT loaded successfully in Spark worker.")
 
    tokenizer = finbert_sentiment_udf.tokenizer
    model = finbert_sentiment_udf.model
    device = finbert_sentiment_udf.device
    id2label = finbert_sentiment_udf.id2label
 
    # Clean missing values and prepare a batch
    texts = [
        str(text) if pd.notna(text) else ""
        for text in news_series
    ]
 
    # Tokenize the complete batch
    inputs = tokenizer(
        texts,
        return_tensors="pt",
        padding=True,
        truncation=True,
        max_length=512
    )
 
    inputs = {
        key: value.to(device)
        for key, value in inputs.items()
    }
 
    # Run FinBERT on the batch
    with torch.no_grad():
 
        outputs = model(**inputs)
 
        probabilities = torch.softmax(
            outputs.logits,
            dim=-1
        )
 
    # Move predictions back to CPU
    probabilities = probabilities.cpu().numpy()
 
    # Prepare results
    results = []
 
    for row in probabilities:
 
        scores = {
            str(id2label[index]).lower(): float(row[index])
            for index in range(len(row))
        }
 
        sentiment = max(scores, key=scores.get)
 
        positive = scores.get("positive", 0.0)
        negative = scores.get("negative", 0.0)
        neutral = scores.get("neutral", 0.0)
 
        sentiment_score = positive - negative
 
        results.append({
 
            "sentiment": sentiment,
 
            "positive_probability": positive,
 
            "negative_probability": negative,
 
            "neutral_probability": neutral,
 
            "sentiment_score": sentiment_score
 
        })
 
    return pd.DataFrame(results)
 
 
# --------------------------------------------------
# 6. READ BRONZE NEWS
# --------------------------------------------------
 
try:
 
    print("\nReading Bronze news...")
 
    bronze_df = (
        spark.read
        .option("multiLine", "true")
        .json(str(INPUT_FILE))
    )
 
    print("\nBronze news records:")
 
    bronze_df.select(
        "article_id",
        "headline"
    ).show(truncate=False)
 
 
    # --------------------------------------------------
    # 7. REMOVE DUPLICATES
    # --------------------------------------------------
 
    unique_df = bronze_df.dropDuplicates(["article_id"])
 
 
    # --------------------------------------------------
    # 8. CLEAN NEWS TEXT
    # --------------------------------------------------
 
    from pyspark.sql.functions import udf
 
    clean_html_udf = udf(clean_html, StringType())
 
    cleaned_df = unique_df.withColumn(
        "cleaned_text",
        clean_html_udf(col("body_html"))
    )
 
    # Combine headline and article body
    prepared_df = cleaned_df.withColumn(
        "search_text",
        concat_ws(
            " ",
            col("headline"),
            col("cleaned_text")
        )
    )
 
 
    # --------------------------------------------------
    # 9. APPLY FINBERT PANDAS UDF
    # --------------------------------------------------
 
    print("\nApplying FinBERT sentiment analysis...")
 
    start_time = time.perf_counter()
 
    result_df = prepared_df.withColumn(
        "sentiment_result",
        finbert_sentiment_udf(col("search_text"))
    )
 
    # Extract the sentiment fields
    final_df = (
        result_df
        .withColumn(
            "sentiment",
            col("sentiment_result.sentiment")
        )
        .withColumn(
            "positive_probability",
            col("sentiment_result.positive_probability")
        )
        .withColumn(
            "negative_probability",
            col("sentiment_result.negative_probability")
        )
        .withColumn(
            "neutral_probability",
            col("sentiment_result.neutral_probability")
        )
        .withColumn(
            "sentiment_score",
            col("sentiment_result.sentiment_score")
        )
        .drop("sentiment_result")
    )
 
 
    # --------------------------------------------------
    # 10. DISPLAY RESULTS
    # --------------------------------------------------
 
    print("\nFINBERT + PYSPARK RESULTS")
 
    final_df.select(
        "article_id",
        "headline",
        "sentiment",
        "positive_probability",
        "negative_probability",
        "neutral_probability",
        "sentiment_score"
    ).show(truncate=False)
 
    # Trigger Spark execution and measure elapsed time
    total_records = final_df.count()
 
    end_time = time.perf_counter()
 
    print("\nTOTAL PROCESSED ARTICLES:", total_records)
 
    print(
        "TOTAL PROCESSING TIME:",
        round(end_time - start_time, 3),
        "seconds"
    )
 
 
finally:
 
    spark.stop()
 
    print("\nSpark session stopped.")