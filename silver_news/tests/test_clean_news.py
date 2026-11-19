 
import os
import sys
from pathlib import Path
 
# --------------------------------------------------
# 1. Configure project paths
# --------------------------------------------------
 
PROJECT_ROOT = Path(__file__).resolve().parents[2]
 
SRC_PATH = PROJECT_ROOT / "silver_news" / "src"
 
sys.path.insert(0, str(SRC_PATH))
 
# --------------------------------------------------
# 2. Configure Python for Spark
# --------------------------------------------------
 
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
 
# --------------------------------------------------
# 3. Import project modules
# --------------------------------------------------
 
from read_bronze import read_new_bronze
from deduplicate_news import deduplicate_news
from validate_news import validate_news
from clean_news import clean_news
 
 
# --------------------------------------------------
# 4. Create Spark session
# --------------------------------------------------
 
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("TestActualNewsCleaning")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)
 
spark.sparkContext.setLogLevel("ERROR")
 
 
try:
 
    # --------------------------------------------------
    # 5. Read actual Bronze records
    # --------------------------------------------------
 
    print("\n========== STEP 1: READ BRONZE ==========")
 
    bronze_df, files = read_new_bronze(spark)
 
    bronze_count = bronze_df.count()
 
    print("Bronze files:", len(files))
    print("Bronze records:", bronze_count)
 
    # --------------------------------------------------
    # 6. Deduplicate articles
    # --------------------------------------------------
 
    print("\n========== STEP 2: DEDUPLICATION ==========")
 
    unique_df, missing_df = deduplicate_news(bronze_df)
 
    print("Unique records:", unique_df.count())
    print("Records with missing article IDs:", missing_df.count())
 
    # Keep records with missing article IDs for validation.
    candidates_df = unique_df.unionByName(
        missing_df,
        allowMissingColumns=True
    )
 
    # --------------------------------------------------
    # 7. Validate records
    # --------------------------------------------------
 
    print("\n========== STEP 3: VALIDATION ==========")
 
    valid_df, invalid_df = validate_news(candidates_df)
 
    print("Valid records:", valid_df.count())
    print("Invalid records:", invalid_df.count())
 
    # --------------------------------------------------
    # 8. Clean valid records
    # --------------------------------------------------
 
    print("\n========== STEP 4: HTML CLEANING ==========")
 
    cleaned_df = clean_news(valid_df)
 
    cleaned_count = cleaned_df.count()
 
    print("\nCLEANED NEWS RECORDS:")
 
    cleaned_df.select(
        "headline",
        "cleaned_full_text",
        "text_for_analysis"
    ).show(20, truncate=False)
 
    # --------------------------------------------------
    # 9. Cleaning quality checks
    # --------------------------------------------------
 
    print("\n========== STEP 5: CLEANING CHECKS ==========")
 
    empty_analysis_count = cleaned_df.filter(
        col("text_for_analysis").isNull()
        | (trim(col("text_for_analysis")) == "")
    ).count()
 
    print("Total cleaned records:", cleaned_count)
 
    print("Empty analysis text:", empty_analysis_count)
 
    # --------------------------------------------------
    # 10. Final result
    # --------------------------------------------------
 
    print("\n========== CLEANING TEST SUMMARY ==========")
 
    print("Bronze records:", bronze_count)
    print("Cleaned records:", cleaned_count)
    print("Invalid records:", invalid_df.count())
    print("Empty analysis text:", empty_analysis_count)
 
    if cleaned_count > 0 and empty_analysis_count == 0:
        print("\nCLEANING TEST PASSED!")
    else:
        print("\nCLEANING TEST NEEDS REVIEW.")
 
finally:
 
    spark.stop()
 
    print("\nSPARK SESSION STOPPED.")
 