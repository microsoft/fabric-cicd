from pyspark.sql import SparkSession
import logging
import sys

from pipeline_config import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

if __name__ == "__main__":

    # Step 1: Create Spark Session
    spark = (SparkSession
          .builder
          .appName("sjdsampleapp") 
          .getOrCreate())
    
    spark_context = spark.sparkContext
    spark_context.setLogLevel("ERROR")

    logger.info("=" * 80)
    logger.info("Starting Job")
    logger.info("=" * 80)

    # Step 2: Sample Data Processing
    df = spark.range(config['range_limit']).collect()
    
    logger.info("=" * 80)
    logger.info("Completing Job")
    logger.info("=" * 80)
    
    
       
   
   