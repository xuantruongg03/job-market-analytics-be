from threading import Lock
from pyspark.sql import SparkSession
import os
import sys

# Thiết lập Python path cho PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

_spark_session = None
_spark_lock = Lock()

def get_spark_session():
    """
    Tạo hoặc lấy Spark session (singleton pattern)
    
    Returns:
        SparkSession: Spark session instance
    """
    global _spark_session
    with _spark_lock:
        if _spark_session is None:
            _spark_session = SparkSession.builder \
                .appName("JobSkillsAnalysis") \
                .config("spark.sql.adaptive.enabled", "true") \
                .getOrCreate()
            _spark_session.sparkContext.setLogLevel("ERROR")
    return _spark_session

def read_file(file_path):
    import json
    results = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            item = json.loads(line)
            results.append(item)

    return results

def read_file_by_pyspark(file_path):
    """Đọc file JSON bằng PySpark"""
    spark = get_spark_session()
    df = spark.read.json(file_path)
    return df
# Nhập mãng skill, quốc gia => nghề

def filter_and_aggregate_jobs(df, search_country, input_skills):
    from pyspark.sql.functions import col, explode, split, lower, trim, count, collect_list, struct
    from pyspark.sql.types import BooleanType
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number
    from pyspark.sql.functions import udf
    # Chuẩn hóa kỹ năng đầu vào
    input_skills_set = set([s.strip().lower() for s in input_skills])

    # Bước 1: Lọc theo quốc gia
    filtered_df = df.filter(lower(col("search_country")) == search_country.lower())

    # Bước 2: Tách kỹ năng thành mảng
    skills_df = filtered_df.withColumn(
        "skill_array",
        split(trim(col("job_skills")), ",\\s*")
    )

    # Bước 3: Giữ lại dòng có đủ tất cả kỹ năng trong input_skills
    def skill_match(skills):
        skill_set = set([s.strip().lower() for s in skills])
        return input_skills_set.issubset(skill_set)

    skill_match_udf = udf(skill_match, BooleanType())
    matched_df = skills_df.filter(skill_match_udf(col("skill_array")))

    # Đếm số lượng bản ghi theo job_title (đếm thật)
    job_counts = matched_df.groupBy("job_title").agg(count("*").alias("count"))

    # Bước 4: Tách skill ra từng dòng
    exploded = matched_df.select("job_title", explode(col("skill_array")).alias("skill"))
    exploded = exploded.withColumn("skill", trim(lower(col("skill"))))

    # Bước 5: Đếm tần suất skill theo job_title
    skill_counts = exploded.groupBy("job_title", "skill").agg(count("*").alias("weight"))

    # Bước 6: Lấy top 10 kỹ năng theo job_title
    window = Window.partitionBy("job_title").orderBy(col("weight").desc())
    ranked_skills = skill_counts.withColumn("rank", row_number().over(window)).filter(col("rank") <= 10)

    # Tạo skill object dạng {"skill": ..., "weight": ...}
    skill_struct = ranked_skills.select(
        "job_title",
        struct(col("skill"), col("weight")).alias("skill_obj")
    )

    # Gom lại theo job_title
    skill_result = skill_struct.groupBy("job_title") \
        .agg(collect_list("skill_obj").alias("skills"))

    # Kết hợp với số lượng bản ghi ban đầu
    final_result = skill_result.join(job_counts, on="job_title", how="left")
    
    import json
    return [json.loads(row) for row in final_result.limit(20).toJSON().collect()] 


if __name__ == "__main__":
    try:
        spark = get_spark_session()
        df = spark.read.csv("combined_csv_final.csv", header=True, inferSchema=True)

        search_country = "Việt Nam"
        input_skills = ["auditing", "accounting","office","university graduate","financial skills"]

        result = filter_and_aggregate_jobs(df, search_country, input_skills)
        # result.show(truncate=False)
        print(result)
        spark.stop()
    except KeyboardInterrupt:
        print("\nServer đã dừng")
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        # Cleanup Spark session
        if _spark_session:
            _spark_session.stop()
            print("Đã đóng Spark session")