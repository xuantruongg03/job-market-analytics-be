from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, trim, lower, col, count, when, lit
from pyspark.sql.types import *
from threading import Lock
from pyspark.sql.functions import col, explode, split, lower, trim, count, collect_list, struct
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from pyspark.sql.functions import udf
import os
import sys
import threading
import time
from format_job import normalize_title
from dotenv import load_dotenv

load_dotenv()

# Thiết lập Python path cho PySpark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

_spark_session = None
_spark_lock = Lock()

_global_cache = {
    'skill_counts_worldwide': None,
    'skill_counts_by_country': None,
    'job_count_worldwide': None,
    'job_count_by_country': None,
    'job_counts_by_month': None,
    'vietnamese_skills': None,
    'countries': None,
    'raw_job_data': None,
    'processed_skills_data': None
}
_cache_initialized = False
_cache_loading = False

def initialize_global_cache():
    """Khởi tạo cache toàn cầu khi startup"""
    global _cache_initialized, _cache_loading
    
    if _cache_initialized or _cache_loading:
        return
    
    _cache_loading = True
    print("Đang khởi tạo cache toàn cầu...")
    start_time = time.time()
    
    def load_skill_worldwide():
        """Load skill worldwide data"""
        try:
            spark = get_spark_session()
            path = os.getenv("SKILL_COUNTS")
            df = spark.read.json(path)
            result = df.select("skill", "count") \
                .orderBy(col("count").desc()) \
                .collect()
            
            _global_cache['skill_counts_worldwide'] = [
                {"skill": row['skill'], "count": row['count']} 
                for row in result
            ]
            print("Loaded skill_counts_worldwide")
        except Exception as e:
            print(f"Error loading skill_counts_worldwide: {e}")
            _global_cache['skill_counts_worldwide'] = []
    
    def load_skill_by_country():
        """Load skill by country data"""
        try:
            spark = get_spark_session()
            path = os.getenv("SKILL_COUNTS_BY_COUNTRY")
            df = spark.read.json(path)
            result = df.select("skill", "count", "search_country") \
                .orderBy(col("count").desc()) \
                .collect()
            
            _global_cache['skill_counts_by_country'] = [
                {
                    "skill": row['skill'], 
                    "count": row['count'],
                    "search_country": row['search_country']
                } 
                for row in result
            ]
            print("Loaded skill_counts_by_country")
        except Exception as e:
            print(f"Error loading skill_counts_by_country: {e}")
            _global_cache['skill_counts_by_country'] = []
    
    def load_other_data():
        """Load các data khác"""
        try:
            path = os.getenv("JOB_COUNTS")
            job_counts_data = read_file_by_pyspark_safe(path)
            _global_cache['job_counts_worldwide'] = job_counts_data if isinstance(job_counts_data, list) else []
            print("Loaded job_counts_worldwide")  

            path = os.getenv("JOB_COUNTS_BY_COUNTRY")
            job_counts_by_country_data = read_file_by_pyspark_safe(path)
            processed_country_data = []
            if isinstance(job_counts_by_country_data, list):
                for country_item in job_counts_by_country_data:
                    if isinstance(country_item, dict) and 'data' in country_item:
                        processed_jobs = []
                        for job in country_item['data']:
                            if hasattr(job, 'asDict'):
                                # Row object từ PySpark
                                job_dict = job.asDict()
                                processed_jobs.append({
                                    "job_title": job_dict.get('job_title', ''),
                                    "count": job_dict.get('count', 0)
                                })
                            elif isinstance(job, dict):
                                processed_jobs.append({
                                    "job_title": job.get('job_title', ''),
                                    "count": job.get('count', 0)
                                })
                        
                        processed_country_data.append({
                            "search_country": country_item.get('search_country', ''),
                            "data": processed_jobs
                        })
            
            _global_cache['job_counts_by_country'] = processed_country_data
            print(f"Loaded and processed {len(processed_country_data)} job_counts_by_country records")

            path = os.getenv("JOB_COUNTS_BY_MONTH")
            job_counts_by_month_data = read_file_by_pyspark_safe(path)
            processed_month_data = []
            if isinstance(job_counts_by_month_data, list):
                for item in job_counts_by_month_data:
                    if isinstance(item, dict) and 'month_year' in item:
                        # Xử lý jobs nếu có Row objects
                        if 'jobs' in item and isinstance(item['jobs'], list):
                            jobs_list = []
                            for job in item['jobs']:
                                # Kiểm tra nếu là Row object từ PySpark
                                if hasattr(job, 'asDict'):
                                    job_dict = job.asDict()
                                    jobs_list.append({
                                        "job_title": job_dict.get('job_title', ''),
                                        "count": job_dict.get('count', 0)
                                    })
                                elif isinstance(job, dict):
                                    jobs_list.append({
                                        "job_title": job.get('job_title', ''),
                                        "count": job.get('count', 0)
                                    })
                            
                            # Sắp xếp jobs theo count giảm dần
                            jobs_list = sorted(jobs_list, key=lambda x: x['count'], reverse=True)
                            
                            processed_month_data.append({
                                "month_year": item['month_year'],
                                "jobs": jobs_list
                            })
                           

            _global_cache['job_counts_by_month'] = processed_month_data
            print(f"Loaded and processed {len(processed_month_data)} job_counts_by_month records")

            path = os.getenv("VIETNAMESE_SKILLS")
            vietnamese_skills_data = read_file_by_pyspark_safe(path)
            _global_cache['vietnamese_skills'] = vietnamese_skills_data if isinstance(vietnamese_skills_data, list) else []
            print("Loaded vietnamese_skills")

            countries = set()
            for item in _global_cache['job_counts_by_country']:
                if isinstance(item, dict) and 'search_country' in item and item['search_country']:
                    countries.add(item['search_country'])
            _global_cache['countries'] = list(countries)
            print(f"Loaded {len(_global_cache['countries'])} countries")

        except Exception as e:
            print(f"Error loading other data: {e}")
            _global_cache['job_counts_worldwide'] = []
            _global_cache['job_counts_by_country'] = []
            _global_cache['job_counts_by_month'] = []
            _global_cache['vietnamese_skills'] = []
            _global_cache['countries'] = []

    def load_raw_job_data():
        try:
            spark = get_spark_session()
            df = spark.read.option("header", "true") \
                .option("inferSchema", "false") \
                .csv(os.getenv("RAW_JOB_DATA"))
            selected_df = df
            
            selected_df.cache()
            _global_cache['raw_job_data'] = selected_df
        except Exception as e:
            print(f"Error loading raw job data: {e}")
            _global_cache['raw_job_data'] = None
    
    try:
        load_skill_worldwide()
        load_skill_by_country()
        load_other_data()
        load_raw_job_data() 
    except Exception as e:
        print(f"Error during cache initialization: {e}")
    
    _cache_initialized = True
    _cache_loading = False
    
    end_time = time.time()
    print(f"Cache khởi tạo hoàn thành trong {end_time - start_time:.2f} giây")

def read_file_by_pyspark_safe(file_path):
    """Đọc file JSON bằng PySpark với xử lý lỗi an toàn"""
    try:
        spark = get_spark_session()
        df = spark.read.json(file_path)
        return [row.asDict() for row in df.collect()]
    except Exception as e:
        print(f"Error reading {file_path}: {e}")

def wait_for_cache():
    """Đợi cache load xong"""
    while _cache_loading:
        time.sleep(0.1)
    
    if not _cache_initialized:
        initialize_global_cache()

def get_spark_session():
    """
    Tạo hoặc lấy Spark session (singleton pattern)
    
    Returns:
        SparkSession: Spark session instance
    """
    global _spark_session
    with _spark_lock:
        if _spark_session is None or _spark_session.sparkContext._jsc is None:
            try:
                if _spark_session is not None:
                    _spark_session.stop()
            except:
                pass
            
            # Tạo session mới
            _spark_session = SparkSession.builder \
                .appName("JobSkillsAnalysis") \
                .master("local[2]") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.pyspark.python", sys.executable) \
                .config("spark.pyspark.driver.python", sys.executable) \
                .config("spark.driver.memory", "2g") \
                .config("spark.driver.maxResultSize", "1g") \
                .config("spark.executor.memory", "2g") \
                .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
                .config("spark.hadoop.io.native.lib.available", "false") \
                .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "64MB") \
                .getOrCreate()
            _spark_session.sparkContext.setLogLevel("ERROR")
            # _spark_session = SparkSession.builder \
            #     .appName("Read B2 via S3") \
            #     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
            #     .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-005.backblazeb2.com") \
            #     .config("spark.hadoop.fs.s3a.access.key", os.getenv("ACCESS_KEY_ID")) \
            #     .config("spark.hadoop.fs.s3a.secret.key", os.getenv("SECRET_ACCESS_KEY")) \
            #     .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true") \
            #     .config("spark.hadoop.fs.s3a.path.style.access", "true") \
            #     .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            #     .config("spark.driver.memory", "2g") \
            #     .config("spark.executor.memory", "2g") \
            #     .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            #     .getOrCreate()

    return _spark_session

def reset_spark_session():
    """Reset Spark session"""
    global _spark_session
    with _spark_lock:
        if _spark_session is not None:
            try:
                _spark_session.stop()
            except:
                pass
            _spark_session = None


def analyze_job_skills(job_title, country, limit=15):
    try:
        wait_for_cache()
        limit = int(limit)
        raw_data_df = _global_cache['raw_job_data']
        job_title = normalize_title(job_title) == "Unknown" and job_title or normalize_title(job_title)
        if raw_data_df is None:
            return {
                "status": "error",
                "message": "Không có dữ liệu raw job data để phân tích",
                "job_title": job_title,
                "country": country
            }
        filtered_jobs_df = raw_data_df.filter(
            (lower(col("job_title")).contains(job_title.lower())) &
            (lower(col("search_country")).contains(country.lower()))
        )

        total_jobs = filtered_jobs_df.count()
        if total_jobs == 0:
            result = {
                "status": "no_data",
                "message": f"Không tìm thấy dữ liệu cho nghề '{job_title}' tại quốc gia '{country}'",
                "job_title": job_title,
                "country": country,
                "total_jobs": 0,
                "total_skills": 0,
                "skills": []
            }
            return result

        # Tách skills và explode để đếm tần suất
        skills_df = filtered_jobs_df.withColumn(
            "skill_array",
            split(trim(col("job_skills")), ",\\s*")
        )
        
        exploded_skills = skills_df.select(explode(col("skill_array")).alias("skill")) \
            .withColumn("skill", trim(col("skill"))) \
            .filter(col("skill") != "")
        
        # Đếm tần suất tất cả skills (không giới hạn để tính other skills)
        all_skill_counts = exploded_skills.groupBy("skill") \
            .agg(count("*").alias("frequency")) \
            .orderBy(col("frequency").desc())
        
        # Lấy tổng số lần xuất hiện của tất cả skills
        total_skill_occurrences = exploded_skills.count()
        
        # Lấy top skills theo limit
        top_skill_counts = all_skill_counts.limit(limit)
        skills_data = top_skill_counts.collect()
        
        # Tính tổng frequency của top skills
        top_skills_frequency = sum(row['frequency'] for row in skills_data)
        
        # Tính frequency của other skills
        other_skills_frequency = total_skill_occurrences - top_skills_frequency
        
        skills_list = []
        top_skills_percentage = 0
        
        for i, row in enumerate(skills_data, 1):
            percentage = round((row['frequency'] / total_skill_occurrences) * 100, 2)
            top_skills_percentage += percentage
            skills_list.append({
                "rank": i,
                "skill": row['skill'],
                "frequency": row['frequency'],
                "percentage": percentage
            })
        
        # Thêm other skills nếu có
        if other_skills_frequency > 0:
            other_percentage = round(100 - top_skills_percentage, 2)
            # Đảm bảo other_percentage không âm
            if other_percentage > 0:
                skills_list.append({
                    "rank": len(skills_list) + 1,
                    "skill": "Other Skills",
                    "frequency": other_skills_frequency,
                    "percentage": other_percentage
                })
          # Điều chỉnh để đảm bảo tổng percentage = 100%
        total_percentage = sum(skill['percentage'] for skill in skills_list)
        if total_percentage != 100 and skills_list:
            # Điều chỉnh skill có percentage cao nhất
            max_skill = max(skills_list, key=lambda x: x['percentage'])
            max_skill['percentage'] = round(max_skill['percentage'] + (100 - total_percentage), 2)
        
        total_unique_skills = all_skill_counts.count()
        
        result = {
            "status": "success",
            "job_title": job_title,
            "country": country,
            "total_jobs": total_jobs,
            "total_unique_skills": total_unique_skills,
            "total_skill_occurrences": total_skill_occurrences,
            "top_skills": skills_list,
            "percentage_validation": {
                "total_percentage": sum(skill['percentage'] for skill in skills_list),
                "is_100_percent": abs(sum(skill['percentage'] for skill in skills_list) - 100) < 0.01
            }
        }
        
        return result
        
    except Exception as e:
        error_result = {
            "status": "error",
            "message": f"Lỗi xử lý dữ liệu: {str(e)}",
            "job_title": job_title,
            "country": country
        }
        return error_result

def filter_and_aggregate_jobs(search_country, input_skills, csv_file="combined_csv_final.csv"):
    wait_for_cache()
    df = _global_cache['raw_job_data']
    # Chuẩn hóa kỹ năng đầu vào
    input_skills_set = set([s.strip().lower() for s in input_skills])
    # Bước 1: Lọc theo quốc gia
    filtered_df = df.filter(lower(col("search_country")) == search_country.lower())
    print(filtered_df)
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

def stop_spark_session():
    """Dừng Spark session"""
    global _spark_session
    with _spark_lock:
        if _spark_session is not None:
            _spark_session.stop()
            _spark_session = None

def get_countries():
    """Lấy danh sách các quốc gia từ file"""
    wait_for_cache() 
    countries = _global_cache['countries']
    return countries

def read_file_by_pyspark(file_path):
    """Đọc file JSON bằng PySpark"""
    try:
        spark = get_spark_session()
        df = spark.read.json(file_path)
        return df
    except Exception as e:
        reset_spark_session()
        spark = get_spark_session()
        df = spark.read.json(file_path)
        return df

def get_skills(character):
    """Lấy danh sách các kỹ năng từ file JSON"""
    wait_for_cache() 
    skills = []
    for item in _global_cache['skill_counts_worldwide']:
        if character.lower() in item['skill'].lower():
            skills.append(item['skill'])
    return skills

def analyze_skill_by_country(country=None, limit=None):
    """Phân tích kỹ năng theo quốc gia"""
    wait_for_cache()
    skill_counts = []
    if limit:
        try:
            limit = int(limit)
            skill_counts = [item for item in _global_cache['skill_counts_by_country'] if item['search_country'].lower() == country.lower()]
            skill_counts = sorted(skill_counts, key=lambda x: x['count'], reverse=True)[:limit]
        except (ValueError, TypeError):
            skill_counts = [item for item in _global_cache['skill_counts_by_country'] if item['search_country'].lower() == country.lower()]
            skill_counts = sorted(skill_counts, key=lambda x: x['count'], reverse=True)[:10]
    else:
        skill_counts = [item for item in _global_cache['skill_counts_by_country'] if item['search_country'].lower() == country.lower()]
        skill_counts = sorted(skill_counts, key=lambda x: x['count'], reverse=True)

    return skill_counts

def get_job_by_country(country=None, limit=None, page=1):
    """Lấy số lượng job theo quốc gia"""
    wait_for_cache()
    
    if not country:
        return []
    
    # Tìm dữ liệu cho country
    for item in _global_cache['job_counts_by_country']:
        if item['search_country'].lower() == country.lower():
            jobs_data = item['data']
            processed_jobs = []
            for job in jobs_data:
                if hasattr(job, 'asDict'):
                    job_dict = job.asDict()
                    processed_jobs.append({
                        "job_title": job_dict.get('job_title', ''),
                        "count": job_dict.get('count', 0)
                    })
                elif isinstance(job, dict):
                    processed_jobs.append({
                        "job_title": job.get('job_title', ''),
                        "count": job.get('count', 0)
                    })
                elif isinstance(job, (list, tuple)) and len(job) >= 2:
                    processed_jobs.append({
                        "job_title": str(job[1]) if len(job) > 1 else '',
                        "count": int(job[0]) if len(job) > 0 else 0
                    })
            
            # Sắp xếp theo count giảm dần
            processed_jobs = sorted(processed_jobs, key=lambda x: x['count'], reverse=True)
            
            # Áp dụng pagination
            if limit:
                try:
                    limit = int(limit)
                    page = int(page)
                    start_index = (page - 1) * limit
                    end_index = start_index + limit
                    processed_jobs = processed_jobs[start_index:end_index]
                except (ValueError, TypeError):
                    processed_jobs = processed_jobs[:10] 
            
            return [{
                "search_country": item['search_country'],
                "data": processed_jobs,
                "total_jobs": len(item['data']),
                "showing": len(processed_jobs)
            }]
    
    return []

def analyze_jobs_trends(type, value=None, limit=15, page=1):
    """Phân tích xu hướng việc làm theo quý hoặc năm"""
    wait_for_cache()
    job_counts_by_month = _global_cache['job_counts_by_month']
    
    if not job_counts_by_month:
        return []
    
    filtered_data = [
        item for item in job_counts_by_month 
        if item['month_year'].split("-")[0] == str(value)
    ]
    return filtered_data

def read_file_skill_counts_worldwide(limit=None):
    """Lấy thống kê kỹ năng toàn cầu"""
    wait_for_cache()
    skill_count_worldwide = _global_cache['skill_counts_worldwide']
    if limit:
        try:
            limit = int(limit)
            skill_count_worldwide = skill_count_worldwide[:limit]
        except (ValueError, TypeError):
            skill_count_worldwide = skill_count_worldwide[:10]
    
    return skill_count_worldwide

# Context manager để tự động cleanup
class JobAnalyzer:
    """Context manager cho Job Analysis"""

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
    
    def analyze_skills(self, job_title, country, limit):
        return analyze_job_skills(job_title, country, limit)

    def filter_and_aggregate_jobs(self, search_country, input_skills):
        return filter_and_aggregate_jobs(search_country, input_skills)

    def analyze_job_by_country(self, country, **kwargs):
        return get_job_by_country(country, **kwargs)
    
    def get_countries(self):
        return get_countries()
    
    def get_skills(self, character):
        return get_skills(character)
    
    def analyze_skill_by_country(self, country=None, limit=None):
        return analyze_skill_by_country(country, limit)
    
    def analyze_jobs_trends(self, type, value):
        return analyze_jobs_trends(type, value)

    def analyze_global_skills(self, limit=None):
        return read_file_skill_counts_worldwide(limit)

    def initialize_global_cache(self):
        return initialize_global_cache()

