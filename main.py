from flask import Flask, request, jsonify
from flask_cors import CORS
from threading import Lock
from functions import (
    JobAnalyzer,
    stop_spark_session,
    get_spark_session
)

app = Flask(__name__)
CORS(app)

# Global Spark session và lock để thread-safe
spark_session = None
spark_lock = Lock()

# API Routes
@app.route('/', methods=['GET'])
def home():
    """API homepage"""
    return jsonify({
        "message": "Job Skills Analysis API",
        "version": "1.0",
        "endpoints": {
            "GET /": "API information",
            "GET /health": "Health check",
            "GET /stats": "General job statistics",
            "POST /analyze": "Analyze job skills by title and country",
            "GET /analyze/<job_title>/<country>": "Analyze job skills (GET method)"
        }
    })

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        spark = get_spark_session()
        return jsonify({
            "status": "healthy",
            "spark_version": spark.version,
            "timestamp": str(spark.sql("SELECT current_timestamp()").collect()[0][0])
        })
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "error": str(e)
        }), 500

# Nhập mảng skill, quốc gia => nghề
@app.route('/analyze/job-recommendations', methods=['GET'])
def filter_and_aggregate_jobs():
    """Lọc và tổng hợp jobs qua GET request"""
    try:
        data = request.args
        search_country = data.get('search_country')
        input_skills = data.getlist('input_skills') 

        # input_skills = A, B, C ==> input_skills = ['A', 'B', 'C']
        if isinstance(input_skills, str):
            input_skills = [input_skills] 
        if not search_country or not input_skills:
            return jsonify({
                "status": "error",
                "message": "Thiếu thông tin search_country hoặc input_skills"
            }), 400
        with JobAnalyzer() as analyzer:
            result = analyzer.filter_and_aggregate_jobs(search_country, input_skills)
            return jsonify({
                "status": "success",
                "message": "Lọc và tổng hợp jobs thành công",
                "data": result,
                "length": len(result)
            })

    except Exception as e:
        import traceback
        error_details = traceback.format_exc()
        print(f"Error in filter_and_aggregate_jobs: {error_details}")
        return jsonify({
            "status": "error",
            "message": f"Lỗi xử lý request: {str(e)}",
            "error_type": type(e).__name__,
            "error_details": error_details
        }), 500

# Nhập tên nghề, quốc gia => kỹ năng
@app.route('/analyze/job-skills-by-title-and-country', methods=['GET'])
def analyze_skills_get():
    """Phân tích skills qua GET request"""
    try:
        job_title = request.args.get('job_title')
        country = request.args.get('country')
        limit = request.args.get('limit', 20)

        if not job_title or not country:
            return jsonify({
                "status": "error",
                "message": "Thiếu thông tin job_title hoặc country"
            }), 400

        with JobAnalyzer() as analyzer:
            skills_result = analyzer.analyze_skills(job_title, country, limit=limit)
            if skills_result["status"] == "error":
                return jsonify({
                    "status": "error",
                    "message": skills_result["message"]
                }), 400
            return jsonify({
                "status": "success",
                "message": "Phân tích kỹ năng thành công",
                "data": skills_result
            })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi xử lý request: {str(e)}"
        }), 500

@app.route('/analyze/jobs-trend', methods=['GET'])
def analyze_jobs_trends():
    """Phân tích xu hướng nghề qua GET request"""
    value = request.args.get('value')
    if not value:
        return jsonify({
            "status": "error",
            "message": "Thiếu thông tin value"
        }), 400
    try:
        with JobAnalyzer() as analyzer:
            result = analyzer.analyze_jobs_trends("year", value=value)
            return jsonify({
                "status": "success",
                "message": "Phân tích xu hướng nghề thành công",
                "data": result,
                "length": len(result)
            })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi xử lý request: {str(e)}"
        }), 500

@app.route('/analyze/get-job-by-country', methods=['GET'])
def analyze_job_by_country():
    """Phân tích nghề theo quốc gia qua GET request"""
    try:
        country = request.args.get('search_country')
        limit = request.args.get('limit', 10)
        if not country:
            return jsonify({
                "status": "error",
                "message": "Thiếu thông tin country"
            }), 400

        with JobAnalyzer() as analyzer:
            result = analyzer.analyze_job_by_country(country, limit=limit)
            return jsonify({
                "status": "success",
                "message": "Phân tích nghề theo quốc gia thành công",
                "data": result,
                "length": len(result)
            })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi xử lý request: {str(e)}"
        }), 500

@app.route('/analyze/countries', methods=['GET'])
def get_countries():
    """Lấy danh sách các quốc gia từ file JSON"""
    try:
        with JobAnalyzer() as analyzer:
            result = analyzer.get_countries()
            return jsonify({
                "status": "success",
                "message": "Lấy danh sách quốc gia thành công",
                "data": result
            })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi xử lý request: {str(e)}"
        }), 500

@app.route('/analyze/skills', methods=['GET'])
def get_skills():
    """Lấy danh sách các kỹ năng từ file JSON"""
    character = request.args.get('character', '')
    try:
        with JobAnalyzer() as analyzer:
            result = analyzer.get_skills(character)
            return jsonify({
                "status": "success",
                "message": "Lấy danh sách kỹ năng thành công",
                "data": result
            })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi xử lý request: {str(e)}"
        }), 500

@app.route('/analyze/skill-by-country', methods=['GET'])
def analyze_skill_by_country():
    """Phân tích kỹ năng theo quốc gia qua GET request"""
    try:
        country = request.args.get('search_country')
        limit = request.args.get('limit', 10)
        if not country:
            return jsonify({
                "status": "error",
                "message": "Thiếu thông tin country"
            }), 400

        with JobAnalyzer() as analyzer:
            result = analyzer.analyze_skill_by_country(country, limit=limit)
            return jsonify({
                "status": "success",
                "message": "Phân tích kỹ năng theo quốc gia thành công",
                "data": result,
                "length": len(result)
            })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi xử lý request: {str(e)}"
        }), 500

@app.route('/analyze/global-skills', methods=['GET'])
def analyze_global_skills():
    """Phân tích kỹ năng toàn cầu qua GET request"""
    try:
        limit = request.args.get('limit', 10)
        with JobAnalyzer() as analyzer:
            result = analyzer.analyze_global_skills(limit)
            return jsonify({
                "status": "success",
                "message": "Phân tích kỹ năng toàn cầu thành công",
                "data": result,
                "length": len(result)
            })

    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi xử lý request: {str(e)}"
        }), 500

@app.errorhandler(404)
def not_found(error):
    return jsonify({
        "status": "error",
        "message": "Endpoint không tồn tại",
        "available_endpoints": [
            "GET /",
            "GET /health", 
            "GET /stats",
            "POST /analyze",
            "POST /analyze"
        ]
    }), 404

@app.errorhandler(500)
def internal_error(error):
    return jsonify({
        "status": "error",
        "message": "Lỗi server nội bộ"
    }), 500

if __name__ == "__main__":
    try:
        print(" Đang khởi động Job Skills Analysis API Server...")
        
        with JobAnalyzer() as analyzer:
            analyzer.initialize_global_cache()
        app.run(debug=True, host='0.0.0.0', port=5000)
        
    except KeyboardInterrupt:
        print("\nServer đã dừng")
    finally:
        # Cleanup Spark session
        if spark_session:
            spark_session.stop()
            print("Đã đóng Spark session")