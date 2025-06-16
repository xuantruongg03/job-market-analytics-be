# Job Explorer Backend API

A comprehensive job market and skills analysis API built with Apache Spark and Flask.

## 📋 Overview

Job Explorer Backend is a RESTful API designed to analyze job market data and professional skills. The system leverages Apache Spark for big data processing and provides insights on:

- Skills analysis by job title and country
- Job market trends over time
- Career recommendations based on skills
- Global labor market statistics

## 🚀 Key Features

### 🔍 Skills Analysis
- **Skills by Job & Country**: Discover required skills for specific positions
- **Global Skills**: Statistics on the most popular skills worldwide
- **Skills by Country**: Analyze skill demand in different countries

### 💼 Career Recommendations
- **Job Suggestions by Skills**: Input your skills to get matching career recommendations
- **Trend Analysis**: Track job market trends by year

### 🌍 Market Statistics
- **Jobs by Country**: Statistics on popular positions in each country
- **Countries & Skills Metadata**: APIs to retrieve reference data

## 🛠️ Technology Stack

- **Backend Framework**: Flask 2.3.3
- **Big Data Processing**: Apache Spark 3.4.1
- **Language**: Python 3.10

## 📦 Installation

### System Requirements
- Python 3.10+
- Java 8+ (for Apache Spark)
- Docker (optional)

### Local Installation

1. **Clone repository**
```bash
git clone <repository-url>
cd job-explorer-be
```

2. **Install dependencies**
```bash
pip install -r requirements.txt
```

3. **Setup environment variables**
Create a `.env` file with configuration:
```env
SKILL_COUNTS = PATH_TO_SKILL_COUNTS_FILE
SKILL_COUNTS_BY_COUNTRY = PATH_TO_SKILL_COUNTS_BY_COUNTRY_FILE
JOB_COUNTS = PATH_TO_JOB_COUNTS_FILE
JOB_COUNTS_BY_COUNTRY = PATH_TO_JOB_COUNTS_BY_COUNTRY_FILE
JOB_COUNTS_BY_MONTH = PATH_TO_JOB_COUNTS_BY_MONTH_FILE
VIETNAMESE_SKILLS = PATH_TO_VIETNAMESE_SKILLS_FILE
RAW_JOB_DATA = PATH_TO_RAW_JOB_DATA_FILE
```

4. **Run application**
```bash
python app.py
```

The API will be available at `http://localhost:5000`
## 📚 API Documentation

### Main Endpoints

#### 🏠 API Information
```http
GET /
```
Returns basic API information and list of available endpoints.

#### 🔍 Health Check
```http
GET /health
```
Checks API and Spark session status.

### 🎯 Skills Analysis

#### Skills by Job Title and Country
```http
GET /analyze/job-skills-by-title-and-country?job_title=<job>&country=<country>&limit=<number>
```

**Parameters:**
- `job_title` (required): Job title
- `country` (required): Country name
- `limit` (optional): Number of results (default: 20)

**Example:**
```bash
curl "http://localhost:5000/analyze/job-skills-by-title-and-country?job_title=Software Engineer&country=Vietnam&limit=10"
```

#### Global Skills
```http
GET /analyze/global-skills?limit=<number>
```

#### Skills by Country
```http
GET /analyze/skill-by-country?search_country=<country>&limit=<number>
```

### 💼 Career Recommendations

#### Find Jobs by Skills
```http
GET /analyze/job-recommendations?search_country=<country>&input_skills=<skill1>&input_skills=<skill2>
```

**Parameters:**
- `search_country` (required): Target country
- `input_skills` (required): List of skills (can be passed multiple times)

**Example:**
```bash
curl "http://localhost:5000/analyze/job-recommendations?search_country=Vietnam&input_skills=Python&input_skills=JavaScript&input_skills=React"
```

### 📈 Trend Analysis

#### Job Trends by Year
```http
GET /analyze/jobs-trend?value=<year>
```

### 🌍 Country Statistics

#### Jobs by Country
```http
GET /analyze/get-job-by-country?search_country=<country>&limit=<number>
```

### 📋 Metadata

#### Countries List
```http
GET /analyze/countries
```

#### Skills List
```http
GET /analyze/skills?character=<starting_letter>
```

## 📊 Data Structure

### Response Format
All API responses follow a standard structure:

```json
{
  "status": "success|error",
  "message": "Result description",
  "data": {}, // Response data
  "length": 0 // Number of items (if applicable)
}
```

### Sample Response - Skills by Job
```json
{
  "status": "success",
  "message": "Skills analysis successful",
  "data": {
    "job_title": "Software Engineer",
    "country": "Vietnam",
    "skills": [
      {
        "skill": "Python",
        "count": 1250,
        "percentage": 15.5
      },
      {
        "skill": "JavaScript",
        "count": 1100,
        "percentage": 13.6
      }
    ]
  }
}
```

### Main Components:

1. **Flask API Server** (`app.py`): REST API endpoints
2. **Job Analyzer** (`functions.py`): Spark-based data processing
3. **Job Formatter** (`format_job.py`): Job title normalization
4. **Global Cache**: Performance optimization

## 🧪 Testing

### Health Check
```bash
curl http://localhost:5000/health
```
