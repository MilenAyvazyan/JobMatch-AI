# JobMatch AI — Real-Time Job Recommendation System

> A full-stack big data application that recommends jobs in real time using Apache Kafka, Apache Spark, MongoDB, and Machine Learning.

---

## Overview

JobMatch AI is a web application where users register with their skills, browse job listings, and receive **AI-powered personalized recommendations** — updated in real time after every click.

The entire pipeline runs end-to-end:

```
User Click → Kafka → Spark Streaming → ML Engine → MongoDB → Web UI
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Web Framework | Python Flask |
| Message Broker | Apache Kafka |
| Stream Processing | Apache Spark (PySpark) |
| Database | MongoDB Atlas |
| ML Engine | Scikit-learn (TF-IDF + Cosine Similarity) |
| Frontend | HTML5, CSS3, JavaScript |

---

## ML Algorithm

Recommendations are generated using a **dual-signal scoring model**:

```
Final Score = (Skills Match × 0.6) + (Click Similarity × 0.4)
```

- **Skills Match** — TF-IDF vectors compare user skills with job requirements
- **Click Similarity** — the last clicked job adds a behavioral signal
- Top 3 jobs ranked by final score are saved to MongoDB and displayed instantly

---

## Project Structure

```
JobMatch-AI/
├── app.py                 # Flask web application + Kafka producer
├── spark_processor.py     # PySpark Streaming + ML recommendation engine
├── seed_data.py           # Populates MongoDB with job listings
├── templates/
│   ├── login.html
│   ├── register.html
│   ├── index.html         # Job listings
│   ├── job_detail.html
│   └── recommendations.html
└── .gitignore
```

---

## How to Run

### Prerequisites
- Python 3.10+
- Java 17+
- Apache Kafka (in `C:\kafka`)
- MongoDB Atlas account

### Install dependencies
```bash
pip install flask pymongo kafka-python pyspark==3.5.1 certifi scikit-learn
```

### 1. Start Kafka
```bash
cd C:\kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
```

### 2. Start Spark Processor
```bash
python spark_processor.py
```

### 3. Start Web App
```bash
python app.py
```

### 4. Open browser
```
http://127.0.0.1:5000
```

---

## 👥 Authors

**Milena Ayvazyan & Sona Tigranyan**
