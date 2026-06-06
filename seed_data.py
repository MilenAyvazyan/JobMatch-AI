import certifi
from pymongo import MongoClient
import os
from dotenv import load_dotenv
load_dotenv()

uri = os.getenv("MONGO_URI")
client = MongoClient(uri, tlsCAFile=certifi.where())
db = client["Job_Recommendation_DB"]

db["jobs"].drop()

jobs = [
    {
        "job_id": 1,
        "title": "Python Developer",
        "company": "SoftConstruct",
        "skills": "Python Django Flask REST API"
    },
    {
        "job_id": 2,
        "title": "Machine Learning Engineer",
        "company": "SmartClick",
        "skills": "Python Machine Learning TensorFlow Deep Learning Neural Networks"
    },
    {
        "job_id": 3,
        "title": "Data Analyst",
        "company": "Picsart",
        "skills": "Python Data Analysis SQL Statistics Visualization"
    },
    {
        "job_id": 4,
        "title": "Backend Engineer",
        "company": "ServiceTitan",
        "skills": "Java Spring SQL Microservices REST API"
    },
    {
        "job_id": 5,
        "title": "AI Research Scientist",
        "company": "YerevaNN",
        "skills": "Python Machine Learning Deep Learning NLP Research TensorFlow"
    }
]

db["jobs"].insert_many(jobs)
print("Job-երը թարմացվեցին skills-երով։")
for job in jobs:
    print(f"- {job['title']} → {job['skills']}")