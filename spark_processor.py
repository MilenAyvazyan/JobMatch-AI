import certifi
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pymongo import MongoClient
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import os
from dotenv import load_dotenv
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")

spark = SparkSession.builder \
    .appName("JobRecommendationStreaming") \
    .config("spark.jars",
            "file:///C:/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,"
            "file:///C:/spark/jars/kafka-clients-3.5.2.jar,"
            "file:///C:/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,"
            "file:///C:/spark/jars/commons-pool2-2.11.1.jar") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType([
    StructField("username", StringType()),
    StructField("job_id", IntegerType()),
    StructField("title", StringType()),
    StructField("timestamp", StringType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "job-clicks") \
    .option("startingOffsets", "latest") \
    .load()

parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

def get_ml_recommendations(user_skills, clicked_title, jobs, username):
    """
    ML — TF-IDF + Cosine Similarity
    Skills Match (60%) + Click Similarity (40%)
    """
    job_texts = [job["skills"] for job in jobs]
    job_titles = [job["title"] for job in jobs]

    vectorizer = TfidfVectorizer()
    all_texts = job_texts + [user_skills, clicked_title]
    tfidf_matrix = vectorizer.fit_transform(all_texts)

    job_vectors = tfidf_matrix[:len(jobs)]
    user_vector = tfidf_matrix[len(jobs)]
    click_vector = tfidf_matrix[len(jobs) + 1]

    skills_scores = cosine_similarity(user_vector, job_vectors)[0]

    click_scores = cosine_similarity(click_vector, job_vectors)[0]

    final_scores = (skills_scores * 0.6) + (click_scores * 0.4)

    scored_jobs = []
    for i, job in enumerate(jobs):
        scored_jobs.append({
            "job_id": job["job_id"],
            "title": job["title"],
            "company": job["company"],
            "skills": job["skills"],
            "score": round(float(final_scores[i]), 3)
        })

    scored_jobs.sort(key=lambda x: x["score"], reverse=True)

    print(f"\n[ML] '{username}'-ի recommendation scores՝")
    for j in scored_jobs:
        print(f"     {j['title']}: {j['score']}")

    return scored_jobs[:3]


def process_batch(batch_df, batch_id):
    rows = batch_df.collect()
    if not rows:
        return

    client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
    db = client["Job_Recommendation_DB"]

    for row in rows:
        username = row["username"]
        title = row["title"] or ""

        print(f"\n[Spark] Batch {batch_id} — օգտատեր '{username}' սեղմեց '{title}'")

        db["clicks"].insert_one({
            "username": username,
            "job_id": row["job_id"],
            "title": title,
            "timestamp": row["timestamp"]
        })

        user = db["users"].find_one({"username": username})
        user_skills = " ".join(user.get("skills", [])) if user else ""
        print(f"[ML] '{username}'-ի skills՝ {user_skills}")

        jobs = list(db["jobs"].find({}, {"_id": 0}))

        top_jobs = get_ml_recommendations(user_skills, title, jobs, username)

        db["recommendations"].replace_one(
            {"username": username},
            {
                "username": username,
                "recommended_jobs": top_jobs,
                "based_on": title,
                "user_skills": user_skills
            },
            upsert=True
        )
        print(f"[Spark] '{username}'-ի top 3 recommendations պահվեցին")

    client.close()


query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .outputMode("update") \
    .option("checkpointLocation", "file:///C:/BigDataProject/checkpoint") \
    .start()

print("Spark Streaming սկսեց աշխատել...")
query.awaitTermination()