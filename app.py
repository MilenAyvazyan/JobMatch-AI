import certifi
import json
from datetime import datetime, timezone 
from flask import Flask, render_template, request, redirect, url_for, session, jsonify
from pymongo import MongoClient
from kafka import KafkaProducer
import os
from dotenv import load_dotenv
load_dotenv()

app = Flask(__name__)
app.secret_key = "super_secret_key"

uri = os.getenv("MONGO_URI")
client = MongoClient(uri, tlsCAFile=certifi.where())
db = client["Job_Recommendation_DB"]

try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    print("Kafka Producer-ը հաջողությամբ միացավ:")
except Exception as e:
    print(f"Kafka-ին միանալու սխալ: {e}")
    producer = None

@app.route('/')
def index():
    if "username" not in session:
        return redirect(url_for("login"))
    
    jobs = list(db["jobs"].find({}, {"_id": 0}))
    return render_template("index.html", username=session["username"], jobs=jobs)

@app.route('/view_job/<int:job_id>')
def view_job(job_id):
    if "username" not in session:
        return redirect(url_for("login"))
    
    username = session["username"]
    job = db["jobs"].find_one({"job_id": job_id}, {"_id": 0})
    
    if job:
        click_event = {
            "username": username,
            "job_id": job["job_id"],
            "title": job["title"],
            "timestamp": datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        }
        
        if producer:
            producer.send('job-clicks', value=click_event)
            producer.flush()
            print(f" [Kafka] Ուղարկվեց իրադարձություն: {click_event}")
            
        return render_template("job_detail.html", job=job, username=username)
        
    return "Աշխատանքը չի գտնվել:", 404

@app.route('/register', methods=['GET', 'POST'])
def register():
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password'].strip()
        skills = [s.strip() for s in request.form['skills'].split(',') if s.strip()]

        existing_user = db["users"].find_one({"username": username})
        if existing_user:
            return render_template("register.html", error="Այս օգտատերը արդեն գրանցված է:")
        
        db["users"].insert_one({
            "username": username,
            "password": password,
            "skills": skills
        })
        session["username"] = username
        return redirect(url_for("index"))
        
    return render_template("register.html")

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        username = request.form['username'].strip()
        password = request.form['password'].strip()

        user = db["users"].find_one({"username": username, "password": password})
        if user:
            session["username"] = username
            return redirect(url_for("index"))
        else:
            return render_template("login.html", error="Սխալ օգտանուն կամ գաղտնաբառ:")

    return render_template("login.html")

@app.route('/recommendations')
def recommendations():
    if "username" not in session:
        return redirect(url_for("login"))
    
    username = session["username"]
    rec = db["recommendations"].find_one({"username": username})
    
    recommended_jobs = rec["recommended_jobs"] if rec else []
    based_on = rec["based_on"] if rec else None
    
    return render_template("recommendations.html", 
                           username=username,
                           recommended_jobs=recommended_jobs,
                           based_on=based_on)

@app.route('/get_recommendations_json')
def get_recommendations_json():
    if "username" not in session:
        return jsonify({"error": "Unauthorized"}), 401
    
    rec = db["recommendations"].find_one({"username": session["username"]})
    if rec:
        return jsonify({
            "recommended_jobs": rec["recommended_jobs"],
            "based_on": rec["based_on"]
        })
    return jsonify({"recommended_jobs": []})

@app.route('/logout')
def logout():
    session.pop("username", None)
    return redirect(url_for("login"))

if __name__ == "__main__":
    app.run(debug=True, port=5000)