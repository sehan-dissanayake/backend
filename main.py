# main.py
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Path, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime
from collections import Counter
from bson.decimal128 import Decimal128
from bson import json_util
from dateutil import parser
from dateutil.relativedelta import relativedelta
from typing import List, Dict
import asyncio
import json
from motor.motor_asyncio import AsyncIOMotorClient
import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME")
COLLECTION_NAME = os.getenv("COLLECTION_NAME")
JOB_COLLECTION_NAME = os.getenv("JOB_COLLECTION_NAME")

app = FastAPI()


# CORS setup
origins = ["http://localhost:5173"]  # Update with your frontend URL in production

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

'''@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.append(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            await collection.insert_one({"message": data})
            for client in connected_clients:
                await client.send_text(f"New: {data}")
    except WebSocketDisconnect:
        connected_clients.remove(websocket)'''

client = AsyncIOMotorClient(MONGO_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]
job_collection = db["jobs"]

# WebSocket clients (optional, uncomment to use)
connected_clients = []

class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []
        self.job_subscribers: dict[str, list[WebSocket]] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        return websocket

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            
        # Also remove from job subscribers
        for job_id, subscribers in self.job_subscribers.items():
            if websocket in subscribers:
                subscribers.remove(websocket)

    async def send_personal_message(self, message: str, websocket: WebSocket):
        try:
            await websocket.send_text(message)
        except Exception as e:
            print(f"Error sending message: {e}")
            self.disconnect(websocket)

    async def broadcast(self, message: str):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                disconnected.append(connection)
        
        # Clean up disconnected clients
        for websocket in disconnected:
            self.disconnect(websocket)

    async def broadcast_job_update(self, job_id: str, message: str):
        """Send update only to subscribers of a specific job"""
        if job_id not in self.job_subscribers:
            return
            
        disconnected = []
        for connection in self.job_subscribers[job_id]:
            try:
                await connection.send_text(message)
            except Exception:
                disconnected.append(connection)
        
        # Clean up disconnected subscribers
        for websocket in disconnected:
            if websocket in self.job_subscribers[job_id]:
                self.job_subscribers[job_id].remove(websocket)

    def subscribe_to_job(self, job_id: str, websocket: WebSocket):
        """Subscribe a websocket to a specific job's updates"""
        if job_id not in self.job_subscribers:
            self.job_subscribers[job_id] = []
        
        if websocket not in self.job_subscribers[job_id]:
            self.job_subscribers[job_id].append(websocket)


# Initialize connection manager
manager = ConnectionManager()

# Helper function to get job ID by role name (already defined in your code)
async def get_job_id_by_role(role: str):
    job = await job_collection.find_one({"title": role}, {"_id": 1})
    return str(job["_id"]) if job else None


# MongoDB change stream watcher
async def watch_collection_changes(collection_name=COLLECTION_NAME):
    """Watch for changes in the MongoDB collection and broadcast to all connected clients."""
    watch_collection = db[collection_name]
    
    pipeline = [{'$match': {'operationType': {'$in': ['insert', 'update', 'delete']}}}]

    while True:
        try:
            async with watch_collection.watch(pipeline,full_document = "updateLookup") as stream:
                print(f"Watching collection: {collection_name}")
                async for change in stream:
                    # Process the change
                    #print(change)
                    operation = change['operationType']
                    document_key = change.get('documentKey', {})
                    document_id = str(document_key.get('_id')) if document_key else None
                    print(document_id)
                    
                    # For insert and update operations, we can access the full document
                    full_doc = change.get('fullDocument', {})
                    #print(full_doc)
                    job_id = full_doc.get('job_id') if operation != 'delete' else None
                    
                    # Create notification message
                    message = {
                        "type": "db_update",
                        "operation": operation,
                        "collection": collection_name,
                        "document_id": document_id,
                        "job_id": job_id,
                        "timestamp": datetime.now().isoformat()
                    }
                    
                    serialized_msg = json.dumps(message, default=json_util.default)
                    
                    # Broadcast to all connected clients
                    await manager.broadcast(serialized_msg)
                    
                    # If there's a job_id, also broadcast to job-specific subscribers
                    if job_id:
                        await manager.broadcast_job_update(job_id, serialized_msg)
                    
                    print(f"Change detected: {operation} in {collection_name} for job_id: {job_id}")
        except Exception as e:
            print(f"Error in change stream: {e}")
            await asyncio.sleep(5)  # Wait before reconnecting

# Start up the change stream watcher
@app.on_event("startup")
async def startup_event():
    # Start watching the resume collection in the background
    asyncio.create_task(watch_collection_changes(COLLECTION_NAME))
    # You could add more watchers for other collections
    asyncio.create_task(watch_collection_changes("jobs"))
    print("Started MongoDB change stream watchers")

# General WebSocket endpoint
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    websocket = await manager.connect(websocket)
    
    try:
        # Send welcome message
        await manager.send_personal_message(
            json.dumps({"status": "connected", "message": "Connected to CV Analytics WebSocket"}),
            websocket
        )
        
        # Listen for messages
        while True:
            data = await websocket.receive_text()
            try:
                message = json.loads(data)
                # Process incoming messages
                if "subscribe" in message and message["subscribe"]:
                    job_role = message["subscribe"]
                    job_id = await get_job_id_by_role(job_role)
                    if job_id:
                        manager.subscribe_to_job(job_id, websocket)
                        await manager.send_personal_message(
                            json.dumps({
                                "type": "subscription",
                                "status": "success", 
                                "job_role": job_role,
                                "job_id": job_id
                            }),
                            websocket
                        )
                    else:
                        await manager.send_personal_message(
                            json.dumps({
                                "type": "subscription",
                                "status": "error", 
                                "message": f"Job role '{job_role}' not found"
                            }),
                            websocket
                        )
            except json.JSONDecodeError:
                await manager.send_personal_message(
                    json.dumps({"status": "error", "message": "Invalid JSON format"}),
                    websocket
                )
                
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print("Client disconnected")

# Job-specific WebSocket endpoint
@app.websocket("/ws/{job_role}")
async def job_specific_websocket(websocket: WebSocket, job_role: str):
    websocket = await manager.connect(websocket)
    
    try:
        job_id = await get_job_id_by_role(job_role)
        if not job_id:
            await manager.send_personal_message(
                json.dumps({"status": "error", "message": f"Job role '{job_role}' not found"}),
                websocket
            )
            return
        
        # Subscribe this websocket to the job's updates
        manager.subscribe_to_job(job_id, websocket)
        
        # Send confirmation
        await manager.send_personal_message(
            json.dumps({
                "status": "connected", 
                "job_role": job_role, 
                "job_id": job_id
            }),
            websocket
        )
        
        # Listen for messages
        while True:
            data = await websocket.receive_text()
            # We could process messages here if needed
            await manager.send_personal_message(f"Received: {data}", websocket)
            
    except WebSocketDisconnect:
        manager.disconnect(websocket)
        print(f"Client for job {job_role} disconnected")

@app.get("/candidates/{job_role}")
async def get_candidates_by_job_role(job_role: str = Path(..., description="Job role name")):
    job_id = await get_job_id_by_role(job_role)
    if not job_id:
        return {"error": f"Job role '{job_role}' not found"}

    result = []

    async for c in collection.find({"job_id": job_id}):
        result.append({
            "name": c.get("name"),
            "email": c.get("email"),
            "ranking_score": c.get("ranking_score"),
            "is_verified": c.get("is_verified"),
            "phone": c.get("phone")
        })

    if not result:
        raise HTTPException(status_code=404, detail="No candidates found for this job")

    return result


@app.get("/stats/summary/{job_role}")
async def get_cv_summary(job_role: str = Path(..., description="Job role name")):
    job_id = await get_job_id_by_role(job_role)
    if not job_id:
        return {"error": f"Job role '{job_role}' not found"}

    query = {"job_id": job_id}

    total_submitted = await collection.count_documents(query)
    total_processed = await collection.count_documents({
    **query,
    "status": {"$in": ["passed", "rejected", "completed"]},
    "is_verified": True
    })
    total_rejected = await collection.count_documents({
    "$or": [
        {**query, "status": "rejected"},
        {**query, "is_verified": False}
    ]
    })
    total_passed = await collection.count_documents({
    "$and": [
        {**query, "status": "passed"},
        {**query, "is_verified": True}
    ]
    })



    pipeline = [
        {"$match": query},
        {"$group": {"_id": None, "average_score": {"$avg": "$ranking_score"}}}
    ]
    cursor = collection.aggregate(pipeline)
    result = [doc async for doc in cursor]
    average_score = result[0]["average_score"] if result else 0
    return {
        "job_role": job_role,
        "submitted": total_submitted,
        "processed": total_processed,
        "avg_score": average_score,
        "rejected": total_rejected,
        "passed": total_passed
    }


@app.get("/stats/score-distribution/{job_role}")
async def get_score_distribution(job_role: str = Path(..., description="Job role name")):
    job_id = await get_job_id_by_role(job_role)
    if not job_id:
        return {"error": f"Job role '{job_role}' not found"}

    bins = {"0-20": 0, "21-40": 0, "41-60": 0, "61-80": 0, "81-100": 0}

    query = {"job_id": job_id, "status": "passed", "is_verified": True}
    async for doc in collection.find(query, {"ranking_score": 1}):
        score = doc.get("ranking_score")
        if score is None:
            score = 0
        elif isinstance(score, Decimal128):
            score = float(score.to_decimal())

        if score <= 20:
            bins["0-20"] += 1
        elif score <= 40:
            bins["21-40"] += 1
        elif score <= 60:
            bins["41-60"] += 1
        elif score <= 80:
            bins["61-80"] += 1
        else:
            bins["81-100"] += 1

    return bins



@app.get("/stats/experience-distribution/{job_role}")
async def get_experience_distribution(job_role: str = Path(..., description="Job role name")):
    job_id = await get_job_id_by_role(job_role)
    if not job_id:
        return {"error": f"Job role '{job_role}' not found"}

    buckets = {"0-1": 0, "2-4": 0, "5-8": 0, "9+": 0}

    query = {"job_id": job_id, "status": "passed", "is_verified": True}
    async for doc in collection.find(query, {"work_experience": 1}):
        total_months = calculate_total_experience_months(doc.get("work_experience", []))

        if total_months < 12:
            buckets["0-1"] += 1
        elif total_months <= 48:
            buckets["2-4"] += 1
        elif total_months <= 96:
            buckets["5-8"] += 1
        else:
            buckets["9+"] += 1

    return buckets



def calculate_total_experience_months(work_experience: list) -> int:
    total_months = 0
    now = datetime.now()

    for exp in work_experience:
        dates = exp.get("dates", "")
        if not dates:
            continue

        # Normalize dash
        dates = dates.replace("â€“", "-")

        try:
            parts = [p.strip() for p in dates.split("-")]
            if len(parts) != 2:
                continue

            start_date = parser.parse(parts[0])
            end_str = parts[1].lower()

            if "present" in end_str:
                end_date = now
            else:
                end_date = parser.parse(parts[1])

            # Use relativedelta for accurate diff
            delta = relativedelta(end_date, start_date)
            months = delta.years * 12 + delta.months
            if months > 0:
                total_months += months
        except Exception:
            continue

    return total_months


@app.get("/stats/degree-distribution/{job_role}")
async def get_degree_distribution(job_role: str = Path(..., description="Job role name")):
    job_id = await get_job_id_by_role(job_role)
    if not job_id:
        return {"error": f"Job role '{job_role}' not found"}

    counters = Counter()
    query = {"job_id": job_id, "status": "passed", "is_verified": True}

    async for doc in collection.find(query, {"education": 1}):
        for edu in doc.get("education", []):
            deg = edu.get("degree", "").lower()
            if "bachelor" in deg or "bsc" in deg:
                counters["Bachelors"] += 1
            elif "master" in deg or "msc" in deg:
                counters["Masters"] += 1
            elif "diploma" in deg or "dip" in deg:
                counters["Diploma"] += 1
            elif "phd" in deg:
                counters["PhD"] += 1
            else:
                counters["Other"] += 1

    return counters



@app.get("/jobs/titles")
async def get_job_titles():
    # Add await to this async operation
    titles = await job_collection.distinct("title")
    return {"titles": titles}


@app.get("/stats/skill-distribution/{job_role}")
async def get_skill_distribution(job_role: str = Path(..., description="Job role name")):
    job_id = await get_job_id_by_role(job_role)
    if not job_id:
        return {"error": f"Job role '{job_role}' not found"}

    skill_counts = Counter()
    query = {"job_id": job_id, "status": "passed", "is_verified": True}

    async for doc in collection.find(query, {"skills": 1}):
        for skill in doc.get("skills", []):
            skill_normalized = skill.strip().lower().capitalize()
            skill_counts[skill_normalized] += 1

    return dict(skill_counts.most_common())

 

@app.get("/candidates/by-job/{job_role}")
async def get_candidates_by_job(job_role: str = Path(..., description="Job role name")):
    job_id = await get_job_id_by_role(job_role)
    result = []

    query = {"job_id": job_id, "status": "passed", "is_verified": True}

    async for c in collection.find(query):
        result.append({
            "id": str(c["_id"]),
            "name": c.get("name"),
            "email": c.get("email"),
            "phone": c.get("phone"),
            "skills": c.get("skills", []),
            "education": c.get("education", []),
            "work_experience": c.get("work_experience", []),
            "upload_date": c.get("upload_date")
        })

    if not result:
        raise HTTPException(status_code=404, detail="No candidates found for this job")

    return result


@app.get("/candidates/score-buckets/{job_role}")
async def get_candidates_by_score_buckets(job_role: str = Path(..., description="Job role name")):
    job_id = await get_job_id_by_role(job_role)
    if not job_id:
        return {"error": f"Job role '{job_role}' not found"}

    score_ranges = {
        "0-20": (0, 20),
        "21-40": (21, 40),
        "41-60": (41, 60),
        "61-80": (61, 80),
        "81-100": (81, 100),
    }
    
    grouped_candidates: Dict[str, List[dict]] = {key: [] for key in score_ranges}

    query = {
        "job_id": job_id,
        "status": "passed",
        "is_verified": True,
        "ranking_score": {"$gte": 0}
    }

    async for doc in collection.find(query):
        score = doc.get("ranking_score", 0)
        name = doc.get("name", "Unknown")
        email = doc.get("email", "N/A")
        phone = doc.get("phone", "N/A")
        education_list = doc.get("education", [])
        experience_list = doc.get("work_experience", [])
        cv_filename = doc.get("original_filename", "")

        education = (
            education_list[-1]["degree"]
            if education_list and "degree" in education_list[-1]
            else "N/A"
        )

        experience = f"{len(experience_list)} job(s)" if experience_list else "0 jobs"
        cv_link = f"/static/cvs/{cv_filename}"

        candidate = {
            "name": name,
            "email": email,
            "phone": phone,
            "education": education,
            "experience": experience,
            "cvLink": cv_link,
            "score": score,
        }

        for label, (low, high) in score_ranges.items():
            if low <= score <= high:
                grouped_candidates[label].append(candidate)
                break

    for label in grouped_candidates:
        grouped_candidates[label].sort(key=lambda x: x["score"], reverse=True)

    return grouped_candidates