import os
import threading
import subprocess
import psycopg2
from flask import Flask, request, jsonify
from psycopg2 import extras
from datetime import datetime

# Initialize the Flask application
app = Flask(__name__)

# Your database connection string
DB_CONN_STRING = os.environ.get("DATABASE_URL")

# Your secret token for authentication, from Render environment variables
AUTH_TOKEN = os.environ.get('CRON_SECRET_TOKEN', '30xIx3CREMZIMwJGPV26cgRigBhqSUErd3ISJ4yt9Y5PPK7FsUqvfDWfuPJV7tPD')

def get_db_connection():
    """Establishes and returns a database connection."""
    return psycopg2.connect(DB_CONN_STRING)

def run_scraper_in_background(job_id):
    """
    Starts the scraper script in a separate thread, passing the job_id.
    This prevents the API request from timing out.
    """
    try:
        print(f"Starting scraper script for job_id: {job_id}")
        # Pass the job ID as a command-line argument to your scraper script
        subprocess.run(
            ["python", "12go.py", str(job_id)],
            check=True
        )
        print(f"Scraper script finished for job_id: {job_id}")
    except subprocess.CalledProcessError as e:
        print(f"Error running scraper for job_id {job_id}: {e}")
        # In a real app, you would update the database here to set the status to FAILED.
        # For simplicity, we are handling this within the scraper itself.
    except Exception as e:
        print(f"An unexpected error occurred for job_id {job_id}: {e}")

@app.route('/run-scraper', methods=['POST'])
def run_scraper_api():
    """
    The main endpoint to trigger a new scraper job.
    It creates a job entry in the database and returns the job ID.
    """
    # Check for the custom authentication header
    if request.headers.get('X-Auth-Token') != AUTH_TOKEN:
        print("Unauthorized access attempt.")
        return jsonify({"error": "Unauthorized request."}), 401
    
    print("Received authorized request. Creating new job.")
    
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        
        # Insert a new job entry into the database
        cur.execute(
            """
            INSERT INTO scraper_jobs (status, start_time, last_updated)
            VALUES (%s, %s, %s)
            RETURNING id;
            """,
            ('PENDING', datetime.now(), datetime.now())
        )
        job_id = cur.fetchone()[0]
        conn.commit()
        
        print(f"New job created with ID: {job_id}")

        # Start the scraper in a new thread, passing the job ID
        thread = threading.Thread(target=run_scraper_in_background, args=(job_id,))
        thread.start()

        return jsonify({"message": "Scraper script triggered successfully!", "job_id": job_id}), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error during API call: {error}")
        if conn:
            conn.rollback()
        return jsonify({"error": "Failed to create a new job."}), 500
    finally:
        if conn:
            conn.close()

@app.route('/job-status/<int:job_id>', methods=['GET'])
def get_job_status(job_id):
    """
    API endpoint to check the status of a specific job by its ID.
    """
    conn = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(cursor_factory=extras.DictCursor)
        
        cur.execute("SELECT * FROM scraper_jobs WHERE id = %s;", (job_id,))
        job = cur.fetchone()

        if job is None:
            return jsonify({"error": "Job not found."}), 404
        
        # Convert the row into a dictionary for a cleaner JSON response
        job_dict = dict(job)
        
        # Handle datetime objects for serialization
        for key in ['start_time', 'end_time', 'last_updated']:
            if job_dict.get(key):
                job_dict[key] = job_dict[key].isoformat()

        return jsonify(job_dict), 200

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Error checking job status: {error}")
        return jsonify({"error": "Failed to retrieve job status."}), 500
    finally:
        if conn:
            conn.close()

if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port)