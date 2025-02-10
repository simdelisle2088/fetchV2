import schedule
import time
import subprocess

print(f"[SCHEDULER] Do not close.")

def job():
    # Replace with your script path
    subprocess.run(["python", "fetchFullInv.py"])

# Schedule job every hours
schedule.every(24).hours.do(job)

while True:
    schedule.run_pending()
    time.sleep(60)  # wait one minute