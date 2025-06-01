from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from datetime import datetime, date
import parser
import polars as pl # type:ignore
from apscheduler.schedulers.background import BackgroundScheduler
import crawler
from datetime import timedelta
import threading

app = FastAPI()
app.mount("/static", StaticFiles(directory="static"), name="static")

# Global variable for the dataframe
df = None
df_lock = threading.Lock()

def load_latest_df():
    global df
    today_str = datetime.now().strftime("%Y-%m-%d")
    file_path = f"output/grouped_{today_str}.ndjson"
    if Path(file_path).exists():
        with df_lock:
            df = pl.scan_ndjson(file_path)
        print(f"Loaded dataframe for {today_str}")
    else:
        print(f"File {file_path} does not exist yet.")
        # Only crawl if it's after 9 AM
        now = datetime.now()
        if now.hour >= 9:
            crawler.collectioncrawl(date.today())
            # then load
            with df_lock:
                df = pl.scan_ndjson(file_path)
        else:
            print("It's not after 9 AM yet. Skipping crawl and load.")
            # Try to load yesterday's data if today's is not available
            yesterday_str = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
            yesterday_file_path = f"output/grouped_{yesterday_str}.ndjson"
            if Path(yesterday_file_path).exists():
                with df_lock:
                    df = pl.scan_ndjson(yesterday_file_path)
                print(f"Loaded dataframe for {yesterday_str}")
            else:
                print(f"File {yesterday_file_path} does not exist either. Dataframe not loaded.")

# Daily job to crawl and load the latest dataframe


def daily_job():
    today = date.today()
    crawler.collectioncrawl(today)
    load_latest_df()

# Initial load at startup
load_latest_df()

# Schedule the job every day at 9 AM
scheduler = BackgroundScheduler()
scheduler.add_job(daily_job, 'cron', hour=9, minute=0)
scheduler.start()

@app.on_event("shutdown")
def shutdown_event():
    scheduler.shutdown()

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    # Check if there are query parameters
    if request.query_params:
        # Serve search.html if any query params
        html_path = Path("static/search.html")
    else:
        # Otherwise serve index.html
        html_path = Path("static/index.html")

    if not html_path.exists():
        return HTMLResponse(content=f"<h1>{html_path.name} not found</h1>", status_code=404)

    content = html_path.read_text(encoding="utf-8")
    return HTMLResponse(content=content)

@app.get("/about")
async def about(request: Request):
    return HTMLResponse(content=Path("static/about.html").read_text(encoding="utf-8"))

import asyncio
@app.get("/search")
async def search(request: Request, query: str = "", type: str = Query("naziv")):
    if not query:
        return JSONResponse(content={"error": "No query provided"}, status_code=400)

    filtered_parts = [part for part in query.split() if len(part) >= 3]
    if not filtered_parts:
        return JSONResponse(content={"error": "Query too short"}, status_code=400)
    filters = {
        type: {"contains": filtered_parts}
    }

    loop = asyncio.get_event_loop()
    with df_lock:
        results = await loop.run_in_executor(None, parser.find, filters, df)

    return JSONResponse(content=results)


@app.exception_handler(404)
async def not_found(request, exc):
    content = Path("static/404.html").read_text(encoding="utf-8")
    return HTMLResponse(content=content, status_code=404)