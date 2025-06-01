from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from datetime import datetime
import parser

import polars as pl #type:ignore
df = pl.scan_ndjson("output/grouped_2025-05-30.ndjson")
app = FastAPI()
# Serve static files (html, css, js) from 'static' folder
app.mount("/static", StaticFiles(directory="static"), name="static")

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
    results = await loop.run_in_executor(None, parser.find, filters, df)

    return JSONResponse(content=results)


@app.exception_handler(404)
async def not_found(request, exc):
    content = Path("static/404.html").read_text(encoding="utf-8")
    return HTMLResponse(content=content, status_code=404)