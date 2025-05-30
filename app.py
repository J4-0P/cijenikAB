from fastapi import FastAPI, Request
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

import asyncio
@app.get("/search")
async def search(request: Request, query: str = ""):
    if not query:
        return JSONResponse(content={"error": "No query provided"}, status_code=400)

    if not any(len(part) >= 3 for part in query.split()):
        return JSONResponse(content={"error": "Query too short"}, status_code=400)

    filters = {
        "naziv": {"contains": query.split(" ")}
    }

    loop = asyncio.get_event_loop()
    results = await loop.run_in_executor(None, parser.find, filters, df)

    return JSONResponse(content=results)


@app.exception_handler(404)
async def not_found(request, exc):
    content = Path("static/404.html").read_text(encoding="utf-8")
    return HTMLResponse(content=content, status_code=404)