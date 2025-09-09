from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse
from pathlib import Path
from datetime import datetime
import shutil

from recon_logic import run_recon_pipeline

BASE = Path(__file__).parent.resolve()
UPLOADS = BASE / "uploads"
OUTPUTS = BASE / "outputs"
UPLOADS.mkdir(exist_ok=True)
OUTPUTS.mkdir(exist_ok=True)

app = FastAPI(title="Recon Backend", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
       "https://recondb.vercel.app"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/files", StaticFiles(directory=str(OUTPUTS)), name="files")

@app.get("/health")
def health():
    return {"status": "ok"}

def _save_upload(file: UploadFile, prefix: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = f"{ts}__{prefix}__{file.filename or 'file'}"
    dest = UPLOADS / safe_name
    with dest.open("wb") as f:
        shutil.copyfileobj(file.file, f)
    return dest

@app.post("/reconcile/pdf-recon")
async def pdf_recon(
    pdf: UploadFile = File(...),
    bank1: UploadFile = File(...),
    mis: UploadFile = File(...),
    outstanding: UploadFile = File(...),
):
    try:
        pdf_path = _save_upload(pdf, "pdf")
        bank1_path = _save_upload(bank1, "bank1")
        mis_path = _save_upload(mis, "mis")
        outstanding_path = _save_upload(outstanding, "outstanding")

        consolidated_output_path = str(OUTPUTS / "joined_output.xlsx")
        updated_outstanding_path = str(OUTPUTS / "outstanding_report_UPDATED.xlsx")

        result = run_recon_pipeline(
            pdf_path=str(pdf_path),
            bank_file_paths=[str(bank1_path)],
            mis_file_path=str(mis_path),
            outstanding_file_path=str(outstanding_path),
            consolidated_output_path=consolidated_output_path,
            updated_outstanding_path=updated_outstanding_path,
        )

        base_url = "http://localhost:8000"
        return JSONResponse(
            {
                "ok": True,
                "summary": {"matches_found": result["matches_found"]},
                "artifacts": {
                    "consolidated_output": f"{base_url}/files/joined_output.xlsx"
                    if result["consolidated_written"] else None,
                    "updated_outstanding": f"{base_url}/files/outstanding_report_UPDATED.xlsx"
                    if result["updated_outstanding_written"] else None,
                },
            }
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
