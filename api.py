from dataclasses import field
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Optional, Dict
from contextlib import asynccontextmanager

from metric_calculator import prepare_metrics_for_analyzer
from metrics_analyzer import safe_analyze

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("API запущен.")
    yield
    print("API завершает работу...")

app = FastAPI(
    title="Team KPI Predictor",
    description="API для расчета KPI и анализа через LLM",
    version="5.0.0",
    lifespan=lifespan
)

# === Модели ===
class Author(BaseModel):
    name: str
    email: str

class Committer(BaseModel):
    name: str
    email: str

class Commit(BaseModel):
    hash: Optional[str] = None
    message: str
    author: Author
    committer: Optional[Committer] = None
    createdAt: str
    parents: List[str] = field(default_factory=list)
    branches: Optional[List[str]] = None
    branch_names: Optional[List[str]] = None

class Repo(BaseModel):
    name: Optional[str] = None
    commits: List[Commit] = field(default_factory=list)

class Project(BaseModel):
    key: Optional[str] = None
    name: Optional[str] = None
    description: Optional[str] = None

class Repository(BaseModel):
    name: Optional[str] = None
    createdAt: Optional[str] = None

class BackendResponse(BaseModel):
    success: bool
    message: str
    project: Project
    repository: Repository
    commits: List[Commit] = field(default_factory=list)


# === Эндпоинты ===
@app.post("/predict_kpi")
def predict_kpi(backend_data: BackendResponse):
    """1️⃣ Получает данные проекта и возвращает рассчитанные KPI"""
    try:
        print("🚀 Расчёт KPI...")
        data_dict = backend_data.model_dump()
        result = prepare_metrics_for_analyzer(data_dict)
        return {
            "success": True,
            "message": "KPI рассчитаны успешно",
            "data": result
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка при расчете KPI: {e}")

@app.post("/analyze_kpi")
def analyze_kpi(kpi_data: Dict):
    """2️⃣ Принимает JSON из /predict_kpi и возвращает LLM-анализ"""
    try:
        print("🤖 Анализ KPI...")
        result_text = safe_analyze(kpi_data)
        return {
            "success": True,
            "analysis": result_text
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ошибка анализа KPI: {e}")

@app.get("/")
def root():
    return {
        "message": "Team KPI Predictor API",
        "version": "5.0.0",
        "endpoints": ["/predict_kpi", "/analyze_kpi"]
    }

@app.get("/health")
def health_check():
    return {
        "status": "healthy",
        "version": "5.0.0",
        "endpoints": ["/predict_kpi", "/analyze_kpi"]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
