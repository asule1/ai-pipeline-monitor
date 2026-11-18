import csv
import io
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel, Field
from sqlalchemy import case, func, select
from sqlalchemy.orm import Session

from . import anomaly, rca
from .db import Diagnosis, Pipeline, PipelineRun, get_session, init_db, utcnow

STATIC = Path(__file__).parent / "static"
HISTORY_WINDOW = 20


def as_utc(dt: datetime | None) -> datetime | None:
    if dt is None:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)


# ---------- schemas ----------

class PipelineIn(BaseModel):
    name: str = Field(min_length=1, max_length=120)
    owner: str | None = None
    schedule: str | None = None
    freshness_sla_minutes: int = Field(default=1440, gt=0)


class RunStart(BaseModel):
    pipeline: str
    started_at: datetime | None = None


class RunComplete(BaseModel):
    status: Literal["success", "failed"]
    finished_at: datetime | None = None
    rows_processed: int | None = Field(default=None, ge=0)
    error_message: str | None = None


class DiagnosisOut(BaseModel):
    category: str
    root_cause: str
    recommended_actions: list[str]
    confidence: float
    source: str


class RunOut(BaseModel):
    id: int
    pipeline: str
    status: str
    started_at: datetime
    finished_at: datetime | None
    duration_seconds: float | None
    rows_processed: int | None
    error_message: str | None
    is_anomaly: bool
    anomaly_reasons: list[str]
    diagnosis: DiagnosisOut | None


def run_out(r: PipelineRun) -> RunOut:
    d = r.diagnosis
    return RunOut(
        id=r.id, pipeline=r.pipeline.name, status=r.status,
        started_at=as_utc(r.started_at), finished_at=as_utc(r.finished_at),
        duration_seconds=r.duration_seconds, rows_processed=r.rows_processed,
        error_message=r.error_message, is_anomaly=r.is_anomaly,
        anomaly_reasons=r.anomaly_reasons or [],
        diagnosis=DiagnosisOut(
            category=d.category, root_cause=d.root_cause, recommended_actions=d.recommended_actions,
            confidence=d.confidence, source=d.source,
        ) if d else None,
    )


# ---------- core logic ----------

def recent_successes(session: Session, pipeline_id: int, before_id: int) -> list[PipelineRun]:
    return list(session.scalars(
        select(PipelineRun)
        .where(PipelineRun.pipeline_id == pipeline_id, PipelineRun.status == "success",
               PipelineRun.id != before_id)
        .order_by(PipelineRun.started_at.desc())
        .limit(HISTORY_WINDOW)
    ))


def attach_diagnosis(session: Session, run: PipelineRun) -> None:
    recent = session.scalars(
        select(PipelineRun.status).where(PipelineRun.pipeline_id == run.pipeline_id)
        .order_by(PipelineRun.started_at.desc()).limit(10)
    ).all()
    result = rca.diagnose(rca.RunContext(
        pipeline=run.pipeline.name, status=run.status, error_message=run.error_message,
        duration_seconds=run.duration_seconds, rows_processed=run.rows_processed,
        anomaly_reasons=run.anomaly_reasons or [], recent_statuses=list(recent),
    ))
    if run.diagnosis:
        session.delete(run.diagnosis)
        session.flush()
    run.diagnosis = Diagnosis(**result.to_dict())


def complete_run(session: Session, run: PipelineRun, body: RunComplete) -> PipelineRun:
    finished = as_utc(body.finished_at) or utcnow()
    run.status = body.status
    run.finished_at = finished
    run.duration_seconds = max(0.0, (finished - as_utc(run.started_at)).total_seconds())
    run.rows_processed = body.rows_processed
    run.error_message = body.error_message

    if body.status == "success":
        history = recent_successes(session, run.pipeline_id, run.id)
        reasons = anomaly.detect(
            run.duration_seconds, run.rows_processed,
            [h.duration_seconds for h in history if h.duration_seconds is not None],
            [h.rows_processed for h in history if h.rows_processed is not None],
        )
        run.is_anomaly = bool(reasons)
        run.anomaly_reasons = reasons

    if run.status == "failed" or run.is_anomaly:
        attach_diagnosis(session, run)
    session.commit()
    return run


def freshness(session: Session, now: datetime | None = None) -> list[dict]:
    now = now or utcnow()
    last_success = dict(session.execute(
        select(PipelineRun.pipeline_id, func.max(PipelineRun.finished_at))
        .where(PipelineRun.status == "success").group_by(PipelineRun.pipeline_id)
    ).all())
    out = []
    for p in session.scalars(select(Pipeline).order_by(Pipeline.name)):
        last = as_utc(last_success.get(p.id))
        minutes = (now - last).total_seconds() / 60 if last else None
        out.append({
            "pipeline": p.name,
            "last_success_at": last,
            "minutes_since_success": round(minutes, 1) if minutes is not None else None,
            "sla_minutes": p.freshness_sla_minutes,
            "is_stale": minutes is None or minutes > p.freshness_sla_minutes,
        })
    return out


# ---------- app ----------

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(title="AI Data Pipeline Monitor", version="1.0.0", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
def dashboard():
    return FileResponse(STATIC / "dashboard.html")


@app.post("/api/pipelines", status_code=201)
def create_pipeline(body: PipelineIn, session: Session = Depends(get_session)):
    if session.scalar(select(Pipeline).where(Pipeline.name == body.name)):
        raise HTTPException(409, f"Pipeline '{body.name}' already exists")
    p = Pipeline(**body.model_dump())
    session.add(p)
    session.commit()
    return {"id": p.id, **body.model_dump()}


@app.get("/api/pipelines")
def list_pipelines(session: Session = Depends(get_session)):
    since = utcnow() - timedelta(days=7)
    stats = {
        pid: (total, ok, avg)
        for pid, total, ok, avg in session.execute(
            select(
                PipelineRun.pipeline_id,
                func.count(),
                func.sum(case((PipelineRun.status == "success", 1), else_=0)),
                func.avg(PipelineRun.duration_seconds),
            ).where(PipelineRun.started_at >= since, PipelineRun.status != "running")
            .group_by(PipelineRun.pipeline_id)
        ).all()
    }
    fresh = {f["pipeline"]: f for f in freshness(session)}
    out = []
    for p in session.scalars(select(Pipeline).order_by(Pipeline.name)):
        last = session.scalar(
            select(PipelineRun).where(PipelineRun.pipeline_id == p.id).order_by(PipelineRun.started_at.desc())
        )
        total, ok, avg = stats.get(p.id, (0, 0, None))
        out.append({
            "name": p.name, "owner": p.owner, "schedule": p.schedule,
            "last_status": last.status if last else None,
            "last_run_at": as_utc(last.started_at) if last else None,
            "runs_7d": total,
            "success_rate_7d": round(ok / total, 3) if total else None,
            "avg_duration_7d": round(avg, 1) if avg is not None else None,
            "is_stale": fresh[p.name]["is_stale"],
        })
    return out


@app.post("/api/runs", response_model=RunOut, status_code=201)
def start_run(body: RunStart, session: Session = Depends(get_session)):
    p = session.scalar(select(Pipeline).where(Pipeline.name == body.pipeline))
    if p is None:
        raise HTTPException(404, f"Unknown pipeline '{body.pipeline}'")
    run = PipelineRun(pipeline=p, status="running", started_at=as_utc(body.started_at) or utcnow(),
                      anomaly_reasons=[])
    session.add(run)
    session.commit()
    return run_out(run)


@app.post("/api/runs/{run_id}/complete", response_model=RunOut)
def finish_run(run_id: int, body: RunComplete, session: Session = Depends(get_session)):
    run = session.get(PipelineRun, run_id)
    if run is None:
        raise HTTPException(404, "Run not found")
    if run.status != "running":
        raise HTTPException(409, "Run already completed")
    return run_out(complete_run(session, run, body))


@app.get("/api/runs", response_model=list[RunOut])
def list_runs(
    pipeline: str | None = None,
    status: str | None = None,
    anomalies_only: bool = False,
    limit: int = Query(100, le=1000),
    session: Session = Depends(get_session),
):
    q = select(PipelineRun).join(Pipeline).order_by(PipelineRun.started_at.desc()).limit(limit)
    if pipeline:
        q = q.where(Pipeline.name == pipeline)
    if status:
        q = q.where(PipelineRun.status == status)
    if anomalies_only:
        q = q.where(PipelineRun.is_anomaly.is_(True))
    return [run_out(r) for r in session.scalars(q)]


@app.get("/api/runs/{run_id}", response_model=RunOut)
def get_run(run_id: int, session: Session = Depends(get_session)):
    run = session.get(PipelineRun, run_id)
    if run is None:
        raise HTTPException(404, "Run not found")
    return run_out(run)


@app.post("/api/runs/{run_id}/diagnose", response_model=RunOut)
def rediagnose(run_id: int, session: Session = Depends(get_session)):
    run = session.get(PipelineRun, run_id)
    if run is None:
        raise HTTPException(404, "Run not found")
    if run.status == "running":
        raise HTTPException(409, "Run is still in progress")
    attach_diagnosis(session, run)
    session.commit()
    return run_out(run)


@app.get("/api/freshness")
def get_freshness(session: Session = Depends(get_session)):
    return freshness(session)


@app.get("/api/metrics/summary")
def summary(hours: int = 24, session: Session = Depends(get_session)):
    since = utcnow() - timedelta(hours=hours)
    runs = session.scalars(
        select(PipelineRun).where(PipelineRun.started_at >= since, PipelineRun.status != "running")
    ).all()
    total = len(runs)
    failed = sum(r.status == "failed" for r in runs)
    durations = [r.duration_seconds for r in runs if r.duration_seconds is not None]
    by_category: dict[str, int] = {}
    for r in runs:
        if r.diagnosis:
            by_category[r.diagnosis.category] = by_category.get(r.diagnosis.category, 0) + 1
    return {
        "window_hours": hours,
        "total_runs": total,
        "failed_runs": failed,
        "success_rate": round((total - failed) / total, 3) if total else None,
        "anomalous_runs": sum(r.is_anomaly for r in runs),
        "avg_duration_seconds": round(sum(durations) / len(durations), 1) if durations else None,
        "stale_pipelines": [f["pipeline"] for f in freshness(session) if f["is_stale"]],
        "issues_by_category": by_category,
    }


@app.get("/api/export/runs.csv")
def export_runs(days: int = 30, session: Session = Depends(get_session)):
    """Flat CSV of runs + diagnoses, for Power BI / Excel when a direct DB connection isn't available."""
    since = utcnow() - timedelta(days=days)
    runs = session.scalars(
        select(PipelineRun).join(Pipeline).where(PipelineRun.started_at >= since)
        .order_by(PipelineRun.started_at)
    ).all()
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["run_id", "pipeline", "status", "started_at", "finished_at", "duration_seconds",
                "rows_processed", "is_anomaly", "issue_category", "root_cause", "error_message"])
    for r in runs:
        d = r.diagnosis
        w.writerow([r.id, r.pipeline.name, r.status, as_utc(r.started_at).isoformat(),
                    as_utc(r.finished_at).isoformat() if r.finished_at else "", r.duration_seconds,
                    r.rows_processed, r.is_anomaly, d.category if d else "", d.root_cause if d else "",
                    r.error_message or ""])
    buf.seek(0)
    return StreamingResponse(iter([buf.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": "attachment; filename=pipeline_runs.csv"})
