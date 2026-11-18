import os
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import JSON, DateTime, Float, ForeignKey, Integer, String, Text, create_engine, text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship, sessionmaker

DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///./monitor.db")
VIEWS_SQL = Path(__file__).resolve().parent.parent / "sql" / "powerbi_views.sql"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
    pool_pre_ping=True,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Base(DeclarativeBase):
    pass


class Pipeline(Base):
    __tablename__ = "pipelines"

    id: Mapped[int] = mapped_column(primary_key=True)
    name: Mapped[str] = mapped_column(String(120), unique=True)
    owner: Mapped[str | None] = mapped_column(String(120))
    schedule: Mapped[str | None] = mapped_column(String(60))
    freshness_sla_minutes: Mapped[int] = mapped_column(Integer, default=1440)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    runs: Mapped[list["PipelineRun"]] = relationship(back_populates="pipeline", cascade="all, delete-orphan")


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"

    id: Mapped[int] = mapped_column(primary_key=True)
    pipeline_id: Mapped[int] = mapped_column(ForeignKey("pipelines.id", ondelete="CASCADE"), index=True)
    status: Mapped[str] = mapped_column(String(20), index=True)  # running | success | failed
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), index=True)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    duration_seconds: Mapped[float | None] = mapped_column(Float)
    rows_processed: Mapped[int | None] = mapped_column(Integer)
    error_message: Mapped[str | None] = mapped_column(Text)
    is_anomaly: Mapped[bool] = mapped_column(default=False, index=True)
    anomaly_reasons: Mapped[list | None] = mapped_column(JSON)

    pipeline: Mapped[Pipeline] = relationship(back_populates="runs")
    diagnosis: Mapped["Diagnosis | None"] = relationship(
        back_populates="run", cascade="all, delete-orphan", uselist=False
    )


class Diagnosis(Base):
    __tablename__ = "diagnoses"

    id: Mapped[int] = mapped_column(primary_key=True)
    run_id: Mapped[int] = mapped_column(ForeignKey("pipeline_runs.id", ondelete="CASCADE"), unique=True)
    category: Mapped[str] = mapped_column(String(60))
    root_cause: Mapped[str] = mapped_column(Text)
    recommended_actions: Mapped[list] = mapped_column(JSON)
    confidence: Mapped[float] = mapped_column(Float)
    source: Mapped[str] = mapped_column(String(20))  # openai | rules
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), default=utcnow)

    run: Mapped[PipelineRun] = relationship(back_populates="diagnosis")


def init_db() -> None:
    Base.metadata.create_all(engine)
    # Reporting views for Power BI (PostgreSQL only).
    if engine.dialect.name == "postgresql" and VIEWS_SQL.exists():
        with engine.begin() as conn:
            conn.execute(text(VIEWS_SQL.read_text()))


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()


def drop_all() -> None:
    if engine.dialect.name == "postgresql":
        with engine.begin() as conn:
            conn.execute(text("DROP VIEW IF EXISTS v_run_facts, v_daily_pipeline_stats, v_pipeline_freshness"))
    Base.metadata.drop_all(engine)
