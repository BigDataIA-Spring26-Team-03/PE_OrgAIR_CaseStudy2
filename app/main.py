from fastapi import FastAPI

from app.config import settings
from app.routers.health import router as health_router
from app.routers.companies import router as companies_router
from app.routers.assessments import router as assessments_router
from app.routers.dimension import router as dimension_router, scores_router
from app.routers.documents import router as documents_router
from app.routers.signals import router as signals_router


def create_app() -> FastAPI:
    app = FastAPI(
        title=settings.APP_NAME,
        version=settings.APP_VERSION,
        debug=settings.DEBUG,
    )

    app.include_router(health_router)
    app.include_router(companies_router, prefix="/api/v1")
    app.include_router(assessments_router, prefix="/api/v1")
    app.include_router(dimension_router, prefix="/api/v1")
    app.include_router(scores_router, prefix="/api/v1")
    app.include_router(documents_router)
    app.include_router(signals_router)

    return app


app = create_app()
