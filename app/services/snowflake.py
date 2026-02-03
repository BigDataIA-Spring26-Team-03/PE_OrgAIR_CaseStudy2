# app/services/snowflake.py
import snowflake.connector
from typing import Optional, List, Dict, Any
from uuid import UUID, uuid4
from datetime import datetime, timezone

from app.config import settings


class SnowflakeService:
    """Service for Snowflake database operations"""

    def __init__(self):
        self.connection = None

    def connect(self):
        """Establish connection to Snowflake"""
        if not self.connection:
            self.connection = snowflake.connector.connect(
                account=settings.SNOWFLAKE_ACCOUNT,
                user=settings.SNOWFLAKE_USER,
                password=settings.SNOWFLAKE_PASSWORD,
                database=settings.SNOWFLAKE_DATABASE,
                schema=settings.SNOWFLAKE_SCHEMA,
                warehouse=settings.SNOWFLAKE_WAREHOUSE,
                role="ACCOUNTADMIN",
            )
        return self.connection

    def close(self):
        """Close Snowflake connection"""
        if self.connection:
            self.connection.close()
            self.connection = None

    def _lowercase_keys(self, data: List[Dict]) -> List[Dict]:
        """Convert dictionary keys to lowercase for Pydantic compatibility"""
        return [{k.lower(): v for k, v in row.items()} for row in data]

    def execute_query(self, query: str, params: Optional[Dict] = None) -> List[Dict]:
        """Execute SELECT query and return results with lowercase keys"""
        conn = self.connect()
        cursor = conn.cursor(snowflake.connector.DictCursor)
        try:
            cursor.execute(query, params or {})
            results = cursor.fetchall()
            return self._lowercase_keys(results)
        finally:
            cursor.close()

    def execute_update(self, query: str, params: Optional[Dict] = None) -> int:
        """Execute INSERT/UPDATE/DELETE and return rows affected"""
        conn = self.connect()
        cursor = conn.cursor()
        try:
            cursor.execute(query, params or {})
            conn.commit()
            return cursor.rowcount
        finally:
            cursor.close()

    async def check_health(self) -> str:
        """Check if Snowflake is accessible"""
        try:
            self.execute_query("SELECT 1")
            return "healthy"
        except Exception as e:
            print(f"Snowflake health check failed: {e}")
            return "unhealthy"

    # ========================================
    # COMPANY CRUD
    # ========================================

    def create_company(self, company_data: Dict) -> str:
        company_id = str(uuid4())
        query = """
            INSERT INTO companies (id, name, ticker, industry_id, position_factor, created_at, updated_at)
            VALUES (%(id)s, %(name)s, %(ticker)s, %(industry_id)s, %(position_factor)s, %(created_at)s, %(updated_at)s)
        """
        now = datetime.now(timezone.utc)
        params = {
            "id": company_id,
            "name": company_data["name"],
            "ticker": company_data.get("ticker"),
            "industry_id": str(company_data["industry_id"]),
            "position_factor": company_data.get("position_factor", 0.0),
            "created_at": now,
            "updated_at": now,
        }
        self.execute_update(query, params)
        return company_id

    def get_company(self, company_id: str) -> Optional[Dict]:
        query = """
            SELECT * FROM companies
            WHERE id = %(id)s AND is_deleted = FALSE
        """
        results = self.execute_query(query, {"id": company_id})
        return results[0] if results else None

    def list_companies(self, limit: int = 10, offset: int = 0) -> List[Dict]:
        query = """
            SELECT * FROM companies
            WHERE is_deleted = FALSE
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        return self.execute_query(query, {"limit": limit, "offset": offset})

    def update_company(self, company_id: str, update_data: Dict) -> bool:
        set_clauses = []
        params = {"id": company_id, "updated_at": datetime.now(timezone.utc)}

        for key, value in update_data.items():
            if value is not None:
                set_clauses.append(f"{key} = %({key})s")
                params[key] = str(value) if isinstance(value, UUID) else value

        if not set_clauses:
            return False

        set_clauses.append("updated_at = %(updated_at)s")

        query = f"""
            UPDATE companies
            SET {', '.join(set_clauses)}
            WHERE id = %(id)s AND is_deleted = FALSE
        """
        rows = self.execute_update(query, params)
        return rows > 0

    def delete_company(self, company_id: str) -> bool:
        query = """
            UPDATE companies
            SET is_deleted = TRUE, updated_at = %(updated_at)s
            WHERE id = %(id)s
        """
        rows = self.execute_update(
            query,
            {"id": company_id, "updated_at": datetime.now(timezone.utc)},
        )
        return rows > 0

    # ========================================
    # INDUSTRIES
    # ========================================

    def get_industry(self, industry_id: str) -> Optional[Dict]:
        query = "SELECT * FROM industries WHERE id = %(id)s"
        results = self.execute_query(query, {"id": industry_id})
        return results[0] if results else None

    def list_industries(self) -> List[Dict]:
        query = "SELECT * FROM industries ORDER BY name"
        return self.execute_query(query)

    # ========================================
    # ASSESSMENTS CRUD
    # ========================================

    def create_assessment(self, assessment_data: Dict) -> str:
        assessment_id = str(uuid4())

        # convert enum -> string if needed
        assessment_type = assessment_data["assessment_type"]
        if hasattr(assessment_type, "value"):
            assessment_type = assessment_type.value

        query = """
            INSERT INTO assessments (
                id, company_id, assessment_type, assessment_date,
                primary_assessor, secondary_assessor, status, created_at
            )
            VALUES (
                %(id)s, %(company_id)s, %(assessment_type)s, %(assessment_date)s,
                %(primary_assessor)s, %(secondary_assessor)s, %(status)s, %(created_at)s
            )
        """

        params = {
            "id": assessment_id,
            "company_id": str(assessment_data["company_id"]),
            "assessment_type": assessment_type,
            "assessment_date": assessment_data["assessment_date"],
            "primary_assessor": assessment_data.get("primary_assessor"),
            "secondary_assessor": assessment_data.get("secondary_assessor"),
            "status": "draft",
            "created_at": datetime.now(timezone.utc),
        }

        self.execute_update(query, params)
        return assessment_id

    def get_assessment(self, assessment_id: str) -> Optional[Dict]:
        query = "SELECT * FROM assessments WHERE id = %(id)s"
        results = self.execute_query(query, {"id": assessment_id})
        return results[0] if results else None

    def list_assessments(
        self,
        limit: int = 10,
        offset: int = 0,
        company_id: Optional[str] = None,
    ) -> List[Dict]:
        where_clause = "WHERE 1=1"
        params: Dict[str, Any] = {"limit": limit, "offset": offset}

        if company_id:
            where_clause += " AND company_id = %(company_id)s"
            params["company_id"] = company_id

        query = f"""
            SELECT * FROM assessments
            {where_clause}
            ORDER BY created_at DESC
            LIMIT %(limit)s OFFSET %(offset)s
        """
        return self.execute_query(query, params)

    def update_assessment_status(self, assessment_id: str, status: str) -> bool:
        query = """
            UPDATE assessments
            SET status = %(status)s
            WHERE id = %(id)s
        """
        rows = self.execute_update(query, {"id": assessment_id, "status": status})
        return rows > 0

    # ========================================
    # DIMENSION SCORES CRUD
    # ========================================

    def create_dimension_score(self, score_data: Dict) -> str:
        score_id = str(uuid4())

        dimension = score_data["dimension"]
        if hasattr(dimension, "value"):
            dimension = dimension.value

        query = """
            INSERT INTO dimension_scores (
                id, assessment_id, dimension, score, weight,
                confidence, evidence_count, created_at
            )
            VALUES (
                %(id)s, %(assessment_id)s, %(dimension)s, %(score)s, %(weight)s,
                %(confidence)s, %(evidence_count)s, %(created_at)s
            )
        """
        params = {
            "id": score_id,
            "assessment_id": str(score_data["assessment_id"]),
            "dimension": dimension,
            "score": score_data["score"],
            "weight": score_data.get("weight"),
            "confidence": score_data.get("confidence", 0.8),
            "evidence_count": score_data.get("evidence_count", 0),
            "created_at": datetime.now(timezone.utc),
        }
        self.execute_update(query, params)
        return score_id

    def get_dimension_scores(self, assessment_id: str) -> List[Dict]:
        query = """
            SELECT * FROM dimension_scores
            WHERE assessment_id = %(assessment_id)s
            ORDER BY dimension
        """
        return self.execute_query(query, {"assessment_id": assessment_id})

    def get_dimension_score(self, score_id: str) -> Optional[Dict]:
        query = "SELECT * FROM dimension_scores WHERE id = %(id)s"
        results = self.execute_query(query, {"id": score_id})
        return results[0] if results else None

    def update_dimension_score(self, score_id: str, update_data: Dict) -> bool:
        set_clauses = []
        params: Dict[str, Any] = {"id": score_id}

        for key, value in update_data.items():
            if value is not None:
                # convert enum -> string if needed
                if hasattr(value, "value"):
                    value = value.value
                set_clauses.append(f"{key} = %({key})s")
                params[key] = value

        if not set_clauses:
            return False

        query = f"""
            UPDATE dimension_scores
            SET {', '.join(set_clauses)}
            WHERE id = %(id)s
        """
        rows = self.execute_update(query, params)
        return rows > 0

    def delete_dimension_score(self, score_id: str) -> bool:
        query = "DELETE FROM dimension_scores WHERE id = %(id)s"
        rows = self.execute_update(query, {"id": score_id})
        return rows > 0

    def delete_dimension_score_by_assessment_and_dimension(self, assessment_id: str, dimension: str) -> bool:
        """
        Delete by natural key (assessment_id, dimension), matching UNIQUE constraint.
        """
        query = """
            DELETE FROM dimension_scores
            WHERE assessment_id = %(assessment_id)s AND dimension = %(dimension)s
        """
        rows = self.execute_update(query, {"assessment_id": assessment_id, "dimension": dimension})
        return rows > 0


# Global instance
db = SnowflakeService()
