import pytest
from pydantic import ValidationError

from video_truthfulness.agent_models import QueryRequest


def test_query_request_rejects_schema_drift() -> None:
    with pytest.raises(ValidationError):
        QueryRequest.model_validate({"query": "valid query", "unexpected": "not allowed"})
