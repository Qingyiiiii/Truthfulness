"""LangGraph state machine for evidence-bound answers and safe escalation."""

from __future__ import annotations

from collections import defaultdict
import asyncio
import operator
from pathlib import Path
import re
from time import perf_counter
from typing import Annotated, Callable, Literal, TypedDict, TypeVar
import uuid

from langgraph.graph import END, START, StateGraph

from video_truthfulness.core.agent_config import AgentSettings
from video_truthfulness.core.agent_generation import AnswerGenerator, build_generator
from video_truthfulness.core.agent_models import (
    AgentTelemetry,
    Citation,
    CitationValidation,
    CostSource,
    CostUsage,
    CreateReviewTaskInput,
    EvidenceAssessment,
    FailureCode,
    GeneratedAnswer,
    NodeStatus,
    NodeTrace,
    OutcomeStatus,
    QueryCategory,
    QueryClassification,
    QueryRequest,
    QueryResponse,
    RetrievedEvidence,
    RiskLevel,
    TokenSource,
    TokenUsage,
)
from video_truthfulness.core.agent_retrieval import (
    ChromaEvidenceStore,
    build_embedding_backend,
    load_sources,
)
from video_truthfulness.core.agent_tools import AgentTools, ReviewTaskStore


class AgentState(TypedDict, total=False):
    request: QueryRequest
    trace_id: str
    fault_stage: str | None
    classification: QueryClassification
    retrieved: list[RetrievedEvidence]
    assessment: EvidenceAssessment
    generated: GeneratedAnswer
    citation_validation: CitationValidation
    outcome_status: OutcomeStatus
    review_task_id: str | None
    failure_code: FailureCode | None
    failure_message: str | None
    node_traces: Annotated[list[NodeTrace], operator.add]
    tokens: TokenUsage
    cost: CostUsage


T = TypeVar("T")


_UNAUTHORIZED_PATTERNS = (
    r"绕过.*(登录|付费|权限|风控)",
    r"提取.*(cookie|token|令牌|密码)",
    r"破解.*drm",
    r"bypass.*(login|paywall|access control)",
    r"steal.*(cookie|token|password)",
)
_HIGH_RISK_PATTERNS = (
    r"替我决定.*(买入|卖出|投资)",
    r"直接告诉我.*(吃药|停药|剂量)",
    r"替我下.*法律结论",
    r"guarantee.*(investment|medical|legal)",
)
_PROMPT_PATTERNS = (
    r"忽略.*(系统|开发者|之前).*(指令|要求)",
    r"ignore.*(system|developer|previous).*(instruction|message)",
)
_SOURCE_INJECTION_PATTERNS = (
    r"ignore (all |the )?(previous|system|developer)",
    r"system message",
    r"developer message",
    r"reveal (the )?(secret|token|password)",
    r"do not cite",
    r"omit citations",
    r"忽略.*(指令|系统|开发者)",
    r"泄露.*(密钥|令牌|密码)",
    r"不要.*引用",
)


def _matches_any(text: str, patterns: tuple[str, ...]) -> bool:
    return any(re.search(pattern, text, flags=re.IGNORECASE) for pattern in patterns)


def classify_request(request: QueryRequest) -> QueryClassification:
    """Apply deterministic authorization and high-risk routing before retrieval."""

    if not request.authorized:
        return QueryClassification(
            category=QueryCategory.UNAUTHORIZED,
            allowed=False,
            risk_level=RiskLevel.HIGH,
            reason="Caller did not confirm authorization for the input.",
        )
    if _matches_any(request.query, _UNAUTHORIZED_PATTERNS):
        return QueryClassification(
            category=QueryCategory.UNAUTHORIZED,
            allowed=False,
            risk_level=RiskLevel.HIGH,
            reason="Request asks to bypass access controls or expose credentials.",
        )
    if _matches_any(request.query, _PROMPT_PATTERNS):
        return QueryClassification(
            category=QueryCategory.PROMPT_MANIPULATION,
            allowed=False,
            risk_level=RiskLevel.HIGH,
            reason="Request attempts to override workflow instructions.",
        )
    if _matches_any(request.query, _HIGH_RISK_PATTERNS):
        return QueryClassification(
            category=QueryCategory.HIGH_RISK_DECISION,
            allowed=False,
            risk_level=RiskLevel.HIGH,
            reason="Truthfulness can organize evidence but cannot make the requested high-risk final decision.",
        )
    return QueryClassification(
        category=QueryCategory.FACT_CHECK,
        allowed=True,
        risk_level=RiskLevel.LOW,
        reason="Request is eligible for evidence retrieval.",
    )


def _lexical_overlap(query: str, content: str) -> float:
    def features(text: str) -> set[str]:
        lowered = text.lower()
        latin = set(re.findall(r"[a-z0-9._-]{3,}", lowered))
        chinese = "".join(re.findall(r"[\u4e00-\u9fff]", lowered))
        bigrams = {chinese[index : index + 2] for index in range(max(0, len(chinese) - 1))}
        return latin | bigrams

    query_features = features(query)
    if not query_features:
        return 0.0
    return len(query_features & features(content)) / len(query_features)


def assess_evidence(query: str, evidence: list[RetrievedEvidence], threshold: float) -> EvidenceAssessment:
    if (
        not evidence
        or evidence[0].score < threshold
        or _lexical_overlap(query, evidence[0].source.content) < 0.08
    ):
        return EvidenceAssessment(
            sufficient=False,
            reason="No authorized source exceeded the minimum retrieval relevance threshold.",
        )
    conflict_values: dict[str, set[str]] = defaultdict(set)
    for item in evidence:
        source = item.source
        if (
            item.score < threshold
            or _lexical_overlap(query, source.content) < 0.2
            or not source.conflict_group
            or source.claim_value is None
        ):
            continue
        conflict_values[source.conflict_group].add(source.claim_value)
    conflicts = [group for group, values in conflict_values.items() if len(values) > 1]
    if conflicts:
        return EvidenceAssessment(
            sufficient=True,
            conflicting=True,
            reason=f"Conflicting source values found for: {', '.join(conflicts)}.",
        )
    return EvidenceAssessment(sufficient=True, reason="At least one authorized relevant source is available.")


def _citation_quote(content: str) -> str:
    for line in content.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        if _matches_any(stripped, _SOURCE_INJECTION_PATTERNS):
            continue
        return stripped[:500]
    return ""


class AgentService:
    """Own the stores, tools, graph, and response telemetry."""

    def __init__(
        self,
        settings: AgentSettings | None = None,
        generator: AnswerGenerator | None = None,
    ) -> None:
        self.settings = settings or AgentSettings.from_env()
        self.settings.runtime_dir.mkdir(parents=True, exist_ok=True)
        backend = build_embedding_backend(
            self.settings.embedding_backend,
            self.settings.embedding_model,
            self.settings.embedding_cache_dir,
        )
        self.evidence_store = ChromaEvidenceStore(
            persist_dir=self.settings.chroma_dir,
            collection_name=self.settings.collection_name,
            embedding_backend=backend,
        )
        self.evidence_store.index_sources(load_sources(self.settings.source_path))
        self.review_store = ReviewTaskStore(self.settings.review_db_path)
        self.tools = AgentTools(self.evidence_store, self.review_store)
        self.generator = generator or build_generator(self.settings)
        self.graph = self._build_graph()

    async def _run_stage(
        self,
        state: AgentState,
        stage: str,
        failure_code: FailureCode,
        operation: Callable[[], T],
    ) -> tuple[T | None, NodeTrace, FailureCode | None, str | None]:
        started = perf_counter()
        last_error: Exception | None = None
        timed_out = False
        for attempt in range(1, self.settings.max_attempts + 1):
            try:
                if state.get("fault_stage") == stage:
                    raise TimeoutError(f"Injected {stage} timeout")
                result = await asyncio.wait_for(
                    asyncio.to_thread(operation),
                    timeout=self.settings.stage_timeout_seconds,
                )
                return (
                    result,
                    NodeTrace(
                        node=stage,
                        status=NodeStatus.SUCCESS,
                        elapsed_ms=round((perf_counter() - started) * 1000, 3),
                        attempts=attempt,
                    ),
                    None,
                    None,
                )
            except (TimeoutError, asyncio.TimeoutError) as exc:
                last_error = exc
                timed_out = True
            except Exception as exc:  # stage boundary converts errors to explicit state
                last_error = exc
                timed_out = False
                break
        attempts = self.settings.max_attempts if timed_out else 1
        status = NodeStatus.TIMEOUT if timed_out else NodeStatus.FAILED
        code = failure_code if timed_out else FailureCode.INTERNAL_ERROR
        message = str(last_error)[:500] if last_error else f"{stage} failed"
        return (
            None,
            NodeTrace(
                node=stage,
                status=status,
                elapsed_ms=round((perf_counter() - started) * 1000, 3),
                attempts=attempts,
                error_code=code,
                error_message=message,
            ),
            code,
            message,
        )

    async def _classify_node(self, state: AgentState) -> AgentState:
        started = perf_counter()
        classification = classify_request(state["request"])
        return {
            "classification": classification,
            "node_traces": [
                NodeTrace(
                    node="classify",
                    status=NodeStatus.SUCCESS,
                    elapsed_ms=round((perf_counter() - started) * 1000, 3),
                    attempts=1,
                )
            ],
        }

    async def _retrieve_node(self, state: AgentState) -> AgentState:
        request = state["request"]
        result, trace, code, message = await self._run_stage(
            state,
            "retrieve",
            FailureCode.RETRIEVAL_TIMEOUT,
            lambda: self.evidence_store.retrieve(request.query, request.top_k),
        )
        update: AgentState = {"node_traces": [trace]}
        if result is not None:
            update["retrieved"] = result
        else:
            update["failure_code"] = code
            update["failure_message"] = message
            update["outcome_status"] = OutcomeStatus.TIMEOUT if trace.status == NodeStatus.TIMEOUT else OutcomeStatus.FAILED
        return update

    async def _evidence_check_node(self, state: AgentState) -> AgentState:
        started = perf_counter()
        assessment = assess_evidence(
            state["request"].query,
            state.get("retrieved", []),
            self.settings.min_relevance_score,
        )
        return {
            "assessment": assessment,
            "node_traces": [
                NodeTrace(
                    node="evidence_check",
                    status=NodeStatus.SUCCESS,
                    elapsed_ms=round((perf_counter() - started) * 1000, 3),
                    attempts=1,
                )
            ],
        }

    async def _generate_node(self, state: AgentState) -> AgentState:
        result, trace, code, message = await self._run_stage(
            state,
            "generate",
            FailureCode.GENERATION_TIMEOUT,
            lambda: self.generator.generate(
                state["request"].query,
                state.get("retrieved", []),
                state["assessment"],
            ),
        )
        update: AgentState = {"node_traces": [trace]}
        if result is not None:
            update["generated"] = result.generated
            update["tokens"] = result.tokens
            update["cost"] = result.cost
        else:
            update["failure_code"] = code
            update["failure_message"] = message
            update["outcome_status"] = OutcomeStatus.TIMEOUT if trace.status == NodeStatus.TIMEOUT else OutcomeStatus.FAILED
        return update

    def _validate_citations(self, state: AgentState) -> CitationValidation:
        generated = state["generated"]
        assessment = state["assessment"]
        if generated.review_reason:
            return CitationValidation(valid=False, reason=generated.review_reason)
        if not assessment.sufficient:
            if generated.citation_source_ids:
                return CitationValidation(valid=False, reason="Insufficient-evidence answer included citations.")
            return CitationValidation(valid=True, reason="No-answer response correctly contains no citation.")
        by_id = {item.source.source_id: item for item in state.get("retrieved", [])}
        if not generated.citation_source_ids:
            return CitationValidation(valid=False, reason="Evidence-backed answer omitted citations.")
        citations: list[Citation] = []
        for source_id in generated.citation_source_ids:
            item = by_id.get(source_id)
            if item is None:
                return CitationValidation(valid=False, reason=f"Citation was not retrieved: {source_id}")
            source_info = self.tools.source_info(source_id)
            quote = _citation_quote(item.source.content)
            if not quote or quote not in item.source.content:
                return CitationValidation(valid=False, reason=f"Citation quote is not anchored: {source_id}")
            citations.append(
                Citation(
                    source_id=source_info.source_id,
                    page_title=source_info.title,
                    publisher=source_info.publisher,
                    source_url=source_info.source_url,
                    quote=quote,
                    retrieved_at=source_info.retrieved_at,
                    score=item.score,
                )
            )
        return CitationValidation(valid=True, citations=citations, reason="All citations resolve to retrieved sources.")

    async def _citation_node(self, state: AgentState) -> AgentState:
        result, trace, code, message = await self._run_stage(
            state,
            "citation_validate",
            FailureCode.CITATION_TIMEOUT,
            lambda: self._validate_citations(state),
        )
        update: AgentState = {"node_traces": [trace]}
        if result is None:
            update["failure_code"] = code
            update["failure_message"] = message
            update["outcome_status"] = OutcomeStatus.TIMEOUT if trace.status == NodeStatus.TIMEOUT else OutcomeStatus.FAILED
            return update
        update["citation_validation"] = result
        if result.valid:
            update["outcome_status"] = (
                OutcomeStatus.ANSWERED if state["assessment"].sufficient else OutcomeStatus.INSUFFICIENT_EVIDENCE
            )
        else:
            update["failure_code"] = FailureCode.INVALID_CITATION
            update["failure_message"] = result.reason
        return update

    async def _refuse_node(self, state: AgentState) -> AgentState:
        started = perf_counter()
        classification = state["classification"]
        code = FailureCode.UNAUTHORIZED if classification.category == QueryCategory.UNAUTHORIZED else FailureCode.POLICY_REFUSAL
        return {
            "generated": GeneratedAnswer(answer=f"拒绝处理：{classification.reason}"),
            "outcome_status": OutcomeStatus.REFUSED,
            "failure_code": code,
            "failure_message": classification.reason,
            "tokens": TokenUsage(prompt_tokens=0, completion_tokens=0, total_tokens=0, source=TokenSource.NO_LLM),
            "cost": CostUsage(amount_usd=0.0, source=CostSource.NO_LLM),
            "node_traces": [
                NodeTrace(
                    node="refuse",
                    status=NodeStatus.SUCCESS,
                    elapsed_ms=round((perf_counter() - started) * 1000, 3),
                    attempts=1,
                )
            ],
        }

    async def _human_review_node(self, state: AgentState) -> AgentState:
        reason = state.get("failure_message") or state.get("citation_validation", CitationValidation(valid=False, reason="Unknown")).reason
        evidence_ids = [item.source.source_id for item in state.get("retrieved", [])]
        result, trace, code, message = await self._run_stage(
            state,
            "human_review",
            FailureCode.TOOL_TIMEOUT,
            lambda: self.tools.create_review(
                CreateReviewTaskInput(
                    trace_id=state["trace_id"],
                    query=state["request"].query,
                    reason=reason,
                    evidence_ids=evidence_ids,
                )
            ),
        )
        update: AgentState = {"node_traces": [trace]}
        if result is not None:
            update["review_task_id"] = result.task_id
            update["outcome_status"] = OutcomeStatus.HUMAN_REVIEW_REQUIRED
            update["generated"] = state.get("generated", GeneratedAnswer(answer="已创建人工复核任务。"))
        else:
            update["failure_code"] = code
            update["failure_message"] = message
            update["outcome_status"] = OutcomeStatus.TIMEOUT if trace.status == NodeStatus.TIMEOUT else OutcomeStatus.FAILED
        return update

    async def _finalize_node(self, state: AgentState) -> AgentState:
        return {
            "node_traces": [
                NodeTrace(node="finalize", status=NodeStatus.SUCCESS, elapsed_ms=0.0, attempts=1)
            ]
        }

    @staticmethod
    def _route_classification(state: AgentState) -> Literal["retrieve", "refuse"]:
        return "retrieve" if state["classification"].allowed else "refuse"

    @staticmethod
    def _route_retrieval(state: AgentState) -> Literal["evidence_check", "human_review", "finalize"]:
        if state.get("outcome_status") == OutcomeStatus.TIMEOUT:
            return "finalize"
        if state.get("outcome_status") == OutcomeStatus.FAILED:
            return "human_review"
        return "evidence_check"

    @staticmethod
    def _route_generation(state: AgentState) -> Literal["citation_validate", "human_review", "finalize"]:
        if state.get("outcome_status") == OutcomeStatus.TIMEOUT:
            return "finalize"
        if state.get("outcome_status") == OutcomeStatus.FAILED:
            return "human_review"
        return "citation_validate"

    @staticmethod
    def _route_citation(state: AgentState) -> Literal["human_review", "finalize"]:
        validation = state.get("citation_validation")
        if validation is not None and not validation.valid:
            return "human_review"
        return "finalize"

    def _build_graph(self):
        builder = StateGraph(AgentState)
        builder.add_node("classify", self._classify_node)
        builder.add_node("retrieve", self._retrieve_node)
        builder.add_node("evidence_check", self._evidence_check_node)
        builder.add_node("generate", self._generate_node)
        builder.add_node("citation_validate", self._citation_node)
        builder.add_node("refuse", self._refuse_node)
        builder.add_node("human_review", self._human_review_node)
        builder.add_node("finalize", self._finalize_node)
        builder.add_edge(START, "classify")
        builder.add_conditional_edges("classify", self._route_classification)
        builder.add_conditional_edges("retrieve", self._route_retrieval)
        builder.add_edge("evidence_check", "generate")
        builder.add_conditional_edges("generate", self._route_generation)
        builder.add_conditional_edges("citation_validate", self._route_citation)
        builder.add_edge("refuse", "finalize")
        builder.add_edge("human_review", "finalize")
        builder.add_edge("finalize", END)
        return builder.compile()

    async def query(
        self,
        request: QueryRequest,
        trace_id: str | None = None,
        fault_stage: str | None = None,
    ) -> QueryResponse:
        trace_id = trace_id or uuid.uuid4().hex
        started = perf_counter()
        initial: AgentState = {
            "request": request,
            "trace_id": trace_id,
            "fault_stage": fault_stage,
            "node_traces": [],
            "tokens": TokenUsage(source=TokenSource.UNAVAILABLE),
            "cost": CostUsage(source=CostSource.UNAVAILABLE),
        }
        try:
            state = await asyncio.wait_for(
                self.graph.ainvoke(initial),
                timeout=self.settings.request_timeout_seconds,
            )
        except (TimeoutError, asyncio.TimeoutError):
            classification = classify_request(request)
            trace = NodeTrace(
                node="request",
                status=NodeStatus.TIMEOUT,
                elapsed_ms=round((perf_counter() - started) * 1000, 3),
                attempts=1,
                error_code=FailureCode.INTERNAL_ERROR,
                error_message="Overall request timeout exceeded.",
            )
            return QueryResponse(
                trace_id=trace_id,
                status=OutcomeStatus.TIMEOUT,
                classification=classification,
                answer="请求整体超时，未生成结论。",
                failure_code=FailureCode.INTERNAL_ERROR,
                failure_message="Overall request timeout exceeded.",
                telemetry=AgentTelemetry(
                    total_elapsed_ms=trace.elapsed_ms,
                    nodes=[trace],
                    timed_out=True,
                ),
            )
        nodes = state.get("node_traces", [])
        status = state.get("outcome_status", OutcomeStatus.FAILED)
        generated = state.get("generated", GeneratedAnswer(answer="工作流未生成可用输出。"))
        validation = state.get("citation_validation")
        retries = sum(max(0, node.attempts - 1) for node in nodes)
        return QueryResponse(
            trace_id=trace_id,
            status=status,
            classification=state.get("classification", classify_request(request)),
            answer=generated.answer,
            citations=validation.citations if validation and validation.valid else [],
            review_task_id=state.get("review_task_id"),
            failure_code=state.get("failure_code"),
            failure_message=state.get("failure_message"),
            telemetry=AgentTelemetry(
                total_elapsed_ms=round((perf_counter() - started) * 1000, 3),
                nodes=nodes,
                tokens=state.get("tokens", TokenUsage(source=TokenSource.UNAVAILABLE)),
                cost=state.get("cost", CostUsage(source=CostSource.UNAVAILABLE)),
                retries=retries,
                timed_out=status == OutcomeStatus.TIMEOUT,
            ),
        )

    def source_info(self, source_id: str):
        return self.tools.source_info(source_id)

    def review_task(self, task_id: str):
        return self.review_store.get(task_id)

    def health(self) -> dict[str, object]:
        return {
            "status": "ok",
            "indexed_sources": self.evidence_store.count(),
            "embedding_backend": self.settings.embedding_backend,
            "llm_provider": self.settings.llm_provider,
        }
