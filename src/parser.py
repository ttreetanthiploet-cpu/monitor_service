"""
parser.py — extracts structured monitoring data from a raw n8n execution dict.

Handles:
  - execution_log      (top-level turn metadata)
  - agent_call_log     (per-LLM-call token/timing data)
  - http_request_log   (external HTTP calls)
  - workflow_agent_flags (which nodes were activated)

Supports multiple workflows:
  - CQCLdVdNwrmvI5do  Prototype_v1.2  (main orchestrator, no direct LLM/HTTP nodes)
  - 5Mr9iwbhAJZYhugN  AdvisorWorkFlow_v1.3  (has LLM + HTTP Request → Offer Engine)
"""
import json
import logging
from typing import Any, Optional

from .config import Config
from .n8n_client import parse_dt, ms_between

log = logging.getLogger(__name__)

# ── Token usage key names searched recursively ────────────────────────────────
USAGE_KEYS = {"tokenUsage", "tokenUsageEstimate", "usage", "usage_metadata", "usageMetadata"}

# ── Per-workflow configuration registry ──────────────────────────────────────
# Each entry describes how to parse one workflow's executions.
WORKFLOW_REGISTRY: dict[str, dict] = {

    # Prototype_v1.2 — main orchestrator
    # LLM and HTTP calls happen inside sub-workflows, not here.
    "CQCLdVdNwrmvI5do": {
        "name":         "Prototype_v1.2",
        "trigger_node": "Webhook",         # webhook body is at run_data[node].body
        "trigger_is_webhook": True,
        "final_output_nodes": [
            "OutputToWebhook",
            "Output_summary",
            "get_output",
            "Auto Reply unknown intent",
        ],
        "agent_node_map": {},              # no direct LLM nodes
        "lm_subnode_map": {},
        "http_node_map":  {},              # no direct HTTP nodes
        "http_request_body_source": None, # node whose output is the HTTP request body
        "subworkflow_flag_map": {
            "Execute InputGuardrailWorkflow": "used_input_guardrail",
            "Execute ClassificationWorkflow":  "used_classification",
            "Execute AdvisorWorkflow":         "used_advisor",
            "Execute EducationWorkflow":       "used_education",
            "Execute Summary Workflow":        "used_summary",
            "Execute OutputGuardrail":         "used_output_guardrail",
        },
        "default_route": None,
    },

    # AdvisorWorkFlow_v1.3 — debt-solution sub-workflow
    # Contains one LLM agent and one HTTP call to the Offer Engine Python API.
    "5Mr9iwbhAJZYhugN": {
        "name":         "AdvisorWorkFlow_v1.3",
        "trigger_node": "Start",           # "Execute Workflow" trigger stores data directly
        "trigger_is_webhook": False,
        "final_output_nodes": [
            "Parsing output from python",
        ],
        "agent_node_map": {
            "Debt Solution Extractor": {
                "agent":    "Debt Solution Extractor",
                "workflow": "AdvisorWorkFlow_v1.3",
            },
        },
        "lm_subnode_map": {
            "Debt Solution Extractor": ["Google Gemini Chat Model"],
        },
        "http_node_map": {
            "HTTP Request": {"workflow": "AdvisorWorkFlow_v1.3"},
        },
        "http_request_body_source": "parsing to Offer Engine Python",
        "subworkflow_flag_map": {},
        "default_route": "advisor",
    },

    # Sub-workflow: intent classification
    "2FXKHDFj5TzZmU1X": {
        "name":                   "Intent_classification",
        "trigger_node":           "Start",
        "trigger_is_webhook":     False,
        "final_output_nodes":     [],
        "agent_node_map":         {},
        "lm_subnode_map":         {},
        "http_node_map":          {},
        "http_request_body_source": None,
        "subworkflow_flag_map":   {},
        "default_route":          None,
    },

    # Sub-workflow: education / RAG retrieval
    "FlV6JNP0eI9uUuQC": {
        "name":                   "Education v1.3",
        "trigger_node":           "Start",
        "trigger_is_webhook":     False,
        "final_output_nodes":     [],
        "agent_node_map":         {},
        "lm_subnode_map":         {},
        "http_node_map":          {},
        "http_request_body_source": None,
        "subworkflow_flag_map":   {},
        "default_route":          None,
    },

    # Sub-workflow: output guardrail
    "Lj62jSjq0LXEwvCI": {
        "name":                   "output Guardrail",
        "trigger_node":           "Start",
        "trigger_is_webhook":     False,
        "final_output_nodes":     [],
        "agent_node_map":         {},
        "lm_subnode_map":         {},
        "http_node_map":          {},
        "http_request_body_source": None,
        "subworkflow_flag_map":   {},
        "default_route":          None,
    },

    # Sub-workflow: summary generation
    "osHzEzfm0Nye2RFl": {
        "name":                   "Summary Workflow",
        "trigger_node":           "Start",
        "trigger_is_webhook":     False,
        "final_output_nodes":     [],
        "agent_node_map":         {},
        "lm_subnode_map":         {},
        "http_node_map":          {},
        "http_request_body_source": None,
        "subworkflow_flag_map":   {},
        "default_route":          None,
    },

    # Sub-workflow: input guardrail
    "rG7kIvCmskZEEvjV": {
        "name":                   "Input Guardrail",
        "trigger_node":           "Start",
        "trigger_is_webhook":     False,
        "final_output_nodes":     [],
        "agent_node_map":         {},
        "lm_subnode_map":         {},
        "http_node_map":          {},
        "http_request_body_source": None,
        "subworkflow_flag_map":   {},
        "default_route":          None,
    },
}

_DEFAULT_CONFIG: dict = {
    "name": "unknown",
    "trigger_node": "Webhook",
    "trigger_is_webhook": True,
    "final_output_nodes": [],
    "agent_node_map": {},
    "lm_subnode_map": {},
    "http_node_map": {},
    "http_request_body_source": None,
    "subworkflow_flag_map": {},
    "default_route": None,
}


# ── Utility helpers ───────────────────────────────────────────────────────────

def _truncate(text: Any, max_len: int) -> Optional[str]:
    if text is None:
        return None
    s = str(text)
    return s[:max_len] if len(s) > max_len else s


def _safe_json(obj: Any, max_chars: int) -> Optional[dict]:
    if obj is None:
        return None
    if isinstance(obj, dict):
        try:
            dumped = json.dumps(obj, ensure_ascii=False)
            if len(dumped) > max_chars:
                return {"_truncated": True, "preview": dumped[:max_chars]}
            return obj
        except Exception:
            return {"_error": "not serialisable"}
    return {"_raw": _truncate(str(obj), max_chars)}


def _find_usage(obj: Any) -> Optional[dict]:
    """Recursively find the first token-usage dict in a nested structure."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in USAGE_KEYS and isinstance(v, dict):
                return v
            found = _find_usage(v)
            if found:
                return found
    elif isinstance(obj, list):
        for item in obj:
            found = _find_usage(item)
            if found:
                return found
    return None


def _normalize_usage(usage: dict) -> dict[str, int]:
    inp = (
        usage.get("promptTokens") or usage.get("prompt_tokens")
        or usage.get("inputTokens") or usage.get("input_tokens")
        or usage.get("promptTokenCount") or 0  # Gemini usageMetadata format
    )
    out = (
        usage.get("completionTokens") or usage.get("completion_tokens")
        or usage.get("outputTokens") or usage.get("output_tokens")
        or usage.get("candidatesTokenCount") or 0  # Gemini usageMetadata format
    )
    return {"input_tokens": int(inp), "output_tokens": int(out), "total_tokens": int(inp + out)}


def _calc_cost(tokens: dict) -> dict[str, float]:
    inp_cost = (tokens["input_tokens"]  / 1_000_000) * Config.PRICE_INPUT_PER_1M
    out_cost = (tokens["output_tokens"] / 1_000_000) * Config.PRICE_OUTPUT_PER_1M
    total    = inp_cost + out_cost
    return {
        "input_cost_usd":  round(inp_cost, 6),
        "output_cost_usd": round(out_cost, 6),
        "total_cost_usd":  round(total, 6),
        "total_cost_thb":  round(total * Config.USD_TO_THB, 4),
    }


def _get_node_output_data(node_runs: list) -> Any:
    """Return the first output JSON from a node run list."""
    try:
        return node_runs[0]["data"]["main"][0][0]["json"]
    except (IndexError, KeyError, TypeError):
        return None


def _get_trigger_data(run_data: dict, trigger_node: str, is_webhook: bool) -> dict:
    """
    Extract input data from the workflow's entry point node.
    Webhooks wrap the POST body under .body; Execute-Workflow triggers don't.
    """
    try:
        raw = run_data[trigger_node][0]["data"]["main"][0][0]["json"]
        return raw.get("body", {}) if is_webhook else raw
    except (KeyError, IndexError, TypeError):
        return {}


def _get_final_output(run_data: dict, candidate_nodes: list[str]) -> dict:
    """Return the first non-empty JSON from the list of candidate output nodes."""
    for node_name in candidate_nodes:
        try:
            data = run_data[node_name][0]["data"]["main"][0][0]["json"]
            if data:
                return data
        except (KeyError, IndexError, TypeError):
            continue
    return {}


def _extract_model_map(execution: dict) -> dict[str, str]:
    """Return {node_name: modelName} from workflowData.nodes configuration."""
    result: dict[str, str] = {}
    for node in execution.get("workflowData", {}).get("nodes", []):
        model = node.get("parameters", {}).get("modelName")
        if model:
            result[node.get("name", "")] = model
    return result


def _extract_http_url_map(execution: dict) -> dict[str, str]:
    """Return {node_name: url} for HTTP Request nodes in the workflow config."""
    result: dict[str, str] = {}
    for node in execution.get("workflowData", {}).get("nodes", []):
        if "httpRequest" in node.get("type", ""):
            url = node.get("parameters", {}).get("url", "")
            result[node.get("name", "")] = url
    return result


def _extract_agent_node_map(execution: dict) -> dict[str, dict]:
    """
    Auto-detect all AI Agent nodes from workflowData.nodes.
    Returns {node_name: {agent, workflow}} for every langchain.agent node.
    """
    result: dict[str, dict] = {}
    wf_name = execution.get("workflowData", {}).get("name", "")
    for node in execution.get("workflowData", {}).get("nodes", []):
        if "langchain.agent" in node.get("type", "").lower():
            name = node["name"]
            result[name] = {"agent": name, "workflow": wf_name}
    return result


def _extract_lm_subnode_map(execution: dict) -> dict[str, list[str]]:
    """
    Auto-detect which LM model sub-nodes connect to which agent node by
    tracing ai_languageModel connections in workflowData.connections.
    Returns {agent_node_name: [lm_node_name, ...]} — list because an agent
    can have multiple LM models wired to it (tokens must be summed).
    """
    result: dict[str, list[str]] = {}
    for src, conn_types in execution.get("workflowData", {}).get("connections", {}).items():
        for targets in (conn_types.get("ai_languageModel") or []):
            for conn in (targets or []):
                agent = conn.get("node", "")
                if agent:
                    result.setdefault(agent, []).append(src)
    return result


def _extract_chain_node_map(execution: dict, exclude_lm_nodes: set[str]) -> dict[str, dict]:
    """
    Find LM model nodes that are sub-nodes of non-agent chain nodes
    (e.g. Guardrails, Basic LLM Chain) not already covered by agent detection.
    Returns {lm_node_name: {parent_node, agent, workflow}}.
    """
    result: dict[str, dict] = {}
    wf_name = execution.get("workflowData", {}).get("name", "")
    node_types = {
        n["name"]: n.get("type", "")
        for n in execution.get("workflowData", {}).get("nodes", [])
    }
    connections = execution.get("workflowData", {}).get("connections", {})

    # Build reverse main-connection map so we can find each chain node's input source
    main_input_of: dict[str, str] = {}
    for src, conn_types in connections.items():
        for groups in (conn_types.get("main") or []):
            for conn in (groups or []):
                target = conn.get("node", "")
                if target:
                    main_input_of[target] = src

    for src, conn_types in connections.items():
        for targets in (conn_types.get("ai_languageModel") or []):
            for conn in (targets or []):
                chain_node = conn.get("node", "")
                if not chain_node:
                    continue
                if src in exclude_lm_nodes:
                    continue  # already captured as agent LM sub-node
                if "langchain.agent" in node_types.get(chain_node, "").lower():
                    continue  # agent nodes are handled separately
                result[src] = {
                    "parent_node":  chain_node,
                    "agent":        chain_node,
                    "workflow":     wf_name,
                    "input_source": main_input_of.get(chain_node),
                }
    return result


# ── Main parser ───────────────────────────────────────────────────────────────

class ExecutionParser:

    def parse(self, execution: dict) -> dict:
        """
        Parse one raw n8n execution into 4 record groups ready for Supabase.
        Dispatches to workflow-specific logic based on workflowId.
        """
        exec_id     = str(execution.get("id"))
        workflow_id = str(execution.get("workflowId", ""))
        status      = execution.get("status", "unknown")
        started_at  = parse_dt(execution.get("startedAt"))
        finished_at = parse_dt(execution.get("stoppedAt"))
        wall_ms     = ms_between(started_at, finished_at)

        run_data: dict = (
            execution
            .get("data", {})
            .get("resultData", {})
            .get("runData", {})
        )

        wf_config  = WORKFLOW_REGISTRY.get(workflow_id, _DEFAULT_CONFIG)
        model_map  = _extract_model_map(execution)
        url_map    = _extract_http_url_map(execution)

        # Auto-detect agent nodes and LM connections; registry entries take precedence.
        auto_agent_map = _extract_agent_node_map(execution)
        auto_lm_map    = _extract_lm_subnode_map(execution)  # {agent: [lm, ...]}
        merged_agent_map = {**auto_agent_map, **wf_config["agent_node_map"]}

        # Normalise registry lm_subnode_map values to lists then merge.
        merged_lm_map: dict[str, list[str]] = {**auto_lm_map}
        for agent, val in wf_config["lm_subnode_map"].items():
            merged_lm_map[agent] = [val] if isinstance(val, str) else list(val)

        # Flatten to get the set of LM nodes already covered by agent detection.
        covered_lm_nodes = {
            lm for a in merged_agent_map if a in merged_lm_map
            for lm in merged_lm_map[a]
        }
        chain_node_map = _extract_chain_node_map(execution, exclude_lm_nodes=covered_lm_nodes)

        trigger_data = _get_trigger_data(
            run_data,
            wf_config["trigger_node"],
            wf_config["trigger_is_webhook"],
        )
        final_output = _get_final_output(run_data, wf_config["final_output_nodes"])

        exec_log = self._build_execution_log(
            exec_id, workflow_id, status, started_at, finished_at, wall_ms,
            trigger_data, final_output, run_data, wf_config, execution,
        )
        agent_calls = (
            self._build_agent_calls(exec_id, run_data, merged_agent_map, merged_lm_map, model_map)
            + self._build_chain_llm_calls(exec_id, run_data, chain_node_map, model_map)
        )
        http_requests = self._build_http_requests(exec_id, run_data, wf_config, url_map)
        flags         = self._build_flags(exec_id, run_data, wf_config)

        return {
            "execution_log":  exec_log,
            "agent_calls":    agent_calls,
            "http_requests":  http_requests,
            "workflow_flags": flags,
        }

    # ── execution_log builder ──────────────────────────────────────────────────

    def _build_execution_log(
        self, exec_id, workflow_id, status, started_at, finished_at,
        wall_ms, trigger_data, final_output, run_data, wf_config, execution,
    ) -> dict:
        wf_id = workflow_id

        # ── Prototype_v1.2 specific ────────────────────────────────────────────
        if wf_id == "CQCLdVdNwrmvI5do":
            # Classification output: {"output": {"route_to": "...", "narrative": "..."}}
            classification_raw = _get_node_output_data(
                run_data.get("Execute ClassificationWorkflow", [])
            ) or {}
            classification = classification_raw.get("output", {})

            route_to  = classification.get("route_to") or final_output.get("agentUsed")
            narrative = _truncate(classification.get("narrative"), 500)

            # Guardrail outputs are flat dicts (no "output" wrapper)
            ig_out = _get_node_output_data(run_data.get("Execute InputGuardrailWorkflow", [])) or {}
            og_out = _get_node_output_data(run_data.get("Execute OutputGuardrail", []))        or {}

            input_guardrail_triggered  = bool(ig_out.get("fail_inputGuardrail", False))
            output_guardrail_triggered = bool(og_out.get("fail_outputGuardrail", False))
            output_guardrail_nsfw      = bool(og_out.get("nsfw", False))
            output_guardrail_hallucin  = bool(og_out.get("hallucinationHarm", False))

            # replyMessage is [{type, content}] from OutputToWebhook
            raw_reply = final_output.get("replyMessage")
            if isinstance(raw_reply, list):
                ai_reply = _truncate((raw_reply or [{}])[0].get("content"), Config.MAX_RESPONSE_LENGTH)
            else:
                ai_reply = _truncate(raw_reply, Config.MAX_RESPONSE_LENGTH)

            need_staff = bool(final_output.get("staffEscalationInfo"))

            return {
                "execution_id":   exec_id,
                "workflow_id":    wf_id,
                "session_id":     trigger_data.get("sessionId"),
                "customer_id":    trigger_data.get("customerId"),
                "started_at":     started_at.isoformat() if started_at else None,
                "finished_at":    finished_at.isoformat() if finished_at else None,
                "wall_time_ms":   wall_ms,
                "user_message":   _truncate(trigger_data.get("message"), Config.MAX_PROMPT_LENGTH),
                "message_type":   trigger_data.get("messageType"),
                "ai_reply":       ai_reply,
                "reply_type":     final_output.get("type"),
                "route_to":       route_to,
                "narrative":      narrative,
                "input_guardrail_triggered":      input_guardrail_triggered,
                "output_guardrail_triggered":     output_guardrail_triggered,
                "output_guardrail_nsfw":          output_guardrail_nsfw,
                "output_guardrail_hallucination": output_guardrail_hallucin,
                "need_staff_contact":             need_staff,
                "status":         status,
                "error_message":  self._get_error(execution, status),
            }

        # ── AdvisorWorkFlow_v1.3 specific ─────────────────────────────────────
        if wf_id == "5Mr9iwbhAJZYhugN":
            agent_out = final_output.get("agentOutput", {}) or {}
            raw_reply = agent_out.get("replyMessage")
            if isinstance(raw_reply, list):
                ai_reply = _truncate((raw_reply or [{}])[0].get("content"), Config.MAX_RESPONSE_LENGTH)
            else:
                ai_reply = _truncate(raw_reply, Config.MAX_RESPONSE_LENGTH)

            need_staff = bool(final_output.get("staffEscalationInfo"))

            return {
                "execution_id":   exec_id,
                "workflow_id":    wf_id,
                "session_id":     trigger_data.get("sessionId"),
                "customer_id":    trigger_data.get("customerId"),
                "started_at":     started_at.isoformat() if started_at else None,
                "finished_at":    finished_at.isoformat() if finished_at else None,
                "wall_time_ms":   wall_ms,
                "user_message":   _truncate(trigger_data.get("userMessage"), Config.MAX_PROMPT_LENGTH),
                "message_type":   "TEXT",
                "ai_reply":       ai_reply,
                "reply_type":     agent_out.get("type"),
                "route_to":       "advisor",
                "narrative":      None,
                "input_guardrail_triggered":      False,
                "output_guardrail_triggered":     False,
                "output_guardrail_nsfw":          False,
                "output_guardrail_hallucination": False,
                "need_staff_contact":             need_staff,
                "status":         status,
                "error_message":  self._get_error(execution, status),
            }

        # ── Generic fallback ──────────────────────────────────────────────────
        return {
            "execution_id":   exec_id,
            "workflow_id":    wf_id,
            "session_id":     trigger_data.get("sessionId"),
            "customer_id":    trigger_data.get("customerId"),
            "started_at":     started_at.isoformat() if started_at else None,
            "finished_at":    finished_at.isoformat() if finished_at else None,
            "wall_time_ms":   wall_ms,
            "user_message":   _truncate(
                                  trigger_data.get("message") or trigger_data.get("userMessage"),
                                  Config.MAX_PROMPT_LENGTH,
                              ),
            "message_type":   trigger_data.get("messageType"),
            "ai_reply":       None,
            "reply_type":     None,
            "route_to":       wf_config.get("default_route"),
            "narrative":      None,
            "input_guardrail_triggered":      False,
            "output_guardrail_triggered":     False,
            "output_guardrail_nsfw":          False,
            "output_guardrail_hallucination": False,
            "need_staff_contact":             False,
            "status":         status,
            "error_message":  self._get_error(execution, status),
        }

    @staticmethod
    def _get_error(execution: dict, status: str) -> Optional[str]:
        if status == "success":
            return None
        err = execution.get("data", {}).get("resultData", {}).get("error") or {}
        return _truncate(err.get("message") or (str(err) if err else None), 500)

    # ── agent_call_log builder ─────────────────────────────────────────────────

    def _build_agent_calls(
        self, exec_id: str, run_data: dict,
        agent_node_map: dict, lm_subnode_map: dict, model_map: dict[str, str],
    ) -> list[dict]:
        rows = []

        for node_name, node_runs in run_data.items():
            if node_name not in agent_node_map:
                continue

            meta        = agent_node_map[node_name]
            lm_subnodes = lm_subnode_map.get(node_name) or []
            if isinstance(lm_subnodes, str):
                lm_subnodes = [lm_subnodes]

            for i, run in enumerate(node_runs):
                exec_time_ms  = run.get("executionTime", 0) or 0
                node_started  = parse_dt(run.get("startTime"))
                node_finished = (
                    parse_dt(run.get("startTime") + exec_time_ms)
                    if run.get("startTime") else None
                )

                # Sum token usage across ALL connected LM sub-nodes for this run.
                total_inp = total_out = 0
                for lm_name in lm_subnodes:
                    lm_runs = run_data.get(lm_name, [])
                    if i < len(lm_runs):
                        ur = _find_usage(lm_runs[i].get("data", {}))
                        if ur:
                            n = _normalize_usage(ur)
                            total_inp += n["input_tokens"]
                            total_out += n["output_tokens"]

                # Fallback: search the agent node's own run data
                if total_inp == 0 and total_out == 0:
                    ur = _find_usage(run.get("data", {}))
                    if ur:
                        n = _normalize_usage(ur)
                        total_inp, total_out = n["input_tokens"], n["output_tokens"]

                tokens = {"input_tokens": total_inp, "output_tokens": total_out,
                          "total_tokens": total_inp + total_out}
                cost       = _calc_cost(tokens)
                model_name = (
                    ", ".join(filter(None, (model_map.get(n) for n in lm_subnodes))) or None
                )

                input_data = output_data = None
                try:
                    input_data  = run["data"]["main"][0][0]["json"] if run.get("data") else None
                    output_data = run["data"]["main"][0][-1]["json"] if run.get("data") else None
                except (KeyError, IndexError, TypeError):
                    pass

                rows.append({
                    "execution_id":       exec_id,
                    "agent_name":         meta["agent"],
                    "workflow_name":      meta["workflow"],
                    "model_name":         model_name,
                    "started_at":         node_started.isoformat()  if node_started  else None,
                    "finished_at":        node_finished.isoformat() if node_finished else None,
                    "processing_time_ms": exec_time_ms,
                    "input_prompt":       _truncate(
                                              json.dumps(input_data,  ensure_ascii=False)
                                              if input_data  else None,
                                              Config.MAX_PROMPT_LENGTH,
                                          ),
                    "output_text":        _truncate(
                                              json.dumps(output_data, ensure_ascii=False)
                                              if output_data else None,
                                              Config.MAX_RESPONSE_LENGTH,
                                          ),
                    **tokens,
                    **cost,
                })

        return rows

    # ── chain LLM call builder (direct lmChat sub-nodes) ──────────────────────

    def _build_chain_llm_calls(
        self, exec_id: str, run_data: dict, chain_node_map: dict, model_map: dict[str, str]
    ) -> list[dict]:
        rows = []

        for lm_node, meta in chain_node_map.items():
            lm_runs     = run_data.get(lm_node, [])
            input_src   = meta.get("input_source")
            input_runs  = run_data.get(input_src, []) if input_src else []

            for i, lm_run in enumerate(lm_runs):
                exec_time_ms  = lm_run.get("executionTime", 0) or 0
                node_started  = parse_dt(lm_run.get("startTime"))
                node_finished = (
                    parse_dt(lm_run.get("startTime") + exec_time_ms)
                    if lm_run.get("startTime") else None
                )

                # Token usage and generated text live in data["ai_languageModel"]
                lm_json    = {}
                output_text = None
                try:
                    lm_json = lm_run["data"]["ai_languageModel"][0][0]["json"]
                    gens    = lm_json.get("response", {}).get("generations", [[]])
                    if gens and gens[0]:
                        output_text = gens[0][0].get("text")
                except (KeyError, IndexError, TypeError):
                    pass

                usage_raw = _find_usage(lm_json) or _find_usage(lm_run.get("data", {}))
                tokens    = _normalize_usage(usage_raw) if usage_raw else {
                    "input_tokens": 0, "output_tokens": 0, "total_tokens": 0
                }
                cost       = _calc_cost(tokens)
                model_name = model_map.get(lm_node)

                # Input: the data fed into the chain node from the previous node
                input_data = None
                if i < len(input_runs):
                    try:
                        input_data = input_runs[i]["data"]["main"][0][0]["json"]
                    except (KeyError, IndexError, TypeError):
                        pass

                rows.append({
                    "execution_id":       exec_id,
                    "agent_name":         meta["agent"],
                    "workflow_name":      meta["workflow"],
                    "model_name":         model_name,
                    "started_at":         node_started.isoformat()  if node_started  else None,
                    "finished_at":        node_finished.isoformat() if node_finished else None,
                    "processing_time_ms": exec_time_ms,
                    "input_prompt":       _truncate(
                                              json.dumps(input_data, ensure_ascii=False)
                                              if input_data else None,
                                              Config.MAX_PROMPT_LENGTH,
                                          ),
                    "output_text":        _truncate(output_text, Config.MAX_RESPONSE_LENGTH),
                    **tokens,
                    **cost,
                })

        return rows

    # ── http_request_log builder ───────────────────────────────────────────────

    def _build_http_requests(
        self, exec_id: str, run_data: dict, wf_config: dict, url_map: dict[str, str]
    ) -> list[dict]:
        rows = []
        http_node_map = wf_config["http_node_map"]
        req_body_source = wf_config.get("http_request_body_source")

        # Request body comes from the node that feeds into the HTTP call
        req_body_data = None
        if req_body_source and req_body_source in run_data:
            req_body_data = _get_node_output_data(run_data[req_body_source])

        for node_name, node_runs in run_data.items():
            if node_name not in http_node_map:
                continue
            meta = http_node_map[node_name]

            for run in node_runs:
                exec_time_ms = run.get("executionTime", 0) or 0
                node_started = parse_dt(run.get("startTime"))
                exec_status  = run.get("executionStatus", "")

                resp_body   = None
                resp_status = None
                success     = exec_status == "success"
                error_msg   = None

                try:
                    resp_json   = run["data"]["main"][0][0]["json"]
                    resp_status = resp_json.get("statusCode") or resp_json.get("status")
                    resp_body   = _safe_json(resp_json, Config.MAX_JSON_BODY_CHARS)
                    # If no explicit status but executionStatus is success → infer 200
                    if resp_status is None and success:
                        resp_status = 200
                except (KeyError, IndexError, TypeError):
                    pass

                if run.get("error"):
                    success   = False
                    error_msg = _truncate(str(run["error"]), 500)

                rows.append({
                    "execution_id":       exec_id,
                    "node_name":          node_name,
                    "workflow_name":      meta["workflow"],
                    "method":             "POST",
                    "url":                url_map.get(node_name),
                    "request_body":       _safe_json(req_body_data, Config.MAX_JSON_BODY_CHARS),
                    "response_status":    resp_status,
                    "response_body":      resp_body,
                    "started_at":         node_started.isoformat() if node_started else None,
                    "finished_at":        None,
                    "processing_time_ms": exec_time_ms,
                    "success":            success,
                    "error_message":      error_msg,
                })

        return rows

    # ── workflow_agent_flags builder ───────────────────────────────────────────

    def _build_flags(self, exec_id: str, run_data: dict, wf_config: dict) -> dict:
        flags: dict[str, Any] = {"execution_id": exec_id}
        workflow_id = None  # resolved below from run_data context — use wf_config name
        wf_name = wf_config.get("name", "")

        # Sub-workflow flags from SUBWORKFLOW_FLAG_MAP (Prototype_v1.2 style)
        for node_name, flag_key in wf_config["subworkflow_flag_map"].items():
            flags[flag_key] = node_name in run_data and len(run_data[node_name]) > 0

        # Ensure all required columns exist
        for col in [
            "used_input_guardrail", "used_classification", "used_advisor",
            "used_education", "used_summary", "used_output_guardrail",
            "advisor_http_call", "summary_storage_upload", "education_embedding_used",
        ]:
            if col not in flags:
                flags[col] = False

        # Workflow-specific secondary flags
        if wf_name == "Prototype_v1.2":
            # Proxy flags: derive from sub-workflow presence
            flags["advisor_http_call"]        = flags.get("used_advisor", False)
            flags["summary_storage_upload"]   = flags.get("used_summary", False)
            flags["education_embedding_used"] = flags.get("used_education", False)

        elif wf_name == "AdvisorWorkFlow_v1.3":
            # This IS the advisor execution
            flags["used_advisor"]      = True
            flags["advisor_http_call"] = "HTTP Request" in run_data and len(run_data["HTTP Request"]) > 0

        elif wf_name == "Intent_classification":
            flags["used_classification"] = True

        elif wf_name == "Input Guardrail":
            flags["used_input_guardrail"] = True

        elif wf_name == "output Guardrail":
            flags["used_output_guardrail"] = True

        elif wf_name == "Summary Workflow":
            flags["used_summary"]           = True
            flags["summary_storage_upload"] = True

        elif wf_name == "Education v1.3":
            flags["used_education"] = True
            flags["education_embedding_used"] = (
                "LLM - Retrieve" in run_data and bool(run_data["LLM - Retrieve"])
            )

        return flags
