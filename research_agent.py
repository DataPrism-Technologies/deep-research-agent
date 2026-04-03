#!/usr/bin/env python3
"""Run configured deep-research jobs and deliver results to Slack."""

from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
import traceback
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


GEMINI_API_BASE_URL = "https://generativelanguage.googleapis.com/v1beta"
DEEP_RESEARCH_AGENT = "deep-research-pro-preview-12-2025"
STRUCTURED_OUTPUT_MODEL = "gemini-2.5-flash"
POLL_INTERVAL_SECONDS = 10
MAX_POLL_SECONDS = 900
DEFAULT_HTTP_TIMEOUT_SECONDS = 180
STRUCTURED_OUTPUT_TIMEOUT_SECONDS = 300
HTTP_RETRY_COUNT = 3
HTTP_RETRY_DELAY_SECONDS = 5
DEFAULT_JOB_DIRECTORY = "jobs"
DEFAULT_PROMPT_DEFINITION_PATH = "prompts/prompt_definition.yaml"
DEFAULT_RESPONSE_SCHEMA_PATH = "schemas/research_result.schema.json"
GEMINI_31_PRO_INPUT_COST_PER_MILLION = 2.00
GEMINI_31_PRO_OUTPUT_COST_PER_MILLION = 12.00
GEMINI_25_FLASH_INPUT_COST_PER_MILLION = 0.30
GEMINI_25_FLASH_OUTPUT_COST_PER_MILLION = 2.50
GEMINI_25_FLASH_CACHE_COST_PER_MILLION = 0.03


@dataclass
class Job:
    name: str
    prompt: str
    slack_webhook_env: str
    search_queries: list[str]


class ConfigError(Exception):
    """Raised when the job configuration is invalid."""


@dataclass
class AppConfig:
    jobs: list[Job]
    deep_research_system_prompt: str
    deep_research_user_prompt: str
    structuring_system_prompt: str
    structuring_user_prompt: str
    response_schema: dict[str, Any]
    skipped_job_files: list[tuple[str, str]]


@dataclass
class CostEstimate:
    total_usd: float
    deep_research_usd: float
    structuring_usd: float
    deep_research_input_tokens: int
    deep_research_output_tokens: int
    structuring_input_tokens: int
    structuring_output_tokens: int
    note: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run configured research jobs.")
    parser.add_argument(
        "--config",
        help="Optional path to a YAML config file for overriding default paths.",
    )
    parser.add_argument(
        "--job",
        action="append",
        dest="jobs",
        help="Limit execution to one or more named jobs.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run research jobs without sending Slack notifications.",
    )
    return parser.parse_args()


def read_json_file(path: str) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            parsed = json.load(handle)
    except FileNotFoundError as exc:
        raise ConfigError(f"Referenced file not found: {path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigError(f"Failed to parse JSON file '{path}': {exc}") from exc
    if not isinstance(parsed, dict):
        raise ConfigError(f"JSON file '{path}' must contain an object at the top level.")
    return parsed


def read_prompt_definition(path: str) -> tuple[str, str, str, str]:
    try:
        with open(path, "r", encoding="utf-8") as handle:
            parsed = yaml.safe_load(handle) or {}
    except FileNotFoundError as exc:
        raise ConfigError(f"Referenced file not found: {path}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"Failed to parse YAML file '{path}': {exc}") from exc

    if not isinstance(parsed, dict):
        raise ConfigError(f"Prompt definition file '{path}' must contain a mapping.")

    deep_research = parsed.get("deep_research")
    structuring = parsed.get("structuring")
    if not isinstance(deep_research, dict) or not isinstance(structuring, dict):
        raise ConfigError(
            f"Prompt definition file '{path}' must include 'deep_research' and 'structuring' mappings."
        )
    deep_research_system_prompt = deep_research.get("system_prompt", "")
    deep_research_user_prompt = deep_research.get("user_prompt")
    structuring_system_prompt = structuring.get("system_prompt", "")
    structuring_user_prompt = structuring.get("user_prompt")
    required_values = (deep_research_user_prompt, structuring_user_prompt)
    if not all(isinstance(value, str) and value.strip() for value in required_values):
        raise ConfigError(
            f"Prompt definition file '{path}' must include non-empty user_prompt strings for deep_research and structuring."
        )
    optional_values = (deep_research_system_prompt, structuring_system_prompt)
    if not all(isinstance(value, str) for value in optional_values):
        raise ConfigError(
            f"Prompt definition file '{path}' system_prompt values must be strings when provided."
        )
    return (
        deep_research_system_prompt.strip(),
        deep_research_user_prompt.strip(),
        structuring_system_prompt.strip(),
        structuring_user_prompt.strip(),
    )


def parse_job_definition(raw_job: dict[str, Any], source_label: str) -> Job:
    name = raw_job.get("name")
    prompt = raw_job.get("prompt")
    slack_webhook_env = raw_job.get("slack_webhook_env")
    raw_search_queries = raw_job.get("search_queries", [])
    if not all(isinstance(value, str) and value.strip() for value in (name, prompt, slack_webhook_env)):
        raise ConfigError(
            f"{source_label} must include non-empty string values for "
            "'name', 'prompt', and 'slack_webhook_env'."
        )
    if raw_search_queries is None:
        search_queries: list[str] = []
    elif isinstance(raw_search_queries, list):
        search_queries = []
        for query_index, raw_query in enumerate(raw_search_queries, start=1):
            if not isinstance(raw_query, str) or not raw_query.strip():
                raise ConfigError(
                    f"{source_label} search_queries entry #{query_index} must be a non-empty string."
                )
            search_queries.append(raw_query.strip())
    else:
        raise ConfigError(f"{source_label} search_queries must be a list of strings when provided.")

    return Job(
        name=name.strip(),
        prompt=prompt.strip(),
        slack_webhook_env=slack_webhook_env.strip(),
        search_queries=search_queries,
    )


def load_jobs_from_directory(job_directory: str, selected_jobs: list[str] | None) -> tuple[list[Job], list[tuple[str, str]]]:
    job_dir = Path(job_directory)
    if not job_dir.exists():
        raise ConfigError(f"Job directory not found: {job_directory}")
    if not job_dir.is_dir():
        raise ConfigError(f"Job directory path is not a directory: {job_directory}")

    job_files = [
        path
        for path in sorted(job_dir.glob("*.yaml")) + sorted(job_dir.glob("*.yml"))
        if path.name not in {"example.yaml", "example.yml"}
    ]
    unique_job_files = sorted({path.resolve(): path for path in job_files}.values(), key=lambda path: str(path))
    if not unique_job_files:
        raise ConfigError(f"No job definition files found in directory: {job_directory}")

    jobs: list[Job] = []
    skipped_job_files: list[tuple[str, str]] = []
    seen_job_names: set[str] = set()

    for job_file in unique_job_files:
        source_label = f"Job file '{job_file}'"
        try:
            with open(job_file, "r", encoding="utf-8") as handle:
                raw_job = yaml.safe_load(handle) or {}
        except yaml.YAMLError as exc:
            skipped_job_files.append((str(job_file), f"Failed to parse YAML: {exc}"))
            continue

        if not isinstance(raw_job, dict):
            skipped_job_files.append((str(job_file), "Top level must be a mapping."))
            continue

        try:
            job = parse_job_definition(raw_job, source_label)
        except ConfigError as exc:
            skipped_job_files.append((str(job_file), str(exc)))
            continue

        if job.name in seen_job_names:
            skipped_job_files.append((str(job_file), f"Duplicate job name '{job.name}'."))
            continue

        seen_job_names.add(job.name)
        jobs.append(job)

    if selected_jobs:
        wanted = set(selected_jobs)
        jobs = [job for job in jobs if job.name in wanted]
        missing = wanted.difference({job.name for job in jobs})
        if missing:
            raise ConfigError(f"Requested job(s) not found among loadable job files: {', '.join(sorted(missing))}")
        if not jobs:
            raise ConfigError("No jobs selected.")

    if not jobs:
        raise ConfigError("No valid jobs could be loaded.")

    return jobs, skipped_job_files


def load_app_config(config_path: str | None, selected_jobs: list[str] | None) -> AppConfig:
    defaults: dict[str, Any] = {}
    if config_path:
        try:
            with open(config_path, "r", encoding="utf-8") as handle:
                raw_config = yaml.safe_load(handle) or {}
        except FileNotFoundError as exc:
            raise ConfigError(f"Config file not found: {config_path}") from exc
        except yaml.YAMLError as exc:
            raise ConfigError(f"Failed to parse YAML config: {exc}") from exc
        defaults = raw_config.get("defaults") or {}
        if not isinstance(defaults, dict):
            raise ConfigError("Config file 'defaults' must be a mapping when provided.")

    job_directory = defaults.get("job_directory", DEFAULT_JOB_DIRECTORY)
    prompt_definition_path = defaults.get("prompt_definition_path", DEFAULT_PROMPT_DEFINITION_PATH)
    response_schema_path = defaults.get("response_schema_path", DEFAULT_RESPONSE_SCHEMA_PATH)
    if not all(isinstance(value, str) and value.strip() for value in (
        job_directory,
        prompt_definition_path,
        response_schema_path,
    )):
        raise ConfigError(
            "Config defaults must define non-empty strings for job_directory, prompt_definition_path, and response_schema_path."
        )
    jobs, skipped_job_files = load_jobs_from_directory(job_directory.strip(), selected_jobs)

    (
        deep_research_system_prompt,
        deep_research_user_prompt,
        structuring_system_prompt,
        structuring_user_prompt,
    ) = read_prompt_definition(prompt_definition_path.strip())
    return AppConfig(
        jobs=jobs,
        deep_research_system_prompt=deep_research_system_prompt,
        deep_research_user_prompt=deep_research_user_prompt,
        structuring_system_prompt=structuring_system_prompt,
        structuring_user_prompt=structuring_user_prompt,
        response_schema=read_json_file(response_schema_path.strip()),
        skipped_job_files=skipped_job_files,
    )


def require_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise ConfigError(f"Required environment variable is missing: {name}")
    return value


def build_search_direction_block(job: Job) -> str:
    if not job.search_queries:
        return ""
    formatted_queries = "\n".join(f"- {query}" for query in job.search_queries)
    return (
        "\n\nSearch directions to use as starting points. Expand beyond them when useful:\n"
        f"{formatted_queries}"
    )


def build_deep_research_user_prompt(job: Job, template: str) -> str:
    return template.format(
        job_name=job.name,
        job_prompt=job.prompt,
        search_directions=build_search_direction_block(job),
    )


def build_structuring_user_prompt(job: Job, report_text: str, template: str) -> str:
    return template.format(job_name=job.name, report_text=report_text)


def make_request(
    url: str,
    payload: dict[str, Any] | None,
    headers: dict[str, str],
    method: str = "POST",
    timeout: int = DEFAULT_HTTP_TIMEOUT_SECONDS,
    retries: int = HTTP_RETRY_COUNT,
    retry_delay_seconds: int = HTTP_RETRY_DELAY_SECONDS,
) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    last_error: Exception | None = None

    for attempt in range(1, retries + 1):
        request = urllib.request.Request(url, data=data, headers=headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                body = response.read().decode("utf-8")
            break
        except urllib.error.HTTPError as exc:
            error_body = exc.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"Gemini API HTTP {exc.code}: {error_body}") from exc
        except (TimeoutError, socket.timeout) as exc:
            last_error = exc
            if attempt == retries:
                raise RuntimeError(
                    f"Gemini API request timed out after {timeout} seconds "
                    f"(attempt {attempt}/{retries})."
                ) from exc
            print(
                f"[RETRY] HTTP timeout calling Gemini API "
                f"(attempt {attempt}/{retries}, waiting {retry_delay_seconds}s)",
                file=sys.stderr,
            )
            time.sleep(retry_delay_seconds)
        except urllib.error.URLError as exc:
            last_error = exc
            if attempt == retries:
                raise RuntimeError(f"Gemini API request failed: {exc}") from exc
            print(
                f"[RETRY] Gemini API request failed "
                f"(attempt {attempt}/{retries}, waiting {retry_delay_seconds}s): {exc}",
                file=sys.stderr,
            )
            time.sleep(retry_delay_seconds)
    else:
        raise RuntimeError(f"Gemini API request failed after retries: {last_error}")

    try:
        return json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Gemini API returned invalid JSON: {exc}") from exc


def compose_prompt(system_prompt: str, user_prompt: str) -> str:
    if system_prompt:
        return f"[System Prompt]\n{system_prompt}\n\n[User Prompt]\n{user_prompt}"
    return user_prompt


def start_deep_research(api_key: str, job: Job, system_prompt: str, user_prompt_template: str) -> str:
    payload = {
        "input": compose_prompt(
            system_prompt=system_prompt,
            user_prompt=build_deep_research_user_prompt(job, user_prompt_template),
        ),
        "agent": DEEP_RESEARCH_AGENT,
        "background": True,
    }
    response = make_request(
        url=f"{GEMINI_API_BASE_URL}/interactions",
        payload=payload,
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
    )
    interaction_id = str(response.get("id", "")).strip()
    if not interaction_id:
        raise RuntimeError("Gemini Deep Research did not return an interaction id.")
    print(f"[INFO] Deep Research started for {job.name}: {interaction_id}", file=sys.stderr)
    return interaction_id


def get_interaction(api_key: str, interaction_id: str) -> dict[str, Any]:
    return make_request(
        url=f"{GEMINI_API_BASE_URL}/interactions/{interaction_id}",
        payload=None,
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
        method="GET",
        timeout=60,
    )


def extract_outputs_text(payload: dict[str, Any]) -> str:
    outputs = payload.get("outputs")
    if not isinstance(outputs, list):
        raise RuntimeError("Gemini response missing outputs list.")

    texts: list[str] = []
    for output in outputs:
        if not isinstance(output, dict):
            continue
        text = output.get("text")
        if isinstance(text, str) and text.strip():
            texts.append(text.strip())
            continue
        content = output.get("content")
        if isinstance(content, dict):
            parts = content.get("parts")
            if isinstance(parts, list):
                for part in parts:
                    if isinstance(part, dict) and isinstance(part.get("text"), str) and part["text"].strip():
                        texts.append(part["text"].strip())

    combined = "\n\n".join(texts).strip()
    if not combined:
        raise RuntimeError("Gemini response did not contain any text output.")
    return combined


def wait_for_deep_research(api_key: str, interaction_id: str) -> dict[str, Any]:
    deadline = time.monotonic() + MAX_POLL_SECONDS
    started_at = time.monotonic()
    while True:
        interaction = get_interaction(api_key=api_key, interaction_id=interaction_id)
        status = str(interaction.get("status", "")).strip().lower()
        if status == "completed":
            elapsed = int(time.monotonic() - started_at)
            print(f"[INFO] Deep Research completed in {elapsed}s: {interaction_id}", file=sys.stderr)
            return interaction
        if status in {"failed", "cancelled"}:
            raise RuntimeError(f"Gemini Deep Research finished with status '{status}'.")
        if time.monotonic() >= deadline:
            raise RuntimeError(
                f"Gemini Deep Research timed out after {MAX_POLL_SECONDS} seconds for interaction {interaction_id}."
            )
        elapsed = int(time.monotonic() - started_at)
        print(f"[INFO] Deep Research status={status or 'unknown'} elapsed={elapsed}s", file=sys.stderr)
        time.sleep(POLL_INTERVAL_SECONDS)


def call_structured_output_model(
    api_key: str,
    job: Job,
    report_text: str,
    system_prompt: str,
    user_prompt_template: str,
    response_schema: dict[str, Any],
) -> dict[str, Any]:
    payload = {
        "contents": [
            {
                "role": "user",
                "parts": [
                    {
                        "text": compose_prompt(
                            system_prompt=system_prompt,
                            user_prompt=build_structuring_user_prompt(
                                job=job,
                                report_text=report_text,
                                template=user_prompt_template,
                            ),
                        )
                    }
                ],
            }
        ],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseJsonSchema": response_schema,
        },
    }
    response = make_request(
        url=f"{GEMINI_API_BASE_URL}/models/{STRUCTURED_OUTPUT_MODEL}:generateContent",
        payload=payload,
        headers={
            "Content-Type": "application/json",
            "x-goog-api-key": api_key,
        },
        timeout=STRUCTURED_OUTPUT_TIMEOUT_SECONDS,
    )
    return response


def extract_generate_content_text(payload: dict[str, Any]) -> str:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise RuntimeError("Gemini structured output response missing candidates.")
    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        content = candidate.get("content")
        if not isinstance(content, dict):
            continue
        parts = content.get("parts")
        if not isinstance(parts, list):
            continue
        texts = [
            part["text"].strip()
            for part in parts
            if isinstance(part, dict) and isinstance(part.get("text"), str) and part["text"].strip()
        ]
        if texts:
            return "\n".join(texts)
    raise RuntimeError("Gemini structured output response contained no text parts.")


def parse_model_json(text: str) -> dict[str, Any]:
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Model response could not be parsed as JSON: {exc}") from exc

    if not isinstance(parsed, dict):
        raise RuntimeError("Model response JSON must be an object.")
    parsed.setdefault("generated_at", datetime.now(UTC).isoformat())
    parsed.setdefault("query_summary", "")
    parsed.setdefault("overall_summary", "")
    parsed.setdefault("opportunities", [])
    parsed.setdefault("watch_items", [])
    if not isinstance(parsed["opportunities"], list):
        raise RuntimeError("Model response 'opportunities' must be a list.")
    if not isinstance(parsed["watch_items"], list):
        raise RuntimeError("Model response 'watch_items' must be a list.")
    return parsed


def parse_iso_date(value: str) -> date | None:
    value = value.strip()
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def get_int_field(payload: dict[str, Any], *keys: str) -> int:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
    return 0


def estimate_deep_research_cost(interaction_payload: dict[str, Any]) -> tuple[float, int, int]:
    usage = interaction_payload.get("usage")
    if not isinstance(usage, dict):
        return 0.0, 0, 0

    input_tokens = (
        get_int_field(usage, "total_input_tokens", "totalInputTokens")
        + get_int_field(usage, "total_tool_use_tokens", "totalToolUseTokens")
    )
    output_tokens = (
        get_int_field(usage, "total_output_tokens", "totalOutputTokens")
        + get_int_field(usage, "total_thought_tokens", "totalThoughtTokens")
    )
    cost = (
        input_tokens / 1_000_000 * GEMINI_31_PRO_INPUT_COST_PER_MILLION
        + output_tokens / 1_000_000 * GEMINI_31_PRO_OUTPUT_COST_PER_MILLION
    )
    return cost, input_tokens, output_tokens


def estimate_structuring_cost(response_payload: dict[str, Any]) -> tuple[float, int, int]:
    usage = response_payload.get("usageMetadata")
    if not isinstance(usage, dict):
        usage = response_payload.get("usage_metadata")
    if not isinstance(usage, dict):
        return 0.0, 0, 0

    prompt_tokens = get_int_field(usage, "promptTokenCount", "prompt_token_count")
    cached_tokens = get_int_field(usage, "cachedContentTokenCount", "cached_content_token_count")
    candidate_tokens = get_int_field(usage, "candidatesTokenCount", "candidates_token_count")
    thought_tokens = get_int_field(usage, "thoughtsTokenCount", "thoughts_token_count")
    billable_prompt_tokens = max(prompt_tokens - cached_tokens, 0)
    output_tokens = candidate_tokens + thought_tokens
    cost = (
        billable_prompt_tokens / 1_000_000 * GEMINI_25_FLASH_INPUT_COST_PER_MILLION
        + cached_tokens / 1_000_000 * GEMINI_25_FLASH_CACHE_COST_PER_MILLION
        + output_tokens / 1_000_000 * GEMINI_25_FLASH_OUTPUT_COST_PER_MILLION
    )
    return cost, prompt_tokens, output_tokens


def build_cost_estimate(interaction_payload: dict[str, Any], structured_payload: dict[str, Any]) -> CostEstimate:
    deep_research_usd, deep_research_input_tokens, deep_research_output_tokens = estimate_deep_research_cost(
        interaction_payload
    )
    structuring_usd, structuring_input_tokens, structuring_output_tokens = estimate_structuring_cost(
        structured_payload
    )
    return CostEstimate(
        total_usd=deep_research_usd + structuring_usd,
        deep_research_usd=deep_research_usd,
        structuring_usd=structuring_usd,
        deep_research_input_tokens=deep_research_input_tokens,
        deep_research_output_tokens=deep_research_output_tokens,
        structuring_input_tokens=structuring_input_tokens,
        structuring_output_tokens=structuring_output_tokens,
        note="Approximate token-only cost. Google Search query fees are excluded.",
    )


def truncate(text: str, limit: int) -> str:
    compact = " ".join(text.split())
    if len(compact) <= limit:
        return compact
    return compact[: limit - 1].rstrip() + "…"


def first_opportunities(opportunities: list[dict[str, Any]], limit: int = 5) -> list[dict[str, Any]]:
    today = datetime.now().date()
    cleaned: list[dict[str, Any]] = []
    for item in opportunities:
        if not isinstance(item, dict):
            continue
        event_date = parse_iso_date(str(item.get("date_iso", "")))
        if event_date is not None and event_date < today:
            continue
        cleaned.append(item)
        if len(cleaned) >= limit:
            break
    return cleaned


def format_slack_date(date_iso: str, fallback_text: str) -> str:
    parsed_date = parse_iso_date(date_iso)
    if parsed_date is None:
        return fallback_text
    unix_timestamp = int(datetime(parsed_date.year, parsed_date.month, parsed_date.day, tzinfo=UTC).timestamp())
    return f"<!date^{unix_timestamp}^{{date_long_pretty}}|{fallback_text}>"


def slack_link(url: str, label: str) -> str:
    if not url:
        return label
    return f"<{url}|{label}>"


def format_opportunity_lines(item: dict[str, Any]) -> str:
    name = str(item.get("name", "無題のイベント")).strip() or "無題のイベント"
    date_text = str(item.get("date_text", "日程未定")).strip() or "日程未定"
    date_iso = str(item.get("date_iso", "")).strip()
    area = str(item.get("area", "")).strip()
    prefecture = str(item.get("prefecture", "")).strip()
    location = " / ".join(part for part in (area, prefecture) if part)
    relevance = truncate(str(item.get("why_relevant", "")).strip(), 90)
    action = truncate(str(item.get("recommended_action", "")).strip(), 70)
    source_url = str(item.get("source_url", "")).strip()
    slack_date = format_slack_date(date_iso=date_iso, fallback_text=date_text)

    title_text = slack_link(source_url, name)
    lines = [f"*{title_text}*", f"開催日: {slack_date}"]
    if location:
        lines.append(f"場所: {location}")
    if relevance:
        lines.append(f"理由: {relevance}")
    if action:
        lines.append(f"次アクション: {action}")
    return "\n".join(lines)


def format_cost_line(cost_estimate: CostEstimate) -> str:
    return (
        f"*概算コスト:* ${cost_estimate.total_usd:.3f} "
        f"(調査 ${cost_estimate.deep_research_usd:.3f} + 整形 ${cost_estimate.structuring_usd:.3f})"
    )


def build_slack_payload(job: Job, result: dict[str, Any], cost_estimate: CostEstimate | None = None) -> dict[str, Any]:
    opportunities = first_opportunities(result.get("opportunities", []))
    summary = truncate(str(result.get("overall_summary", "")).strip(), 240) or "No summary returned."

    blocks: list[dict[str, Any]] = [
        {
            "type": "header",
            "text": {"type": "plain_text", "text": f"🔎 リサーチ結果: {job.name}", "emoji": True},
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": summary},
        },
    ]

    if cost_estimate is not None:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": format_cost_line(cost_estimate) + "\n_概算です。検索課金は含みません。_",
                },
            }
        )

    blocks.extend(
        [
            {"type": "divider"},
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "📅 *有望な候補*"},
            },
        ]
    )

    if opportunities:
        for item in opportunities:
            blocks.append(
                {
                    "type": "section",
                    "text": {"type": "mrkdwn", "text": format_opportunity_lines(item)},
                }
            )
    else:
        blocks.append(
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": "_今回の実行では有望な候補は見つかりませんでした。_"},
            }
        )

    return {"text": f"リサーチ結果: {job.name}", "blocks": blocks}


def send_slack_notification(webhook_url: str, payload: dict[str, Any]) -> None:
    request = urllib.request.Request(
        webhook_url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"content-type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            body = response.read().decode("utf-8", errors="replace").strip()
            if response.status >= 400:
                raise RuntimeError(f"Slack webhook returned HTTP {response.status}: {body}")
            if body and body.lower() != "ok":
                raise RuntimeError(f"Slack webhook returned unexpected body: {body}")
    except urllib.error.HTTPError as exc:
        error_body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"Slack webhook HTTP {exc.code}: {error_body}") from exc
    except urllib.error.URLError as exc:
        raise RuntimeError(f"Slack webhook request failed: {exc}") from exc


def run_job(api_key: str, app_config: AppConfig, job: Job, dry_run: bool = False) -> None:
    interaction_id = start_deep_research(
        api_key=api_key,
        job=job,
        system_prompt=app_config.deep_research_system_prompt,
        user_prompt_template=app_config.deep_research_user_prompt,
    )
    interaction = wait_for_deep_research(api_key=api_key, interaction_id=interaction_id)
    deep_research_report = extract_outputs_text(interaction)
    print(f"[INFO] Structuring Deep Research output for {job.name}", file=sys.stderr)
    structured_payload = call_structured_output_model(
        api_key=api_key,
        job=job,
        report_text=deep_research_report,
        system_prompt=app_config.structuring_system_prompt,
        user_prompt_template=app_config.structuring_user_prompt,
        response_schema=app_config.response_schema,
    )
    structured_text = extract_generate_content_text(structured_payload)
    parsed_result = parse_model_json(structured_text)
    cost_estimate = build_cost_estimate(interaction, structured_payload)
    parsed_result["cost_estimate"] = {
        "total_usd": round(cost_estimate.total_usd, 6),
        "deep_research_usd": round(cost_estimate.deep_research_usd, 6),
        "structuring_usd": round(cost_estimate.structuring_usd, 6),
        "deep_research_input_tokens": cost_estimate.deep_research_input_tokens,
        "deep_research_output_tokens": cost_estimate.deep_research_output_tokens,
        "structuring_input_tokens": cost_estimate.structuring_input_tokens,
        "structuring_output_tokens": cost_estimate.structuring_output_tokens,
        "note": cost_estimate.note,
    }

    if dry_run:
        print(json.dumps(parsed_result, ensure_ascii=False, indent=2))
        return

    webhook_url = require_env(job.slack_webhook_env)
    slack_payload = build_slack_payload(job=job, result=parsed_result, cost_estimate=cost_estimate)
    send_slack_notification(webhook_url=webhook_url, payload=slack_payload)


def main() -> int:
    args = parse_args()
    load_dotenv(override=False)
    try:
        api_key = require_env("GEMINI_API_KEY")
        app_config = load_app_config(args.config, args.jobs)
    except ConfigError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    if app_config.skipped_job_files:
        print("Skipped invalid job files:", file=sys.stderr)
        for path, reason in app_config.skipped_job_files:
            print(f" - {path}: {reason}", file=sys.stderr)

    failures: list[tuple[str, str]] = []
    for job in app_config.jobs:
        print(f"[START] {job.name}")
        try:
            run_job(api_key=api_key, app_config=app_config, job=job, dry_run=args.dry_run)
        except Exception as exc:  # noqa: BLE001
            failures.append((job.name, str(exc)))
            print(f"[FAIL] {job.name}: {exc}", file=sys.stderr)
            traceback.print_exc()
            continue
        print(f"[OK] {job.name}")

    if failures:
        print("\nCompleted with failures:", file=sys.stderr)
        for job_name, error_message in failures:
            print(f" - {job_name}: {error_message}", file=sys.stderr)
        return 1

    print("\nAll jobs completed successfully.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
