#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import shlex
import subprocess
from pathlib import Path


DEFAULT_REVIEWER_MODEL = "gpt-5.5"
DEFAULT_REVIEWER_REASONING_EFFORT = "xhigh"
DEFAULT_REVIEWER_SERVICE_TIER = "fast"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _split_files(raw: str) -> list[str]:
    return [part for part in shlex.split(raw or "") if part]


def _build_prompt(*, root: Path, title: str, files: list[str], base: str, extra_context: str) -> str:
    brief = (root / "docs" / "INDEPENDENT_REVIEW_BRIEF.md").read_text(encoding="utf-8")
    file_list = "\n".join(f"- `{path}`" for path in files) if files else "- Review current uncommitted diff."
    return (
        f"{brief}\n\n"
        "## Review Scope\n\n"
        f"Title: {title or 'Independent review'}\n\n"
        f"Base/ref: {base or 'current working tree'}\n\n"
        "Files or scope:\n"
        f"{file_list}\n\n"
        "Relevant repository contracts to check first:\n"
        "- `AGENTS.md`\n"
        "- `docs/PRE_AGENT_CONTRACT_REVIEW.md`\n"
        "- `docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`\n"
        "- `docs/INDEPENDENT_REVIEW_GATE.md`\n\n"
        f"Additional context:\n{extra_context.strip() or 'None.'}\n\n"
        "Run read-only inspection commands as needed. Do not edit files. "
        "This is a scoped independent review, not a full development-session resume: use targeted "
        "`git diff -- <files>`, `rg`, and line-range reads for the listed scope and relevant Contract "
        "sections. Do not read full PROGRESS.md, full docs/NEXT_TODO.md, or full long Contract files "
        "unless the review scope explicitly requires that full context. Prioritize blocking correctness, "
        "contract, provider-cost, fallback, migration, and runtime risks."
    )


def _artifact_header(
    *,
    title: str,
    files: list[str],
    base: str,
    model: str,
    reasoning_effort: str,
    service_tier: str,
    timeout_seconds: int,
    prompt_path: Path,
    shell_command: str,
) -> str:
    scope = "\n".join(f"- `{path}`" for path in files) if files else "- Current uncommitted diff."
    return (
        "## Review Metadata\n\n"
        f"- title: {title or 'Independent review'}\n"
        f"- base/ref: {base or 'current working tree'}\n"
        f"- reviewer_model: {model or 'default'}\n"
        f"- reviewer_reasoning_effort: {reasoning_effort or 'default'}\n"
        f"- reviewer_service_tier: {service_tier or 'default'}\n"
        f"- timeout_seconds: {timeout_seconds}\n"
        f"- prompt_path: `{prompt_path}`\n"
        f"- command: `{shell_command}`\n"
        "- contract_docs_considered: `AGENTS.md`, `docs/PRE_AGENT_CONTRACT_REVIEW.md`, "
        "`docs/DURABLE_EXECUTION_RUNTIME_CONTRACT.md`, `docs/INDEPENDENT_REVIEW_GATE.md`\n"
        "- author_validation: see latest assistant handoff/progress note and targeted test output for this run\n"
        "- accepted_exceptions: none recorded by this runner\n"
        "\nReviewed scope:\n"
        f"{scope}\n\n"
        "## Reviewer Output\n\n"
    )


def _is_explicit_codex_value(value: str) -> bool:
    return bool(value and value.lower() not in {"default", "auto", "inherit"})


def _build_codex_args(
    *,
    root: Path,
    output_path: Path,
    model: str,
    reasoning_effort: str,
    service_tier: str,
) -> list[str]:
    codex_args = [
        "codex",
        "exec",
        "--cd",
        str(root),
        "--sandbox",
        "read-only",
    ]
    if _is_explicit_codex_value(model):
        codex_args.extend(["--model", model])
    if _is_explicit_codex_value(service_tier):
        codex_args.extend(["-c", f'service_tier="{service_tier}"'])
    if _is_explicit_codex_value(reasoning_effort):
        codex_args.extend(["-c", f'model_reasoning_effort="{reasoning_effort}"'])
    codex_args.extend(
        [
            "--output-last-message",
            str(output_path),
            "-",
        ]
    )
    return codex_args


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate or execute the independent review gate prompt.")
    parser.add_argument("--title", default="", help="Review title.")
    parser.add_argument("--files", default="", help="Shell-style space-separated file list.")
    parser.add_argument("--base", default="", help="Optional base branch/ref for reviewer context.")
    parser.add_argument("--extra-context", default="", help="Additional review context.")
    parser.add_argument("--model", default=DEFAULT_REVIEWER_MODEL, help="Codex reviewer model.")
    parser.add_argument(
        "--reasoning-effort",
        default=DEFAULT_REVIEWER_REASONING_EFFORT,
        help="Codex model_reasoning_effort config value; use default/auto/inherit to omit.",
    )
    parser.add_argument(
        "--service-tier",
        default=DEFAULT_REVIEWER_SERVICE_TIER,
        help="Codex service_tier config value; fast is the project default. Use default/auto/inherit to omit.",
    )
    parser.add_argument("--timeout-seconds", type=int, default=420, help="Hard timeout for --execute.")
    parser.add_argument("--output", default="", help="Review output file.")
    parser.add_argument("--prompt-output", default="", help="Prompt output file for dry-run/use elsewhere.")
    parser.add_argument("--execute", action="store_true", help="Run codex exec in read-only mode.")
    args = parser.parse_args()

    root = _repo_root()
    files = _split_files(args.files)
    prompt = _build_prompt(
        root=root,
        title=args.title,
        files=files,
        base=args.base,
        extra_context=args.extra_context,
    )
    review_dir = root / "runtime" / "reviews"
    review_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_title = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in (args.title or "independent_review"))[:80]
    output_path = Path(args.output) if args.output else review_dir / f"{stamp}_{safe_title}.md"
    prompt_path = Path(args.prompt_output) if args.prompt_output else review_dir / f"{stamp}_{safe_title}.prompt.md"
    prompt_path.write_text(prompt, encoding="utf-8")

    timeout_seconds = max(60, int(args.timeout_seconds or 420))
    codex_args = _build_codex_args(
        root=root,
        output_path=output_path,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
        service_tier=args.service_tier,
    )
    shell_command = " ".join(shlex.quote(part) for part in codex_args) + f" < {shlex.quote(str(prompt_path))}"
    if not args.execute:
        print(f"prompt_written={prompt_path}")
        print(f"review_output={output_path}")
        print(f"python_timeout_seconds={timeout_seconds}")
        print(f"reviewer_model={args.model or 'default'}")
        print(f"reviewer_reasoning_effort={args.reasoning_effort or 'default'}")
        print(f"reviewer_service_tier={args.service_tier or 'default'}")
        print("execute_command=" + shell_command)
        return 0

    try:
        completed = subprocess.run(
            ["bash", "-lc", shell_command],
            cwd=root,
            stdin=subprocess.DEVNULL,
            text=True,
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired:
        timeout_note = (
            f"\n\nNO-GO: independent review timed out after {timeout_seconds} seconds. "
            "Increase REVIEW_TIMEOUT_SECONDS or reduce REVIEW_FILES scope; do not treat this as review evidence.\n"
        )
        output_path.write_text(timeout_note, encoding="utf-8")
        print(f"prompt_written={prompt_path}")
        print(f"review_output={output_path}")
        return 124
    print(f"prompt_written={prompt_path}")
    print(f"review_output={output_path}")
    review_body = output_path.read_text(encoding="utf-8").strip() if output_path.exists() else ""
    header = _artifact_header(
        title=args.title,
        files=files,
        base=args.base,
        model=args.model,
        reasoning_effort=args.reasoning_effort,
        service_tier=args.service_tier,
        timeout_seconds=timeout_seconds,
        prompt_path=prompt_path,
        shell_command=shell_command,
    )
    if not review_body:
        output_path.write_text(
            header + "NO-GO: reviewer produced no output; do not treat this as review evidence.\n",
            encoding="utf-8",
        )
        return 2
    if review_body == "NO-GO":
        invalid_note = (
            "\n\nINVALID_REVIEW_ARTIFACT: bare NO-GO without at least one prioritized finding. "
            "Rerun with narrower scope or clearer prompt; do not treat this as review evidence.\n"
        )
        output_path.write_text(header + review_body + invalid_note, encoding="utf-8")
        return 2
    output_path.write_text(header + review_body, encoding="utf-8")
    return int(completed.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
