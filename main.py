#!/usr/bin/env python3
"""
Main orchestration script for GenerarGrafoPetersen project.

This script:
1. Loads environment configuration
2. Wipes the Neo4j database
3. Runs the FlowToGraph pipeline
4. Runs the Migracion GP pipeline

Designed to be run via: bash /opt/GenerarGrafoGP/main.sh
"""

import os
import sys
import logging
import smtplib
import traceback
from enum import Enum
from email.mime.text import MIMEText
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from neo4j import GraphDatabase

# Load environment configuration FIRST, before importing subproject modules
env_file = Path(__file__).parent / ".env"
if not env_file.exists():
    print(f"ERROR: .env file not found at {env_file}")
    sys.exit(1)
load_dotenv(env_file, override=True)

# Add subprojects to path
sys.path.insert(0, str(Path(__file__).parent / "FlowToGraph"))
sys.path.insert(0, str(Path(__file__).parent / "Migracion GP"))

# Import subproject modules (static analysis warnings are expected - imports work at runtime)
from flow_pipeline import fetch_and_process_all_flows  # type: ignore
from config import Config  # type: ignore
from src.pipeline import run_migration  # type: ignore


# Configure logging
LOG_DIR = Path(__file__).parent / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"orchestrator_{timestamp}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(sys.stdout)
    ]
)


log = logging.getLogger(__name__)


def load_env_config() -> dict:
    """Validate environment configuration (already loaded by dotenv at startup)."""
    config = {
        "neo4j_host": os.getenv("NEO4J_HOST"),
        "neo4j_user": os.getenv("NEO4J_USER"),
        "neo4j_pass": os.getenv("NEO4J_PASS"),
    }
    
    if not all(config.values()):
        log.error("ERROR: Missing required environment variables (NEO4J_HOST, NEO4J_USER, NEO4J_PASS)")
        sys.exit(1)
    
    log.info(f"=== Neo4j target: {config['neo4j_host']} ===")
    return config


def wipe_neo4j_database(config: dict) -> None:
    """Wipe all nodes and relationships from the Neo4j database."""
    log.info("")
    log.info("=== Wiping Neo4j database ===")
    
    try:
        
        driver = GraphDatabase.driver(
            config["neo4j_host"],
            auth=(config["neo4j_user"], config["neo4j_pass"])
        )
        
        with driver.session() as session:
            result = session.run("MATCH (n) DETACH DELETE n")
            summary = result.consume()
            
            nodes_deleted = summary.counters.nodes_deleted
            rels_deleted = summary.counters.relationships_deleted
            
            log.info(
                f"Database wiped — {nodes_deleted} nodes deleted, "
                f"{rels_deleted} relationships deleted."
            )
        
        driver.close()
        
    except Exception as e:
        log.error(f"Failed to wipe Neo4j database: {e}")
        sys.exit(1)


def run_flow_to_graph_pipeline() -> dict:
    """
    Run the FlowToGraph pipeline by calling its main function.
    The FlowToGraph pipeline handles fetching and processing all flows.

    Returns a dict with:
        fetch_ok    – cluster names retrieved successfully
        fetch_fail  – cluster names that failed retrieval
        process_ok  – flow file names processed successfully
        process_fail – flow file names that failed processing
    """
    log.info("")
    log.info("=== Running FlowToGraph Pipeline ===")

    _empty = {"fetch_ok": [], "fetch_fail": [], "process_ok": [], "process_fail": []}

    try:
        # Create default configuration
        class Args:
            def __init__(self):
                self.allowed_rels = None  # Will use defaults
                self.not_allowed_rels = None
                self.logging_name_regex = None
                self.logging_types = None
                self.no_enforce_allowed = False
                self.stop_on_ua_override = False

        args = Args()
        cfg = Config.from_args(args)

        # Fetch and process all flows
        result = fetch_and_process_all_flows(cfg, verbose=False)

        fetch_ok = result["fetch_ok"]
        fetch_fail = result["fetch_fail"]
        process_ok = result["process_ok"]
        process_fail = result["process_fail"]

        log.info("=== FlowToGraph pipeline finished ===")
        log.info("  Fetched successfully  (%d): %s", len(fetch_ok), ", ".join(fetch_ok) or "—")
        log.info("  Fetch failed          (%d): %s", len(fetch_fail), ", ".join(fetch_fail) or "—")
        log.info("  Processed successfully(%d): %s", len(process_ok), ", ".join(process_ok) or "—")
        log.info("  Processing failed     (%d): %s", len(process_fail), ", ".join(process_fail) or "—")

        return result

    except Exception as e:
        log.error(f"FlowToGraph pipeline failed: {e}", exc_info=True)
        return _empty


def run_migracion_gp_pipeline() -> dict:
    """
    Run the Migracion GP pipeline to migrate Atlas terms to Neo4j.

    Returns a dict with three keys:
        ok        – terms loaded successfully
        not_found – terms not found in Atlas or GitLab
        failed    – terms that raised an exception during processing
    An empty dict signals a hard failure (pipeline could not run at all).
    """
    log.info("")
    log.info("=== Running Migracion GP Pipeline ===")

    try:
        csv_file = Path(__file__).parent / "data" / "atlas_terms.csv"

        result = run_migration(csv_file)

        log.info("=== Migracion GP Pipeline finished ===")
        log.info("  Loaded successfully  (%d)", len(result["ok"]))
        log.info("  Not found           (%d)", len(result["not_found"]))
        log.info("  Failed              (%d): %s", len(result["failed"]), ", ".join(result["failed"]) or "—")

        return result

    except Exception as e:
        log.error(f"Migracion GP pipeline failed: {e}", exc_info=True)
        return {}


# ---------------------------------------------------------------------------
# Error-level classification
# ---------------------------------------------------------------------------

_WARN_THRESHOLD     = 0.05   # Atlas term failure rate > 5%  → WARNING
_ERROR_THRESHOLD    = 0.15   # Atlas term failure rate > 15% → ERROR
_CRITICAL_THRESHOLD = 0.30   # Atlas term failure rate > 30% → CRITICAL


class ErrorLevel(Enum):
    NO_ERROR = 0
    WARNING  = 1
    ERROR    = 2
    CRITICAL = 3

    def label(self) -> str:
        return {
            ErrorLevel.NO_ERROR: "✔ NO ERROR",
            ErrorLevel.WARNING:  "⚠ WARNING",
            ErrorLevel.ERROR:    "✘ ERROR",
            ErrorLevel.CRITICAL: "✘ CRITICAL ERROR",
        }[self]


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

def send_summary_email(
    flow_result: dict,
    flow_success: bool,
    migration_result: dict,
    migration_success: bool,
    error_level: ErrorLevel,
) -> None:
    """Send a plain-text summary email with pipeline results.

    SMTP connection details and recipient are read from environment variables:
        SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD,
        EMAIL_FROM, EMAIL_TO (comma-separated for multiple recipients).

    Failures are logged as warnings and never crash the script.
    """
    flow_fetch_ok     = flow_result.get("fetch_ok", [])
    flow_fetch_fail   = flow_result.get("fetch_fail", [])
    flow_process_ok   = flow_result.get("process_ok", [])
    flow_process_fail = flow_result.get("process_fail", [])
    mg_ok             = migration_result.get("ok", [])
    mg_not_found      = migration_result.get("not_found", [])
    mg_failed         = migration_result.get("failed", [])

    subject = f"[GenerarGrafoPetersen] {error_level.label()} \u2014 {timestamp}"

    body_lines = [
        f"GenerarGrafoPetersen pipeline run: {timestamp}",
        f"Overall status: {error_level.label()}",
        "",
        "=" * 60,
        "FlowToGraph:",
        f"  Retrieved successfully   ({len(flow_fetch_ok)}): {', '.join(flow_fetch_ok) or '\u2014'}",
        f"  Retrieval failed         ({len(flow_fetch_fail)}): {', '.join(flow_fetch_fail) or '\u2014'}",
        f"  Processed successfully   ({len(flow_process_ok)}): {', '.join(flow_process_ok) or '\u2014'}",
        f"  Processing failed        ({len(flow_process_fail)}): {', '.join(flow_process_fail) or '\u2014'}",
        f"  Overall: {'\u2714 SUCCESS' if flow_success else '\u2718 FAILED'}",
        "",
        "Migracion GP:",
        f"  Loaded successfully      ({len(mg_ok)})",
        f"  Not found                ({len(mg_not_found)})",
        f"  Failed                   ({len(mg_failed)}): {', '.join(mg_failed) or '\u2014'}",
        f"  Overall: {'\u2714 SUCCESS' if migration_success else '\u2718 FAILED'}",
        "=" * 60,
        "",
        f"Log file: {log_file}",
    ]

    _send_email(subject, "\n".join(body_lines))


def send_crash_email(exc: BaseException) -> None:
    """Send a critical-error email when an uncaught exception terminates the orchestrator."""
    subject = f"[GenerarGrafoPetersen] \u2718 CRITICAL ERROR \u2014 {timestamp}"

    body_lines = [
        f"GenerarGrafoPetersen pipeline run: {timestamp}",
        f"Overall status: {ErrorLevel.CRITICAL.label()}",
        "",
        "The orchestrator terminated with an unhandled exception:",
        "",
        f"  {type(exc).__name__}: {exc}",
        "",
        "Traceback:",
        traceback.format_exc(),
        "=" * 60,
        f"Log file: {log_file}",
    ]

    _send_email(subject, "\n".join(body_lines))


def _send_email(subject: str, body: str) -> None:
    """Low-level helper: build and dispatch one email via STARTTLS SMTP.

    All credentials are read from environment variables. Errors are logged
    as warnings so they never crash the caller.
    """
    smtp_host = os.getenv("SMTP_HOST")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    smtp_user = os.getenv("SMTP_USERNAME")
    smtp_pass = os.getenv("SMTP_PASSWORD")
    email_from = os.getenv("EMAIL_FROM")
    email_to = [a.strip() for a in os.getenv("EMAIL_TO", "").split(",") if a.strip()]

    if not all([smtp_host, smtp_user, smtp_pass, email_from, email_to]):
        log.warning("Email notification skipped: missing SMTP/email environment variables.")
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"]    = email_from
    msg["To"]      = ", ".join(email_to)

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.starttls()
            server.login(smtp_user, smtp_pass)
            server.sendmail(email_from, email_to, msg.as_string())
        log.info("Email sent to: %s", ", ".join(email_to))
    except Exception as send_exc:
        log.warning("Failed to send email: %s", send_exc)


# ---------------------------------------------------------------------------
# Final state verification
# ---------------------------------------------------------------------------

def verify_final_state(flow_result: dict, migration_result: dict) -> ErrorLevel:
    """Evaluate both pipeline results, classify the error level, and send the summary email.

    Atlas term failure rate thresholds (failed / total attempted):
        >  5% → WARNING
        > 15% → ERROR     (also triggered by any flow fetch failure)
        > 30% → CRITICAL  (also triggered by any flow process failure)

    The highest triggered level wins. Returns the computed ErrorLevel.
    """
    mg_ok             = migration_result.get("ok", [])
    mg_not_found      = migration_result.get("not_found", [])
    mg_failed         = migration_result.get("failed", [])
    flow_fetch_fail   = flow_result.get("fetch_fail", [])
    flow_process_fail = flow_result.get("process_fail", [])

    # Atlas term failure rate
    mg_total     = len(mg_ok) + len(mg_not_found) + len(mg_failed)
    mg_fail_rate = len(mg_failed) / mg_total if mg_total > 0 else 0.0

    # Evaluate from lowest to highest severity; last match wins
    level = ErrorLevel.NO_ERROR

    if mg_fail_rate > _WARN_THRESHOLD:
        level = ErrorLevel.WARNING

    if mg_fail_rate > _ERROR_THRESHOLD or flow_fetch_fail:
        level = ErrorLevel.ERROR

    if mg_fail_rate > _CRITICAL_THRESHOLD or flow_process_fail:
        level = ErrorLevel.CRITICAL

    # Log the assessment
    log.info("")
    log.info("=== Final state: %s ===", level.label())
    if mg_total > 0:
        log.info(
            "    Atlas term failure rate: %.1f%% (%d failed / %d total)",
            mg_fail_rate * 100, len(mg_failed), mg_total,
        )
    if flow_fetch_fail:
        log.info("    Flow fetch failures: %s", ", ".join(flow_fetch_fail))
    if flow_process_fail:
        log.info("    Flow process failures: %s", ", ".join(flow_process_fail))

    flow_success      = not flow_fetch_fail and not flow_process_fail
    migration_success = bool(migration_result) and not mg_failed

    send_summary_email(
        flow_result=flow_result,
        flow_success=flow_success,
        migration_result=migration_result,
        migration_success=migration_success,
        error_level=level,
    )

    return level


def main():
    """Main orchestration function."""
    try:
        _main()
    except Exception as exc:
        log.critical("Unhandled exception — orchestrator terminated.", exc_info=True)
        send_crash_email(exc)
        sys.exit(1)


def _main():
    """Inner orchestration logic, wrapped by main() for top-level error handling."""
    log.info("=" * 70)
    log.info("=== GenerarGrafoPetersen Orchestrator Started ===")
    log.info(f"=== Log file: {log_file} ===")
    log.info("=" * 70)

    # 1. Load environment configuration
    config = load_env_config()

    # 2. Wipe Neo4j database
    wipe_neo4j_database(config)

    # 3. Run FlowToGraph pipeline
    flow_result = run_flow_to_graph_pipeline()
    flow_fetch_ok    = flow_result["fetch_ok"]
    flow_fetch_fail  = flow_result["fetch_fail"]
    flow_process_ok  = flow_result["process_ok"]
    flow_process_fail = flow_result["process_fail"]
    flow_success = not flow_fetch_fail and not flow_process_fail

    # 4. Run Migracion GP pipeline
    migration_result  = run_migracion_gp_pipeline()
    mg_ok        = migration_result.get("ok", [])
    mg_not_found = migration_result.get("not_found", [])
    mg_failed    = migration_result.get("failed", [])
    migration_success = bool(migration_result) and not mg_failed

    # 5. Summary
    log.info("")
    log.info("=" * 70)
    log.info("=== Pipeline Summary ===")
    log.info("")
    log.info("FlowToGraph:")
    log.info("  Retrieved successfully  (%d): %s", len(flow_fetch_ok),    ", ".join(flow_fetch_ok)    or "—")
    log.info("  Retrieval failed        (%d): %s", len(flow_fetch_fail),  ", ".join(flow_fetch_fail)  or "—")
    log.info("  Processed successfully  (%d): %s", len(flow_process_ok),  ", ".join(flow_process_ok)  or "—")
    log.info("  Processing failed       (%d): %s", len(flow_process_fail),", ".join(flow_process_fail) or "—")
    log.info("  Overall: %s", "✔ SUCCESS" if flow_success else "✘ FAILED")
    log.info("")
    log.info("Migracion GP:")
    log.info("  Loaded successfully     (%d)", len(mg_ok))
    log.info("  Not found               (%d)", len(mg_not_found))
    log.info("  Failed                  (%d): %s", len(mg_failed), ", ".join(mg_failed) or "—")
    log.info("  Overall: %s", "✔ SUCCESS" if migration_success else "✘ FAILED")
    log.info("=" * 70)
    log.info("=== All done ===")

    # 6. Verify final state (classifies severity and sends summary email)
    level = verify_final_state(flow_result=flow_result, migration_result=migration_result)

    # Exit with error code for anything above WARNING
    if level.value >= ErrorLevel.ERROR.value:
        sys.exit(1)


if __name__ == "__main__":
    main()
