#!/usr/bin/env python3
"""
Main orchestration script for GenerarGrafoPetersen project.

This script:
1. Loads environment configuration
2. Wipes the Neo4j database
3. Runs the FlowToGraph pipeline
4. Runs the TermToGraph pipeline

Designed to be run via: bash /opt/GenerarGrafoGP/main.sh
"""

import os
import sys
import atexit
import logging
import smtplib
import traceback
from enum import Enum
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
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
sys.path.insert(0, str(Path(__file__).parent / "TermToGraph"))
sys.path.insert(0, str(Path(__file__).parent))

# Import subproject modules (static analysis warnings are expected - imports work at runtime)
from flow_pipeline import fetch_and_process_all_flows  # type: ignore
from config import Config  # type: ignore
from src.pipeline import run_migration  # type: ignore
from OrigenToGraph import run_origen_pipeline  # type: ignore


# Configure logging
LOG_DIR = Path(__file__).parent / "log"
LOG_DIR.mkdir(parents=True, exist_ok=True)

timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_file = LOG_DIR / f"orchestrator_{timestamp}.log"

# Create file handler with explicit encoding and flush
file_handler = logging.FileHandler(log_file, encoding='utf-8')
file_handler.setLevel(logging.DEBUG)
file_handler.flush()

# Create stream handler for console
stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setLevel(logging.INFO)

# Configure formatter
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
file_handler.setFormatter(formatter)
stream_handler.setFormatter(formatter)

# Configure root logger
logging.basicConfig(
    level=logging.DEBUG,
    handlers=[file_handler, stream_handler]
)

# Ensure logs are flushed on exit
def _flush_logs():
    """Flush all logging handlers before exit."""
    for handler in logging.root.handlers:
        handler.flush()
        if hasattr(handler, 'close'):
            handler.close()

atexit.register(_flush_logs)
logging.shutdown = lambda: _flush_logs()


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


def run_origen_to_graph_pipeline() -> dict:
    """
    Run the OrigenToGraph pipeline to classify every Tabla node in Neo4j
    with an `origen` attribute: "Microservicios", "DIS", or
    "Proceso no estándar".

    Returns a dict with keys:
        microservicios  – nodes tagged "Microservicios"
        dis             – nodes tagged "DIS"
        no_estandar     – nodes tagged "Proceso no estándar"
        total           – total Tabla nodes processed
        errors          – list of error message strings
    An empty dict signals a hard failure.
    """
    log.info("")
    log.info("=== Running OrigenToGraph Pipeline ===")

    try:
        result = run_origen_pipeline()

        log.info("=== OrigenToGraph pipeline finished ===")
        log.info("  Total Tabla nodes   : %d", result.get("total", 0))
        log.info("  Microservicios      : %d", result.get("microservicios", 0))
        log.info("  DIS                 : %d", result.get("dis", 0))
        log.info("  Proceso no estándar : %d", result.get("no_estandar", 0))
        if result.get("errors"):
            log.warning("  Errors (%d): %s", len(result["errors"]), "; ".join(result["errors"]))

        return result

    except Exception as e:
        log.error(f"OrigenToGraph pipeline failed: {e}", exc_info=True)
        return {}


def run_term_to_graph_pipeline() -> dict:
    """
    Run the TermToGraph pipeline to migrate Atlas terms to Neo4j.

    Returns a dict with three keys:
        ok        – terms loaded successfully
        not_found – terms not found in Atlas or GitLab
        failed    – terms that raised an exception during processing
    An empty dict signals a hard failure (pipeline could not run at all).
    """
    log.info("")
    log.info("=== Running TermToGraph Pipeline ===")

    try:
        csv_file = Path(__file__).parent / "data" / "atlas_terms.csv"

        result = run_migration(csv_file)

        log.info("=== TermToGraph Pipeline finished ===")
        log.info("  Loaded successfully  (%d)", len(result["ok"]))
        log.info("  Not found           (%d)", len(result["not_found"]))
        log.info("  Failed              (%d): %s", len(result["failed"]), ", ".join(result["failed"]) or "—")

        return result

    except Exception as e:
        log.error(f"TermToGraph pipeline failed: {e}", exc_info=True)
        return {}


# ---------------------------------------------------------------------------
# Error-level classification
# ---------------------------------------------------------------------------

_WARN_THRESHOLD     = 0.05   # Atlas term failure rate > 5%  → WARNING
_ERROR_THRESHOLD    = 0.15   # Atlas term failure rate > 15% → ERROR
_CRITICAL_THRESHOLD = 0.30   # Atlas term failure rate > 30% → CRITICAL

# Unicode symbols
_EM_DASH = "—"
_CHECK   = "✔"
_CROSS   = "✘"
_WARN    = "⚠"


class ErrorLevel(Enum):
    NO_ERROR = 0
    WARNING  = 1
    ERROR    = 2
    CRITICAL = 3

    def label(self) -> str:
        return {
            ErrorLevel.NO_ERROR: f"{_CHECK} NO ERROR",
            ErrorLevel.WARNING:  f"{_WARN} WARNING",
            ErrorLevel.ERROR:    f"{_CROSS} ERROR",
            ErrorLevel.CRITICAL: f"{_CROSS} CRITICAL ERROR",
        }[self]


# ---------------------------------------------------------------------------
# Email
# ---------------------------------------------------------------------------

class SMTPConnection:
    """Reusable SMTP connection with optional TLS and auto-reconnect on failure.

    All parameters are read from environment variables when not supplied:
        SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD,
        SMTP_TLS (true/false, default false),
        EMAIL_FROM, EMAIL_TO (comma-separated).
    """

    def __init__(self, host, username, password, sender_email,
                 receiver_emails: list | None = None, port=587, tls=False):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.sender_email = sender_email
        self.receiver_emails = receiver_emails
        self.server = None
        self.retries = 0
        self.tls = tls

    def initialize_server(self):
        self.server = smtplib.SMTP(self.host, self.port)
        if self.tls:
            try:
                self.server.starttls()
            except Exception as e:
                log.error("Error starting TLS: %s", e)
        self.server.login(self.username, self.password)

    def disconnect(self):
        if self.server is not None:
            self.server.quit()
            self.server = None

    def send_email(self, subject: str, body: str,
                   recipient_emails: list | None = None, html_type=False):
        try:
            if self.server is None:
                self.initialize_server()
            recipients = recipient_emails or self.receiver_emails

            msg = MIMEMultipart()
            msg["From"]    = self.sender_email
            msg["To"]      = ", ".join(recipients)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "html" if html_type else "plain", "utf-8"))

            self.server.sendmail(self.sender_email, recipients, msg.as_string())
            log.info("Email sent to: %s", ", ".join(recipients))
            self.retries = 0

        except Exception as exc:
            log.error("Error sending email: %s", exc)
            log.debug(traceback.format_exc())
            if self.retries == 0:
                self.retries += 1
                self.initialize_server()
                self.send_email(subject, body, recipient_emails, html_type)
            else:
                self.retries = 0
                log.warning("Failed to send email after retry: %s", exc)


def _build_smtp_connection() -> SMTPConnection | None:
    """Instantiate an SMTPConnection from environment variables, or return None
    if any required variable is missing."""
    host      = os.getenv("SMTP_HOST")
    port      = int(os.getenv("SMTP_PORT", "587"))
    username  = os.getenv("SMTP_USERNAME")
    password  = os.getenv("SMTP_PASSWORD")
    tls       = os.getenv("SMTP_TLS", "false").strip().lower() == "true"
    sender    = os.getenv("EMAIL_FROM")
    receivers = [a.strip() for a in os.getenv("EMAIL_TO", "").split(",") if a.strip()]

    if not all([host, username, password, sender, receivers]):
        log.warning("Email notification skipped: missing SMTP/email environment variables.")
        return None

    return SMTPConnection(
        host=host, port=port, username=username, password=password,
        sender_email=sender, receiver_emails=receivers, tls=tls,
    )


def _send_email(subject: str, body: str) -> None:
    """Build a one-shot SMTP connection and send a single email."""
    conn = _build_smtp_connection()
    if conn is None:
        return
    try:
        conn.initialize_server()
        conn.send_email(subject, body)
    finally:
        conn.disconnect()


def send_summary_email(
    flow_result: dict,
    flow_success: bool,
    migration_result: dict,
    migration_success: bool,
    origen_result: dict,
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
    or_total          = origen_result.get("total", 0)
    or_ms             = origen_result.get("microservicios", 0)
    or_dis            = origen_result.get("dis", 0)
    or_ne             = origen_result.get("no_estandar", 0)
    or_errors         = origen_result.get("errors", [])

    subject = f"[GenerarGrafoPetersen] {error_level.label()} {_EM_DASH} {timestamp}"

    body_lines = [
        f"GenerarGrafoPetersen pipeline run: {timestamp}",
        f"Overall status: {error_level.label()}",
        "",
        "=" * 60,
        "FlowToGraph:",
        f"  Retrieved successfully   ({len(flow_fetch_ok)}): {', '.join(flow_fetch_ok) or _EM_DASH}",
        f"  Retrieval failed         ({len(flow_fetch_fail)}): {', '.join(flow_fetch_fail) or _EM_DASH}",
        f"  Processed successfully   ({len(flow_process_ok)}): {', '.join(flow_process_ok) or _EM_DASH}",
        f"  Processing failed        ({len(flow_process_fail)}): {', '.join(flow_process_fail) or _EM_DASH}",
        f"  Overall: {_CHECK + ' SUCCESS' if flow_success else _CROSS + ' FAILED'}",
        "",
        "TermToGraph:",
        f"  Loaded successfully      ({len(mg_ok)})",
        f"  Not found                ({len(mg_not_found)})",
        f"  Failed                   ({len(mg_failed)}): {', '.join(mg_failed) or _EM_DASH}",
        f"  Overall: {_CHECK + ' SUCCESS' if migration_success else _CROSS + ' FAILED'}",
        "",
        "OrigenToGraph:",
        f"  Total Tabla nodes        ({or_total})",
        f"  Microservicios           ({or_ms})",
        f"  DIS                      ({or_dis})",
        f"  Proceso no estándar      ({or_ne})",
        f"  Errors                   ({len(or_errors)}): {'; '.join(or_errors) or _EM_DASH}",
        f"  Overall: {_CHECK + ' SUCCESS' if not or_errors else _CROSS + ' FAILED (partial)'}",
        "=" * 60,
        "",
        f"Log file: {log_file}",
    ]

    _send_email(subject, "\n".join(body_lines))


def send_crash_email(exc: BaseException) -> None:
    """Send a critical-error email when an uncaught exception terminates the orchestrator."""
    subject = f"[GenerarGrafoPetersen] {_CROSS} CRITICAL ERROR {_EM_DASH} {timestamp}"

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


# ---------------------------------------------------------------------------
# Final state verification
# ---------------------------------------------------------------------------

def verify_final_state(
    flow_result: dict,
    migration_result: dict,
    origen_result: dict,
) -> ErrorLevel:
    """Evaluate all pipeline results, classify the error level, and send the summary email.

    Atlas term failure rate thresholds (failed / total attempted):
        >  5% → WARNING
        > 15% → ERROR     (also triggered by any flow fetch failure)
        > 30% → CRITICAL  (also triggered by any flow process failure)

    OrigenToGraph errors degrade the level by one step.
    The highest triggered level wins. Returns the computed ErrorLevel.
    """
    mg_ok             = migration_result.get("ok", [])
    mg_not_found      = migration_result.get("not_found", [])
    mg_failed         = migration_result.get("failed", [])
    flow_fetch_fail   = flow_result.get("fetch_fail", [])
    flow_process_fail = flow_result.get("process_fail", [])
    origen_errors     = origen_result.get("errors", [])

    # Atlas term failure rate
    mg_total     = len(mg_ok) + len(mg_not_found) + len(mg_failed)
    mg_fail_rate = len(mg_failed) / mg_total if mg_total > 0 else 0.0

    # Evaluate from lowest to highest severity; last match wins
    level = ErrorLevel.NO_ERROR

    if mg_fail_rate > _WARN_THRESHOLD or origen_errors:
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
    if origen_errors:
        log.info("    OrigenToGraph errors: %s", "; ".join(origen_errors))

    flow_success      = not flow_fetch_fail and not flow_process_fail
    migration_success = bool(migration_result) and not mg_failed

    send_summary_email(
        flow_result=flow_result,
        flow_success=flow_success,
        migration_result=migration_result,
        migration_success=migration_success,
        origen_result=origen_result,
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

    # 4. Run TermToGraph pipeline
    migration_result  = run_term_to_graph_pipeline()
    mg_ok        = migration_result.get("ok", [])
    mg_not_found = migration_result.get("not_found", [])
    mg_failed    = migration_result.get("failed", [])
    migration_success = bool(migration_result) and not mg_failed

    # 5. Run OrigenToGraph pipeline (classifies Tabla nodes by loading origin)
    origen_result   = run_origen_to_graph_pipeline()
    or_total        = origen_result.get("total", 0)
    or_ms           = origen_result.get("microservicios", 0)
    or_dis          = origen_result.get("dis", 0)
    or_ne           = origen_result.get("no_estandar", 0)
    or_errors       = origen_result.get("errors", [])
    origen_success  = bool(origen_result) and not or_errors

    # 6. Summary
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
    log.info("TermToGraph:")
    log.info("  Loaded successfully     (%d)", len(mg_ok))
    log.info("  Not found               (%d)", len(mg_not_found))
    log.info("  Failed                  (%d): %s", len(mg_failed), ", ".join(mg_failed) or "—")
    log.info("  Overall: %s", "✔ SUCCESS" if migration_success else "✘ FAILED")
    log.info("")
    log.info("OrigenToGraph:")
    log.info("  Total Tabla nodes       (%d)", or_total)
    log.info("  Microservicios          (%d)", or_ms)
    log.info("  DIS                     (%d)", or_dis)
    log.info("  Proceso no estándar     (%d)", or_ne)
    log.info("  Errors                  (%d): %s", len(or_errors), "; ".join(or_errors) or "—")
    log.info("  Overall: %s", "✔ SUCCESS" if origen_success else "✘ FAILED (partial)")
    log.info("=" * 70)
    log.info("=== All done ===")

    # 7. Verify final state (classifies severity and sends summary email)
    level = verify_final_state(
        flow_result=flow_result,
        migration_result=migration_result,
        origen_result=origen_result,
    )

    # Exit with error code for anything above WARNING
    if level.value >= ErrorLevel.ERROR.value:
        sys.exit(1)


if __name__ == "__main__":
    main()
