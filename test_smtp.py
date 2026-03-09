#!/usr/bin/env python3
"""
SMTP Test Script - Debug SMTP connection and email sending separately.

This extracts the SMTP logic from main.py for isolated testing.
Run: python test_smtp.py
"""

import os
import sys
import logging
import smtplib
import traceback
from pathlib import Path
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from dotenv import load_dotenv

# Setup logging
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

# Load environment configuration
env_file = Path(__file__).parent / ".env"
if not env_file.exists():
    print(f"ERROR: .env file not found at {env_file}")
    sys.exit(1)

log.info(f"Loading environment from: {env_file}")
load_dotenv(env_file, override=True)


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
        
        log.debug(f"SMTPConnection initialized:")
        log.debug(f"  host: {self.host}")
        log.debug(f"  port: {self.port}")
        log.debug(f"  username: {self.username}")
        log.debug(f"  tls: {self.tls}")
        log.debug(f"  sender_email: {self.sender_email}")
        log.debug(f"  receiver_emails: {self.receiver_emails}")

    def initialize_server(self):
        log.info("Initializing SMTP server connection...")
        try:
            log.debug(f"Connecting to {self.host}:{self.port}")
            self.server = smtplib.SMTP(self.host, self.port)
            
            if self.tls:
                log.info("Starting TLS...")
                try:
                    self.server.starttls()
                    log.info("TLS started successfully")
                except Exception as e:
                    log.error(f"Error starting TLS: {e}")
                    raise
            
            log.info(f"Logging in with username: {self.username}")
            self.server.login(self.username, self.password)
            log.info("Login successful!")
            
        except smtplib.SMTPAuthenticationError as e:
            log.error(f"SMTP Authentication failed: {e}")
            log.error("→ Check SMTP_USERNAME and SMTP_PASSWORD in .env")
            raise
        except smtplib.SMTPException as e:
            log.error(f"SMTP error: {e}")
            raise
        except Exception as e:
            log.error(f"Unexpected error: {e}")
            log.debug(traceback.format_exc())
            raise

    def disconnect(self):
        if self.server is not None:
            try:
                self.server.quit()
                log.info("SMTP server disconnected")
            except Exception as e:
                log.warning(f"Error disconnecting: {e}")
            self.server = None

    def send_email(self, subject: str, body: str,
                   recipient_emails: list | None = None, html_type=False):
        try:
            if self.server is None:
                self.initialize_server()
            
            recipients = recipient_emails or self.receiver_emails
            
            if not recipients:
                log.error("No recipient emails provided!")
                return False
            
            log.info(f"Preparing email to: {', '.join(recipients)}")
            msg = MIMEMultipart()
            msg["From"]    = self.sender_email
            msg["To"]      = ", ".join(recipients)
            msg["Subject"] = subject
            msg.attach(MIMEText(body, "html" if html_type else "plain", "utf-8"))

            log.info(f"Sending email...")
            self.server.sendmail(self.sender_email, recipients, msg.as_string())
            log.info(f"✓ Email sent successfully to: {', '.join(recipients)}")
            self.retries = 0
            return True

        except smtplib.SMTPRecipientsRefused as e:
            log.error(f"SMTP Recipients refused: {e}")
            log.error("→ Check EMAIL_TO in .env")
            raise
        except smtplib.SMTPSenderRefused as e:
            log.error(f"SMTP Sender refused: {e}")
            log.error("→ Check EMAIL_FROM in .env")
            raise
        except smtplib.SMTPException as e:
            log.error(f"SMTP error sending email: {e}")
            log.debug(traceback.format_exc())
            if self.retries == 0:
                log.info("Retrying connection...")
                self.retries += 1
                try:
                    self.initialize_server()
                    return self.send_email(subject, body, recipient_emails, html_type)
                except Exception as retry_exc:
                    log.error(f"Retry failed: {retry_exc}")
                    self.retries = 0
                    return False
            else:
                self.retries = 0
                return False
        except Exception as exc:
            log.error(f"Unexpected error sending email: {exc}")
            log.debug(traceback.format_exc())
            return False


def build_smtp_connection() -> SMTPConnection | None:
    """Instantiate an SMTPConnection from environment variables."""
    log.info("Reading SMTP configuration from environment variables...")
    
    host      = os.getenv("SMTP_HOST")
    port_str  = os.getenv("SMTP_PORT", "587")
    username  = os.getenv("SMTP_USERNAME")
    password  = os.getenv("SMTP_PASSWORD")
    tls_str   = os.getenv("SMTP_TLS", "false").strip().lower()
    sender    = os.getenv("EMAIL_FROM")
    receivers_str = os.getenv("EMAIL_TO", "")
    
    log.debug(f"  SMTP_HOST: {host}")
    log.debug(f"  SMTP_PORT: {port_str}")
    log.debug(f"  SMTP_USERNAME: {username}")
    log.debug(f"  SMTP_PASSWORD: {'*' * len(password) if password else 'NOT SET'}")
    log.debug(f"  SMTP_TLS: {tls_str}")
    log.debug(f"  EMAIL_FROM: {sender}")
    log.debug(f"  EMAIL_TO: {receivers_str}")
    
    # Validate
    if not host:
        log.error("✗ SMTP_HOST not set in .env")
        return None
    if not username:
        log.error("✗ SMTP_USERNAME not set in .env")
        return None
    if not password:
        log.error("✗ SMTP_PASSWORD not set in .env")
        return None
    if not sender:
        log.error("✗ EMAIL_FROM not set in .env")
        return None
    if not receivers_str:
        log.error("✗ EMAIL_TO not set in .env")
        return None
    
    receivers = [a.strip() for a in receivers_str.split(",") if a.strip()]
    
    try:
        port = int(port_str)
    except ValueError:
        log.error(f"✗ SMTP_PORT must be an integer, got: {port_str}")
        return None
    
    tls = tls_str == "true"

    return SMTPConnection(
        host=host, port=port, username=username, password=password,
        sender_email=sender, receiver_emails=receivers, tls=tls,
    )


def test_connection():
    """Test SMTP connection without sending an email."""
    log.info("=" * 70)
    log.info("TEST 1: Connection and Authentication")
    log.info("=" * 70)
    
    conn = build_smtp_connection()
    if conn is None:
        log.error("Failed to build SMTP connection from environment")
        return False
    
    try:
        conn.initialize_server()
        log.info("✓ Connection test PASSED")
        return True
    except Exception as e:
        log.error(f"✗ Connection test FAILED: {e}")
        return False
    finally:
        conn.disconnect()


def test_email():
    """Test sending an actual email."""
    log.info("=" * 70)
    log.info("TEST 2: Send Test Email")
    log.info("=" * 70)
    
    conn = build_smtp_connection()
    if conn is None:
        log.error("Failed to build SMTP connection from environment")
        return False
    
    try:
        conn.initialize_server()
        
        subject = "[TEST] GenerarGrafoPetersen SMTP Test"
        body = """Hi,

This is a test email from the GenerarGrafoPetersen SMTP test script.

If you received this message, your SMTP configuration is working correctly!

Regards,
GenerarGrafoPetersen Test Script
"""
        
        success = conn.send_email(subject, body)
        if success:
            log.info("✓ Email send test PASSED")
            return True
        else:
            log.error("✗ Email send test FAILED")
            return False
            
    except Exception as e:
        log.error(f"✗ Email send test FAILED: {e}")
        return False
    finally:
        conn.disconnect()


def main():
    log.info("")
    log.info("╔" + "=" * 68 + "╗")
    log.info("║" + " GenerarGrafoPetersen SMTP Test Script ".center(68) + "║")
    log.info("╚" + "=" * 68 + "╝")
    log.info("")
    
    results = []
    
    # Test 1: Connection
    results.append(("Connection & Authentication", test_connection()))
    
    # Test 2: Email
    results.append(("Send Email", test_email()))
    
    # Summary
    log.info("")
    log.info("=" * 70)
    log.info("TEST SUMMARY")
    log.info("=" * 70)
    
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        log.info(f"{test_name}: {status}")
    
    all_passed = all(passed for _, passed in results)
    
    if all_passed:
        log.info("")
        log.info("✓ All tests PASSED! Your SMTP configuration is working.")
        return 0
    else:
        log.info("")
        log.error("✗ Some tests FAILED. Check the error messages above.")
        return 1


if __name__ == "__main__":
    sys.exit(main())
