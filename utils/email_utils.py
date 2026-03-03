"""
Email utility — sends HTML emails via Gmail SMTP (STARTTLS on port 587).
All credentials are read from config at call time (no module-level caching).
"""
import smtplib
import secrets
import string
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime


# ── Core sender ───────────────────────────────────────────────────────────────

def send_email(recipient_email: str, recipient_name: str, subject: str, html_body: str):
    """Send an HTML email. Returns None on success, error string on failure."""
    import config
    try:
        msg = MIMEMultipart("alternative")
        msg["From"] = f"{config.SMTP_SENDER_NAME} <{config.SMTP_SENDER_EMAIL}>"
        msg["To"] = f"{recipient_name} <{recipient_email}>"
        msg["Subject"] = subject
        msg.attach(MIMEText(html_body, "html"))

        with smtplib.SMTP(config.SMTP_HOST, config.SMTP_PORT) as smtp:
            smtp.ehlo()
            smtp.starttls()
            smtp.login(config.SMTP_SENDER_EMAIL, config.SMTP_SENDER_PASSWORD)
            smtp.sendmail(config.SMTP_SENDER_EMAIL, recipient_email, msg.as_string())
        return None
    except Exception as e:
        return str(e)


def generate_temp_password(length: int = 10) -> str:
    alphabet = string.ascii_letters + string.digits
    return "".join(secrets.choice(alphabet) for _ in range(length))


# ── Date formatter ─────────────────────────────────────────────────────────────

def _fmt(dt) -> str:
    if isinstance(dt, str):
        try:
            dt = datetime.fromisoformat(dt.replace("Z", "+00:00"))
        except Exception:
            return dt
    return dt.strftime("%Y-%m-%d")


# ── Shared HTML shell ──────────────────────────────────────────────────────────

def _shell(header_icon: str, header_title: str, header_subtitle: str,
           accent_from: str, accent_to: str, content: str, sender_email: str) -> str:
    year = datetime.now().year
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1.0">
  <title>KIT-IFMS</title>
  <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
    * {{ box-sizing: border-box; }}
    body {{ margin:0; padding:0; background:#EEF2FF; font-family:'Inter',Arial,sans-serif; }}
    .wrapper {{ width:100%; background:#EEF2FF; padding:48px 16px; }}
    .card {{ max-width:600px; margin:0 auto; background:#ffffff; border-radius:20px; overflow:hidden;
             box-shadow:0 8px 40px rgba(30,58,138,0.12); }}
    .header {{ background:linear-gradient(135deg,{accent_from} 0%,{accent_to} 100%);
               padding:44px 40px 36px; text-align:center; }}
    .header-icon {{ font-size:48px; line-height:1; margin-bottom:12px; }}
    .header h1 {{ color:#fff; margin:0 0 6px; font-size:28px; font-weight:800; letter-spacing:-0.5px; }}
    .header p  {{ color:rgba(255,255,255,0.80); margin:0; font-size:13px; font-weight:500; letter-spacing:0.3px; }}
    .body {{ padding:40px; }}
    .greeting {{ font-size:16px; color:#1F2937; margin:0 0 10px; font-weight:600; }}
    .intro    {{ font-size:14px; color:#6B7280; margin:0 0 28px; line-height:1.7; }}
    .key-card {{ border-radius:14px; padding:28px 24px 20px; margin-bottom:28px; text-align:center;
                 background:#F0F4FF; border:1.5px solid #C7D2FE; }}
    .key-label {{ font-size:10px; font-weight:700; letter-spacing:2px; text-transform:uppercase;
                  color:#6366F1; margin:0 0 12px; }}
    .key-value {{ font-family:'Courier New',Courier,monospace; font-size:13px; font-weight:700;
                  color:#1E1B4B; word-break:break-all; letter-spacing:0.6px; background:#E0E7FF;
                  border-radius:8px; padding:12px 14px; margin:0 0 16px;
                  border:1px solid #C7D2FE; display:block; user-select:all; }}
    .copy-btn {{ display:inline-flex; align-items:center; gap:7px; padding:9px 20px;
                 border-radius:8px; border:none; background:#4F46E5; color:#fff;
                 font-size:13px; font-weight:600; cursor:pointer; text-decoration:none; }}
    .details {{ width:100%; border-radius:12px; margin-bottom:28px; overflow:hidden;
                border:1px solid #E5E7EB; border-collapse:collapse; }}
    .details td {{ padding:14px 20px; border-bottom:1px solid #F3F4F6; background:#FAFAFA; }}
    .details tr:last-child td {{ border-bottom:none; }}
    .detail-label {{ font-size:10px; font-weight:700; letter-spacing:1.5px; text-transform:uppercase;
                     color:#9CA3AF; display:block; margin-bottom:4px; }}
    .detail-value {{ font-size:15px; font-weight:700; color:#111827; }}
    .info-box {{ border-radius:0 12px 12px 0; padding:16px 20px; margin-bottom:28px; }}
    .info-box.amber {{ background:#FFFBEB; border-left:4px solid #F59E0B; }}
    .info-box.red   {{ background:#FFF1F2; border-left:4px solid #F43F5E; }}
    .info-box.green {{ background:#F0FDF4; border-left:4px solid #10B981; }}
    .info-title {{ font-size:12px; font-weight:700; margin:0 0 8px; text-transform:uppercase; letter-spacing:0.5px; }}
    .info-box.amber .info-title {{ color:#92400E; }}
    .info-box.red   .info-title {{ color:#9F1239; }}
    .info-box.green .info-title {{ color:#065F46; }}
    .info-box ol, .info-box ul {{ margin:0; padding-left:20px; font-size:13px; line-height:1.9; }}
    .info-box.amber ol {{ color:#78350F; }}
    .info-box.green ul {{ color:#047857; }}
    .footer {{ background:#F8FAFC; padding:20px 40px; text-align:center; border-top:1px solid #E5E7EB; }}
    .footer p {{ margin:0; font-size:11px; color:#9CA3AF; line-height:1.8; }}
    .footer a {{ color:#6366F1; text-decoration:none; font-weight:500; }}
    .disclaimer {{ font-size:12px; color:#9CA3AF; line-height:1.6; margin:0; }}
    @media only screen and (max-width:620px) {{
      .body {{ padding:24px 20px; }} .header {{ padding:32px 20px 28px; }}
    }}
  </style>
</head>
<body>
<div class="wrapper"><div class="card">
  <div class="header">
    <div class="header-icon">{header_icon}</div>
    <h1>{header_title}</h1>
    <p>{header_subtitle}</p>
  </div>
  <div class="body">{content}</div>
  <div class="footer">
    <p>&copy; {year} <strong>KIT-IFMS</strong> &nbsp;&middot;&nbsp;
       <a href="mailto:{sender_email}">{sender_email}</a></p>
    <p style="margin-top:4px;">This is an automated message — please do not reply directly.</p>
  </div>
</div></div>
</body></html>"""


def _key_box(license_key: str) -> str:
    return f"""
      <div class="key-card">
        <p class="key-label">&#128273; Your License Key</p>
        <span class="key-value">{license_key}</span>
      </div>"""


# ── Public email builders ──────────────────────────────────────────────────────

def send_license_email(recipient_email: str, recipient_name: str,
                       license_key: str, expires_at) -> str | None:
    """Send a new license key email. Returns None on success, error string on failure."""
    import config
    expiry_str = _fmt(expires_at)
    content = f"""
      <p class="greeting">Hello, {recipient_name} &#128075;</p>
      <p class="intro">Your <strong>KIT-IFMS</strong> license key has been generated and is ready to activate.
        Please keep this key secure &mdash; it grants full access to your business account.</p>
      {_key_box(license_key)}
      <table class="details">
        <tr><td><span class="detail-label">Issued To</span><span class="detail-value">{recipient_name}</span></td></tr>
        <tr><td><span class="detail-label">Valid Until</span><span class="detail-value">{expiry_str}</span></td></tr>
        <tr><td><span class="detail-label">Product</span><span class="detail-value">KIT-IFMS Business Management Suite</span></td></tr>
      </table>
      <div class="info-box amber">
        <p class="info-title">&#9654; How to activate</p>
        <ol>
          <li>Open the <strong>KIT-IFMS</strong> app</li>
          <li>On the activation screen, tap <em>Paste</em> or type the key above</li>
          <li>Tap <strong>"Activate License"</strong> &mdash; you're all set!</li>
        </ol>
      </div>
      <p class="disclaimer">If you did not request this license, contact
        <a href="mailto:{config.SMTP_SENDER_EMAIL}" style="color:#6366F1;">{config.SMTP_SENDER_EMAIL}</a> immediately.</p>"""
    return send_email(
        recipient_email, recipient_name,
        "&#128273; KIT-IFMS \u2014 Your License Key",
        _shell("&#128272;", "KIT-IFMS", "Your new license key is ready",
               "#1E3A5F", "#4F46E5", content, config.SMTP_SENDER_EMAIL),
    )


def send_license_renewal_email(recipient_email: str, recipient_name: str,
                               license_key: str, new_expires_at) -> str | None:
    """Send a license renewal email."""
    import config
    expiry_str = _fmt(new_expires_at)
    content = f"""
      <p class="greeting">Great news, {recipient_name}! &#127881;</p>
      <p class="intro">Your <strong>KIT-IFMS</strong> license has been successfully renewed.
        Your key remains the same &mdash; use it to continue as normal.</p>
      {_key_box(license_key)}
      <table class="details">
        <tr><td><span class="detail-label">License Holder</span><span class="detail-value">{recipient_name}</span></td></tr>
        <tr><td><span class="detail-label">New Expiry Date</span><span class="detail-value">{expiry_str}</span></td></tr>
      </table>
      <div class="info-box green">
        <p class="info-title">&#10003; No re-activation needed</p>
        <ul>
          <li>Your existing session remains active</li>
          <li>The renewed expiry date takes effect immediately</li>
          <li>No changes to your data or settings</li>
        </ul>
      </div>
      <p class="disclaimer">Questions? Reach us at
        <a href="mailto:{config.SMTP_SENDER_EMAIL}" style="color:#6366F1;">{config.SMTP_SENDER_EMAIL}</a>.</p>"""
    return send_email(
        recipient_email, recipient_name,
        "&#10003; KIT-IFMS \u2014 License Renewed",
        _shell("&#10003;", "License Renewed", "KIT-IFMS Business Management Suite",
               "#065F46", "#10B981", content, config.SMTP_SENDER_EMAIL),
    )


def send_license_expiry_warning_email(recipient_email: str, recipient_name: str,
                                      expires_at) -> str | None:
    """Send a license expiry warning email."""
    import config
    from datetime import timezone
    expiry_str = _fmt(expires_at)
    if isinstance(expires_at, str):
        try:
            expires_at = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except Exception:
            expires_at = datetime.now(timezone.utc)
    days_left = (expires_at - datetime.now(expires_at.tzinfo)).days
    urgency = "red" if days_left <= 3 else "amber"
    warn_color = "#9F1239" if urgency == "red" else "#92400E"
    content = f"""
      <p class="greeting">Hello, {recipient_name}</p>
      <p class="intro">Your <strong>KIT-IFMS</strong> license is expiring soon.
        Please renew before the deadline to avoid interruption.</p>
      <div class="info-box {urgency}" style="text-align:center;border-radius:12px;border-left:none;padding:28px;margin-bottom:28px;">
        <p style="margin:0 0 6px;font-size:36px;font-weight:800;color:{warn_color};">
          {days_left} day{"s" if days_left != 1 else ""}</p>
        <p style="margin:0;font-size:13px;color:{warn_color};">remaining &mdash; expires on <strong>{expiry_str}</strong></p>
      </div>
      <table class="details">
        <tr><td><span class="detail-label">License Holder</span><span class="detail-value">{recipient_name}</span></td></tr>
        <tr><td><span class="detail-label">Expiry Date</span><span class="detail-value" style="color:{warn_color};">{expiry_str}</span></td></tr>
      </table>
      <div class="info-box amber">
        <p class="info-title">&#128338; Next steps</p>
        <ol>
          <li>Contact your KIT-IFMS administrator to arrange renewal</li>
          <li>Once renewed, the new expiry date applies automatically</li>
          <li>No data loss occurs during or after renewal</li>
        </ol>
      </div>
      <p class="disclaimer">To arrange renewal, contact
        <a href="mailto:{config.SMTP_SENDER_EMAIL}" style="color:#6366F1;">{config.SMTP_SENDER_EMAIL}</a>.</p>"""
    return send_email(
        recipient_email, recipient_name,
        "&#9888; KIT-IFMS \u2014 License Expiring Soon",
        _shell("&#9203;", "License Expiring Soon", "Action required \u2014 KIT-IFMS",
               "#92400E", "#F59E0B", content, config.SMTP_SENDER_EMAIL),
    )


def send_temp_password_email(recipient_email: str, recipient_name: str,
                             temp_password: str) -> str | None:
    """Send a temporary password email for password reset."""
    import config
    content = f"""
      <p class="greeting">Hello, {recipient_name}</p>
      <p class="intro">We received a request to reset your <strong>KIT-IFMS</strong> password.
        Use the temporary password below to log in, then update your password from settings.</p>
      <div class="key-card">
        <p class="key-label">&#128274; Temporary Password</p>
        <span class="key-value">{temp_password}</span>
      </div>
      <div class="info-box amber">
        <p class="info-title">&#9654; Next steps</p>
        <ol>
          <li>Open the <strong>KIT-IFMS</strong> app and log in with this temporary password</li>
          <li>Go to <em>Settings &rarr; Profile</em> and set a new password</li>
          <li>This temporary password expires after your next successful login</li>
        </ol>
      </div>
      <p class="disclaimer">If you did not request a password reset, contact
        <a href="mailto:{config.SMTP_SENDER_EMAIL}" style="color:#6366F1;">{config.SMTP_SENDER_EMAIL}</a> immediately.</p>"""
    return send_email(
        recipient_email, recipient_name,
        "&#128274; KIT-IFMS \u2014 Password Reset",
        _shell("&#128274;", "Password Reset", "Your temporary password is ready",
               "#1E3A5F", "#6366F1", content, config.SMTP_SENDER_EMAIL),
    )
