import streamlit as st
import pandas as pd
import aioboto3
import asyncio
from aiolimiter import AsyncLimiter
from jinja2 import Template
from datetime import datetime

from dotenv import load_dotenv
import os
import logging
from botocore.exceptions import ClientError, BotoCoreError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import re

# Load environment variables
load_dotenv()

# ================== LOGGING CONFIG ==================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ses_mailer.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Enable boto3/botocore debug logging for SES
logging.getLogger('botocore').setLevel(logging.INFO)
logging.getLogger('aioboto3').setLevel(logging.INFO)
# =====================================================

# ================== CONFIG ==================
AWS_REGION = "ap-south-1"
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
# ============================================

st.set_page_config(page_title="AWS SES Mass Mailer", layout="centered")
st.title("📧 AWS SES Mass Mailer")

# --- Sidebar: SES Settings ---
st.sidebar.header("AWS SES Settings")
sender_name = st.sidebar.text_input("Sender Name", value="E-Cell Team")
sender_email = st.sidebar.text_input("Sender Email", value="no-reply@ecell.in")
region = st.sidebar.text_input("AWS Region", value=AWS_REGION)
rate_limit = st.sidebar.number_input("Emails per second", min_value=1, value=50, step=1)
concurrency = st.sidebar.number_input("Concurrent Tasks", min_value=1, value=100, step=10)

# Initialize session state
if 'cancel_bulk' not in st.session_state:
    st.session_state.cancel_bulk = False
if 'bulk_running' not in st.session_state:
    st.session_state.bulk_running = False
if 'sent_emails' not in st.session_state:
    st.session_state.sent_emails = []
if 'failed_emails' not in st.session_state:
    st.session_state.failed_emails = []
if 'demo_sent' not in st.session_state:
    st.session_state.demo_sent = False

# ==================== ASYNC FUNCTIONS ====================
async def send_demo_email(name, email, subject, html_body):
    logger.info(f"Sending demo email to {email}")
    try:
        html_template = Template(html_body)
        rendered_html = html_template.render(name=name)
        text_content = re.sub('<[^<]+?>', '', rendered_html)
        
        # Create MIME message
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = f"{sender_name} <{sender_email}>"
        msg['To'] = email
        
        # Add text and HTML parts
        text_part = MIMEText(text_content, 'plain', 'utf-8')
        html_part = MIMEText(rendered_html, 'html', 'utf-8')
        msg.attach(text_part)
        msg.attach(html_part)
        
        session = aioboto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            region_name=region
        )
        async with session.client("ses") as ses_client:
            response = await ses_client.send_raw_email(
                Source=f"{sender_name} <{sender_email}>",
                Destinations=[email],
                RawMessage={'Data': msg.as_string()}
            )
            logger.info(f"Demo email sent successfully to {email}. MessageId: {response.get('MessageId')}")
            return response
    except ClientError as e:
        error_code = e.response['Error']['Code']
        error_message = e.response['Error']['Message']
        logger.error(f"SES ClientError sending demo email to {email}: {error_code} - {error_message}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error sending demo email to {email}: {str(e)}")
        raise

async def send_single_email(recipient, name, subject, html_body, ses_client, limiter):
    async with limiter:
        try:
            html_template = Template(html_body)
            rendered_html = html_template.render(name=name)
            text_content = re.sub('<[^<]+?>', '', rendered_html)
            
            # Create MIME message
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = f"{sender_name} <{sender_email}>"
            msg['To'] = recipient
            
            # Add text and HTML parts
            text_part = MIMEText(text_content, 'plain', 'utf-8')
            html_part = MIMEText(rendered_html, 'html', 'utf-8')
            msg.attach(text_part)
            msg.attach(html_part)
            
            response = await ses_client.send_raw_email(
                Source=f"{sender_name} <{sender_email}>",
                Destinations=[recipient],
                RawMessage={'Data': msg.as_string()}
            )
            logger.info(f"Email sent to {recipient}. MessageId: {response.get('MessageId')}")
            return response
        except ClientError as e:
            error_code = e.response['Error']['Code']
            error_message = e.response['Error']['Message']
            logger.error(f"SES ClientError for {recipient}: {error_code} - {error_message}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error for {recipient}: {str(e)}")
            raise

async def run_bulk_with_progress(df, subject, html_body, progress_bar, status_text, current_email, sent_container, failed_container):
    logger.info(f"Starting bulk email campaign for {len(df)} recipients")
    limiter = AsyncLimiter(rate_limit, 1)
    total = len(df)
    sent_count = 0
    
    session = aioboto3.Session(
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=region
    )
    async with session.client("ses") as ses_client:
        for idx, row in df.iterrows():
            if st.session_state.cancel_bulk:
                status_text.text("❌ Sending cancelled")
                break
                
            recipient = row["email"]
            name = row.get("name", "there")
            current_email.text(f"📧 Sending to: {recipient}")
            
            try:
                await send_single_email(recipient, name, subject, html_body, ses_client, limiter)
                st.session_state.sent_emails.append(recipient)
                sent_count += 1
            except Exception as e:
                error_msg = f"{recipient}: {str(e)}"
                st.session_state.failed_emails.append(error_msg)
                logger.error(f"Failed to send email to {recipient}: {str(e)}")
            
            # Update progress
            progress = (idx + 1) / total
            progress_bar.progress(progress)
            status_text.text(f"Progress: {idx + 1}/{total} emails processed")
            
            # Update email lists
            sent_container.text("\n".join(st.session_state.sent_emails[-10:]))
            failed_container.text("\n".join(st.session_state.failed_emails[-10:]))
    
    # Final status
    if not st.session_state.cancel_bulk:
        status_text.text(f"✅ Completed! Sent: {sent_count}, Failed: {len(st.session_state.failed_emails)}")
        logger.info(f"Bulk email campaign completed. Sent: {sent_count}, Failed: {len(st.session_state.failed_emails)}")
    else:
        logger.info(f"Bulk email campaign cancelled. Sent: {sent_count}, Failed: {len(st.session_state.failed_emails)}")
    
    st.session_state.bulk_running = False

# ==================== EMAIL COMPOSER ====================
st.subheader("✉️ Compose Email")

subject = st.text_input("Subject", value="E-Cell Update")
st.write("**HTML Body:**")
html_content = st.text_area(
    "Email Content (HTML)",
    value="<p>Hello {{ name }}, this is an update from E-Cell!</p>",
    height=200,
    help="Use HTML tags for formatting. Use {{ name }} for personalization."
)
# Ensure proper HTML structure
if html_content:
    # Wrap content in proper HTML structure if not already wrapped
    if not html_content.strip().startswith('<!DOCTYPE') and not html_content.strip().startswith('<html'):
        html_body = f"""<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Email</title>
</head>
<body>
    {html_content}
</body>
</html>"""
    else:
        html_body = html_content
else:
    html_body = """<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Email</title>
</head>
<body>
    <p>Hello {{ name }}, this is an update from E-Cell!</p>
</body>
</html>"""

# ==================== EMAIL PREVIEW ====================
if st.checkbox("📋 Preview Email"):
    st.write("**Email Preview:**")
    preview_name = st.text_input("Preview Name", value="John Doe", key="preview_name")
    
    # Render preview
    try:
        from jinja2 import Template
        preview_template = Template(html_body)
        preview_html = preview_template.render(name=preview_name)
        st.components.v1.html(preview_html, height=400, scrolling=True)
    except Exception as e:
        st.error(f"Preview error: {e}")

# ==================== DEMO & BULK BUTTONS ====================
col1, col2 = st.columns(2)

with col1:
    if st.button("📨 Send Demo", use_container_width=True):
        st.session_state.show_demo_popup = True
        st.session_state.demo_sent = False

with col2:
    csv_file = st.file_uploader("Upload CSV", type=["csv"], key="csv_upload")
    if csv_file and st.button("📊 Bulk Email", use_container_width=True):
        df = pd.read_csv(csv_file)
        if "email" not in df.columns:
            st.error("CSV must contain an 'email' column!")
        else:
            # Get unique emails
            unique_df = df.drop_duplicates(subset=['email'])
            st.session_state.bulk_df = unique_df
            st.session_state.show_bulk_confirm = True
            st.info(f"Found {len(unique_df)} unique emails out of {len(df)} total emails")

# ==================== DEMO SUCCESS MESSAGE ====================
if st.session_state.get('demo_sent', False):
    st.success("📧 Demo email sent successfully!")
    st.session_state.demo_sent = False

# ==================== DEMO POPUP ====================
if st.session_state.get('show_demo_popup', False):
    st.markdown("---")
    st.subheader("📧 Demo Email")
    demo_name = st.text_input("Recipient Name", value="Dinesh Test")
    demo_email = st.text_input("Recipient Email", value="dinesh@ecell.in")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Send Demo", type="primary"):
            if demo_name and demo_email:
                try:
                    asyncio.run(send_demo_email(demo_name, demo_email, subject, html_body))
                    st.session_state.demo_sent = True
                    st.session_state.show_demo_popup = False
                    st.rerun()
                except Exception as e:
                    st.error(f"Failed: {e}")
            else:
                st.error("Please fill all fields")
    with col2:
        if st.button("Cancel"):
            st.session_state.show_demo_popup = False
            st.rerun()

# ==================== BULK CONFIRMATION ====================
if st.session_state.get('show_bulk_confirm', False):
    with st.container():
        st.markdown("---")
        st.subheader(f"📊 Bulk Email - {len(st.session_state.bulk_df)} Recipients")
        
        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Confirm Send", type="primary"):
                st.session_state.show_bulk_confirm = False
                st.session_state.bulk_running = True
                st.session_state.cancel_bulk = False
                st.session_state.sent_emails = []
                st.session_state.failed_emails = []
                st.rerun()
        with col2:
            if st.button("❌ Cancel"):
                st.session_state.show_bulk_confirm = False
                st.rerun()

# ==================== BULK EMAIL SENDING ====================
if st.session_state.bulk_running:
    st.markdown("---")
    st.subheader("📤 Sending Bulk Emails")
    
    # Cancel button
    if st.button("🛑 Cancel Sending", type="secondary"):
        st.session_state.cancel_bulk = True
    
    # Progress containers
    progress_bar = st.progress(0)
    status_text = st.empty()
    current_email = st.empty()
    
    # Email lists
    col1, col2 = st.columns(2)
    with col1:
        st.write("**✅ Sent Emails:**")
        sent_container = st.empty()
    with col2:
        st.write("**❌ Failed Emails:**")
        failed_container = st.empty()
    
    # Run bulk sending
    asyncio.run(run_bulk_with_progress(
        st.session_state.bulk_df, 
        subject, 
        html_body,
        progress_bar, 
        status_text, 
        current_email,
        sent_container,
        failed_container
    ))