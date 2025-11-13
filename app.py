import streamlit as st
import pandas as pd
import aioboto3
import asyncio
from aiolimiter import AsyncLimiter
from jinja2 import Template
from datetime import datetime, timedelta
import plotly.graph_objects as go

from dotenv import load_dotenv
import os
import logging
from botocore.exceptions import ClientError, BotoCoreError
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import re

# ==================== TEMPLATE VARIABLE DETECTION ====================
def extract_template_variables(template_text):
    """Extract variables from Jinja2 template like {{name}}, {{college}}"""
    pattern = r'\{\{\s*([a-zA-Z_][a-zA-Z0-9_]*)\s*\}\}'
    variables = re.findall(pattern, template_text)
    return list(set(variables))  # Remove duplicates

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
# Mailing credentials
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
# Statistics credentials
STATS_ACCESS_KEY_ID = os.getenv('STATS_ACCESS_KEY_ID')
STATS_SECRET_ACCESS_KEY = os.getenv('STATS_SECRET_ACCESS_KEY')
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

# ==================== SES STATISTICS ====================
async def get_ses_statistics():
    try:
        session = aioboto3.Session(
            aws_access_key_id=STATS_ACCESS_KEY_ID or AWS_ACCESS_KEY_ID,
            aws_secret_access_key=STATS_SECRET_ACCESS_KEY or AWS_SECRET_ACCESS_KEY,
            region_name=region
        )
        async with session.client("ses") as ses_client:
            # Get send statistics for last 24 hours
            response = await ses_client.get_send_statistics()
            
            # Calculate 24 hours ago
            now = datetime.utcnow()
            twenty_four_hours_ago = now - timedelta(hours=24)
            
            # Filter data points from last 24 hours
            recent_stats = []
            for data_point in response.get('SendDataPoints', []):
                timestamp = data_point['Timestamp'].replace(tzinfo=None)
                if timestamp >= twenty_four_hours_ago:
                    recent_stats.append(data_point)
            
            # Calculate totals
            total_sent = sum(point.get('DeliveryAttempts', 0) for point in recent_stats)
            total_bounces = sum(point.get('Bounces', 0) for point in recent_stats)
            total_complaints = sum(point.get('Complaints', 0) for point in recent_stats)
            
            # Calculate rates
            bounce_rate = (total_bounces / total_sent * 100) if total_sent > 0 else 0
            complaint_rate = (total_complaints / total_sent * 100) if total_sent > 0 else 0
            
            # Process all data points for historic view
            all_stats = []
            for data_point in response.get('SendDataPoints', []):
                timestamp = data_point['Timestamp'].replace(tzinfo=None)
                sent = data_point.get('DeliveryAttempts', 0)
                bounces = data_point.get('Bounces', 0)
                complaints = data_point.get('Complaints', 0)
                
                bounce_rate_point = (bounces / sent * 100) if sent > 0 else 0
                complaint_rate_point = (complaints / sent * 100) if sent > 0 else 0
                
                all_stats.append({
                    'timestamp': timestamp,
                    'bounce_rate': bounce_rate_point,
                    'complaint_rate': complaint_rate_point
                })
            
            return {
                'emails_sent_24h': total_sent,
                'bounce_rate': bounce_rate,
                'complaint_rate': complaint_rate,
                'total_bounces': total_bounces,
                'total_complaints': total_complaints,
                'historic_data': all_stats
            }
    except Exception as e:
        logger.error(f"Error fetching SES statistics: {str(e)}")
        return None



# ==================== ASYNC FUNCTIONS ====================
async def send_demo_email(email, subject, html_body, template_vars, pdf_path=None):
    logger.info(f"Sending demo email to {email}")
    try:
        html_template = Template(html_body)
        rendered_html = html_template.render(**template_vars)
        text_content = re.sub('<[^<]+?>', '', rendered_html)
        
        # Create MIME message
        msg = MIMEMultipart('mixed')
        msg['Subject'] = subject
        msg['From'] = f"{sender_name} <{sender_email}>"
        msg['To'] = email
        
        # Create alternative container for text/html
        msg_alt = MIMEMultipart('alternative')
        text_part = MIMEText(text_content, 'plain', 'utf-8')
        html_part = MIMEText(rendered_html, 'html', 'utf-8')
        msg_alt.attach(text_part)
        msg_alt.attach(html_part)
        msg.attach(msg_alt)
        
        # Add PDF attachment if provided
        if pdf_path and os.path.exists(pdf_path):
            with open(pdf_path, 'rb') as f:
                pdf_attachment = MIMEApplication(f.read(), _subtype='pdf')
                pdf_attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(pdf_path))
                msg.attach(pdf_attachment)
        
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

async def send_single_email(recipient, subject, html_body, template_vars, ses_client, limiter, pdf_path=None):
    async with limiter:
        try:
            html_template = Template(html_body)
            rendered_html = html_template.render(**template_vars)
            text_content = re.sub('<[^<]+?>', '', rendered_html)
            
            # Create MIME message
            msg = MIMEMultipart('mixed')
            msg['Subject'] = subject
            msg['From'] = f"{sender_name} <{sender_email}>"
            msg['To'] = recipient
            
            # Create alternative container for text/html
            msg_alt = MIMEMultipart('alternative')
            text_part = MIMEText(text_content, 'plain', 'utf-8')
            html_part = MIMEText(rendered_html, 'html', 'utf-8')
            msg_alt.attach(text_part)
            msg_alt.attach(html_part)
            msg.attach(msg_alt)
            
            # Add PDF attachment if provided
            if pdf_path and os.path.exists(pdf_path):
                with open(pdf_path, 'rb') as f:
                    pdf_attachment = MIMEApplication(f.read(), _subtype='pdf')
                    pdf_attachment.add_header('Content-Disposition', 'attachment', filename=os.path.basename(pdf_path))
                    msg.attach(pdf_attachment)
            
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

async def run_bulk_with_progress(df, subject, html_body, progress_bar, status_text, current_email, sent_container, failed_container, pdf_folder=None):
    template_vars = extract_template_variables(html_body)
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
            pdf_filename = row.get("pdf", None)
            pdf_path = None
            if pdf_filename and pdf_folder:
                pdf_path = os.path.join(pdf_folder, pdf_filename)
                if not os.path.exists(pdf_path):
                    logger.warning(f"PDF not found: {pdf_path} for {recipient}")
                    pdf_path = None
                else:
                    logger.info(f"PDF found: {pdf_path} for {recipient}")
            
            # Build template variables from CSV row
            template_values = {}
            for var in template_vars:
                template_values[var] = row.get(var, f"[{var}]")
            
            current_email.text(f"📧 Sending to: {recipient}")
            
            try:
                await send_single_email(recipient, subject, html_body, template_values, ses_client, limiter, pdf_path)
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
    help="Use HTML tags for formatting. Use {{ variable }} for personalization (e.g., {{ name }}, {{ college }})."
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

# ==================== VARIABLE DETECTION & PREVIEW ====================
# Detect template variables
template_vars = extract_template_variables(html_content)
if template_vars:
    st.info(f"📝 Detected variables: {', '.join(template_vars)}")

if st.checkbox("📋 Preview Email"):
    st.write("**Email Preview:**")
    
    # Dynamic input fields for all detected variables
    preview_values = {}
    if template_vars:
        for var in template_vars:
            default_val = "John Doe" if var == "name" else f"Sample {var.title()}"
            preview_values[var] = st.text_input(f"Preview {var.title()}", value=default_val, key=f"preview_{var}")
    
    # Render preview
    try:
        from jinja2 import Template
        preview_template = Template(html_body)
        preview_html = preview_template.render(**preview_values)
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
    pdf_folder = "pdfs"  # Default PDF folder
    
    # Show PDF folder status
    if os.path.exists(pdf_folder):
        pdf_files = [f for f in os.listdir(pdf_folder) if f.endswith('.pdf')]
        if pdf_files:
            st.success(f"✓ Found {len(pdf_files)} PDF files in 'pdfs' folder")
        else:
            st.info("📁 'pdfs' folder exists but no PDF files found")
    else:
        st.info("📁 Create 'pdfs' folder for PDF attachments")
    if csv_file and st.button("📊 Bulk Email", use_container_width=True):
        df = pd.read_csv(csv_file)
        if "email" not in df.columns:
            st.error("CSV must contain an 'email' column!")
        else:
            # Check if CSV has required template variables
            missing_vars = [var for var in template_vars if var not in df.columns]
            if missing_vars:
                st.error(f"CSV missing required columns: {', '.join(missing_vars)}")
                st.info(f"Required columns: email{', ' + ', '.join(template_vars) if template_vars else ''}")
            else:
                # Get unique emails
                unique_df = df.drop_duplicates(subset=['email'])
                st.session_state.bulk_df = unique_df
                st.session_state.pdf_folder = pdf_folder
                st.session_state.show_bulk_confirm = True
                has_pdf_col = 'pdf' in df.columns
                st.info(f"Found {len(unique_df)} unique emails out of {len(df)} total emails")
                if has_pdf_col:
                    st.info("📎 PDF column detected - attachments will be included")
                if template_vars:
                    st.success(f"✓ Template variables found: {', '.join(template_vars)}")

# ==================== SES STATISTICS ====================
st.markdown("---")
st.subheader("📊 SES Statistics")

if 'ses_stats' not in st.session_state:
    st.session_state.ses_stats = None

if st.button("🔄 Refresh Stats"):
    with st.spinner("Fetching statistics..."):
        st.session_state.ses_stats = asyncio.run(get_ses_statistics())

if st.session_state.ses_stats:
    stats = st.session_state.ses_stats
    
    st.write("**Last 24 Hours:**")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Emails Sent", f"{stats['emails_sent_24h']:,}")
    with col2:
        st.metric("Bounce Rate", f"{stats['bounce_rate']:.2f}%", 
                 delta=f"{stats['total_bounces']} bounces")
    with col3:
        st.metric("Complaint Rate", f"{stats['complaint_rate']:.2f}%", 
                 delta=f"{stats['total_complaints']} complaints")
    
    if stats['historic_data']:
        st.write("**Historic Trends:**")
        df = pd.DataFrame(stats['historic_data'])
        df = df.sort_values('timestamp')
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['bounce_rate'], 
                                name='Bounce Rate (%)', line=dict(color='red')))
        fig.add_trace(go.Scatter(x=df['timestamp'], y=df['complaint_rate'], 
                                name='Complaint Rate (%)', line=dict(color='orange')))
        
        fig.update_layout(title='Bounce & Complaint Rates Over Time', 
                         xaxis_title='Time', yaxis_title='Rate (%)', height=400)
        st.plotly_chart(fig, use_container_width=True)
else:
    st.write("**Last 24 Hours:**")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Emails Sent", "--")
    with col2:
        st.metric("Bounce Rate", "--")
    with col3:
        st.metric("Complaint Rate", "--")
    st.info("Click 'Refresh Stats' to load data")

# ==================== DEMO SUCCESS MESSAGE ====================
if st.session_state.get('demo_sent', False):
    st.success("📧 Demo email sent successfully!")
    st.session_state.demo_sent = False

# ==================== DEMO POPUP ====================
if st.session_state.get('show_demo_popup', False):
    st.markdown("---")
    st.subheader("📧 Demo Email")
    
    # Dynamic input fields for all template variables
    demo_values = {}
    demo_email = st.text_input("Recipient Email", value="dinesh@ecell.in")
    
    if template_vars:
        st.write("**Fill template variables:**")
        for var in template_vars:
            default_val = "Dinesh Test" if var == "name" else f"Sample {var.title()}"
            demo_values[var] = st.text_input(f"{var.title()}", value=default_val, key=f"demo_{var}")
    
    demo_pdf = st.file_uploader("Attach PDF (optional)", type=["pdf"], key="demo_pdf")
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("Send Demo", type="primary"):
            if demo_email and all(demo_values.values()):
                try:
                    pdf_path = None
                    if demo_pdf:
                        # Save uploaded file temporarily
                        pdf_path = f"/tmp/{demo_pdf.name}"
                        with open(pdf_path, "wb") as f:
                            f.write(demo_pdf.getbuffer())
                    
                    asyncio.run(send_demo_email(demo_email, subject, html_body, demo_values, pdf_path))
                    
                    # Clean up temp file
                    if pdf_path and os.path.exists(pdf_path):
                        os.remove(pdf_path)
                    
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
        failed_container,
        st.session_state.get('pdf_folder')
    ))

st.markdown("---")