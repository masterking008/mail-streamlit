#!/bin/bash
echo "🚀 Starting E-Cell Mail Streamlit App..."
source venv/bin/activate
echo "✅ Virtual environment activated!"
echo "📧 Launching mail app at http://localhost:8501"
streamlit run app.py