@echo off
echo 🚀 Starting E-Cell Mail Streamlit App...
call venv\Scripts\activate.bat
echo ✅ Virtual environment activated!
echo 📧 Launching mail app at http://localhost:8501
streamlit run app.py
pause
