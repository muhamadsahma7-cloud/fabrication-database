@echo off
echo ============================================
echo  Fabrication Tracker - Web App
echo ============================================
echo.

echo Installing dependencies...
pip install -r requirements.txt --quiet

echo.
echo Starting web server...
echo Open your browser at: http://localhost:8501
echo Press Ctrl+C to stop.
echo.
streamlit run app.py
pause
