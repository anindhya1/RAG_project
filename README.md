Make sure Docker container is active.

Step 1:-
backend: uv run uvicorn main:app

Step 2:-
start inngest server: npx inngest-cli@latest dev -u http://127.0.0.1:8000/api/inngest

Step 3:-
frontend: uv run streamlit run streamlit_app.py