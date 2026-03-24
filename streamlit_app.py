from pathlib import Path
import time

import streamlit as st
import inngest
from dotenv import load_dotenv
import os
import requests

load_dotenv()

st.set_page_config(page_title="RAG Ingest PDF", page_icon="📄", layout="centered")


@st.cache_resource
def get_inngest_client() -> inngest.Inngest:
    return inngest.Inngest(app_id="rag_app", is_production=False)


def save_uploaded_pdf(file) -> Path:
    uploads_dir = Path("uploads")
    uploads_dir.mkdir(parents=True, exist_ok=True)
    file_path = uploads_dir / file.name
    file_bytes = file.getbuffer()
    file_path.write_bytes(file_bytes)
    return file_path


def send_rag_ingest_event(pdf_path: Path) -> None:
    client = get_inngest_client()
    client.send_sync(
        inngest.Event(
            name="rag/ingest_pdf",
            data={
                "pdf_path": str(pdf_path.resolve()),
                "source_id": pdf_path.name,
            },
        )
    )


st.title("Upload a PDF to Ingest")
uploaded = st.file_uploader("Choose a PDF", type=["pdf"], accept_multiple_files=False)

if uploaded is not None:
    with st.spinner("Uploading and triggering ingestion..."):
        path = save_uploaded_pdf(uploaded)
        # Kick off the event and block until the send completes
        send_rag_ingest_event(path)
        # Small pause for user feedback continuity
        time.sleep(0.3)
    st.success(f"Triggered ingestion for: {path.name}")
    st.caption("You can upload another PDF if you like.")

st.divider()
st.title("Ask a question about your PDFs")


def send_rag_query_event(question: str, top_k: int) -> str:
    client = get_inngest_client()
    ids = client.send_sync(
        inngest.Event(
            name="rag/query_pdf_ai",
            data={
                "question": question,
                "top_k": top_k,
            },
        )
    )
    return ids[0]


def _inngest_api_base() -> str:
    # Local dev server default; configurable via env
    return os.getenv("INNGEST_API_BASE", "http://127.0.0.1:8288/v1")


def fetch_runs(event_id: str) -> list[dict]:
    url = f"{_inngest_api_base()}/events/{event_id}/runs"
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()
    return data.get("data", [])


def _pick_run_for_query(runs: list[dict]) -> dict | None:
    """Prefer the RAG query function if the API returns multiple runs for one event."""
    if not runs:
        return None
    hint = os.getenv("INNGEST_QUERY_FN_HINT", "Query PDF").lower()
    for r in runs:
        fid = str(r.get("function_id") or r.get("functionId") or "")
        name = str(r.get("name") or "")
        blob = f"{fid} {name}".lower()
        if hint in blob:
            return r
    return runs[0]


def _status_norm(status: str | None) -> str:
    return (status or "").strip().lower()


def wait_for_run_output(
    event_id: str,
    timeout_s: float | None = None,
    poll_interval_s: float = 0.5,
) -> dict:
    if timeout_s is None:
        timeout_s = float(os.getenv("RAG_QUERY_POLL_TIMEOUT_S", "600"))
    start = time.time()
    last_status = None
    last_run_id = None
    while True:
        runs = fetch_runs(event_id)
        if runs:
            run = _pick_run_for_query(runs)
            status = run.get("status")
            last_status = status or last_status
            last_run_id = run.get("run_id") or run.get("runId") or last_run_id
            sn = _status_norm(status)
            if sn in ("completed", "succeeded", "success", "finished"):
                return run.get("output") or {}
            if sn in ("failed", "cancelled", "canceled"):
                raise RuntimeError(f"Function run {status}")
        if time.time() - start > timeout_s:
            detail = f"last status: {last_status!r}"
            if last_run_id:
                detail += f", run_id: {last_run_id!r}"
            detail += (
                f". Increase RAG_QUERY_POLL_TIMEOUT_S (seconds) if the pipeline is slow, "
                f"or check the Inngest UI / uvicorn logs if it stays stuck."
            )
            raise TimeoutError(f"Timed out waiting for run output ({detail})")
        time.sleep(poll_interval_s)


with st.form("rag_query_form"):
    question = st.text_input("Your question")
    top_k = st.number_input("How many chunks to retrieve", min_value=1, max_value=20, value=5, step=1)
    submitted = st.form_submit_button("Ask")

    if submitted and question.strip():
        with st.spinner("Sending event and generating answer..."):
            # Fire-and-forget event to Inngest for observability/workflow
            event_id = send_rag_query_event(question.strip(), int(top_k))
            # Poll the local Inngest API for the run's output
            output = wait_for_run_output(event_id)
            answer = output.get("answer", "")
            sources = output.get("sources", [])

        st.subheader("Answer")
        st.write(answer or "(No answer)")
        if sources:
            st.caption("Sources")
            for s in sources:
                st.write(f"- {s}")
