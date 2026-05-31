"""
Page 4 — AI Assistant
Multi-LLM RAG chat: Gemini, Groq, or OpenAI
"""
import sys; sys.path.insert(0, ".")
import os
import streamlit as st
import chromadb
from chromadb.utils import embedding_functions
from dotenv import load_dotenv
load_dotenv()

st.set_page_config(page_title="AI Assistant", page_icon="💬", layout="wide")
st.title("💬 Pulse AI Assistant")
st.markdown("Ask anything about your pipeline data. Choose your preferred LLM below.")

# ── LLM + Model config — all deprecated models removed ──
LLM_MODELS = {
    "Gemini": [
        "gemini-2.0-flash",       # fastest, free tier ✓
        "gemini-2.5-flash",       # best balance
        "gemini-2.5-flash-lite",  # most budget friendly
        "gemini-2.5-pro",         # most capable
    ],
    "Groq": [
        "llama3-8b-8192",
        "llama3-70b-8192",
        "mixtral-8x7b-32768",
        "llama-3.1-8b-instant",
    ],
    "OpenAI": [
        "gpt-4.1-mini",   # cheapest — best for $5 budget ✓
        "gpt-4.1",
        "gpt-4o-mini",
        "gpt-4o",
    ],
}

# ── Sidebar — LLM selection ────────────────────────────
st.sidebar.header("🤖 LLM Configuration")

selected_llm = st.sidebar.selectbox(
    "Select LLM Provider",
    options=list(LLM_MODELS.keys()),
    index=0
)

selected_model = st.sidebar.selectbox(
    "Select Model",
    options=LLM_MODELS[selected_llm]
)

api_key_input = st.sidebar.text_input(
    f"{selected_llm} API Key",
    type="password",
    placeholder=f"Enter your {selected_llm} API key..."
)

# ── API Key validators — all using current non-deprecated models ──
def validate_gemini_key(key: str) -> tuple[bool, str]:
    try:
        import google.generativeai as genai
        genai.configure(api_key=key)
        model = genai.GenerativeModel("gemini-2.0-flash")
        model.generate_content("ping")
        return True, "✅ Gemini key valid"
    except Exception as e:
        err = str(e)[:120]
        return False, f"❌ Invalid Gemini key: {err}"


def validate_groq_key(key: str) -> tuple[bool, str]:
    try:
        from groq import Groq
        client = Groq(api_key=key)
        client.chat.completions.create(
            messages=[{"role": "user", "content": "ping"}],
            model="llama-3.1-8b-instant",
            max_tokens=5
        )
        return True, "✅ Groq key valid"
    except Exception as e:
        err = str(e)[:120]
        return False, f"❌ Invalid Groq key: {err}"


def validate_openai_key(key: str) -> tuple[bool, str]:
    try:
        from openai import OpenAI
        client = OpenAI(api_key=key)
        client.chat.completions.create(
            model="gpt-4.1-mini",
            messages=[{"role": "user", "content": "ping"}],
            max_tokens=5
        )
        return True, "✅ OpenAI key valid"
    except Exception as e:
        err = str(e)[:120]
        return False, f"❌ Invalid OpenAI key: {err}"


VALIDATORS = {
    "Gemini": validate_gemini_key,
    "Groq":   validate_groq_key,
    "OpenAI": validate_openai_key,
}

# ── Clear stale session state when provider changes ─
# the old validation state active
if st.session_state.get("validated_llm") != selected_llm:
    st.session_state.pop("validated_key",   None)
    st.session_state.pop("validated_llm",   None)
    st.session_state.pop("validated_model", None)

# ── Validate button ────────────────────────────────────
api_key_valid = False

if api_key_input:
    if st.sidebar.button("🔍 Validate API Key"):
        with st.sidebar:
            with st.spinner("Validating..."):
                valid, msg = VALIDATORS[selected_llm](api_key_input)
                if valid:
                    st.success(msg)
                    st.session_state["validated_key"]   = api_key_input
                    st.session_state["validated_llm"]   = selected_llm
                    st.session_state["validated_model"] = selected_model
                    api_key_valid = True
                else:
                    st.error(msg)
                    # Clear any stale valid state
                    st.session_state.pop("validated_key",   None)
                    st.session_state.pop("validated_llm",   None)
                    st.session_state.pop("validated_model", None)

# ── Check session for already-validated key ────────────
# Only accept if provider AND model both match current selection
if (
    st.session_state.get("validated_key") and
    st.session_state.get("validated_llm")   == selected_llm and
    st.session_state.get("validated_model") == selected_model
):
    api_key_valid = True
    active_key    = st.session_state["validated_key"]
    active_model  = st.session_state["validated_model"]
    st.sidebar.success(f"✅ {selected_llm} connected — {active_model}")
else:
    active_key   = None
    active_model = selected_model
    # If key exists but model changed — show re-validate hint
    if st.session_state.get("validated_key") and \
       st.session_state.get("validated_llm") == selected_llm:
        st.sidebar.info("ℹ Model changed — please re-validate your key.")

# ── Embedding section ──────────────────────────────────
st.sidebar.divider()
st.sidebar.header("🔢 Embedding Settings")

embedding_model = st.sidebar.selectbox(
    "Embedding Model",
    options=[
        "all-MiniLM-L6-v2 (local, fast)",
        "all-mpnet-base-v2 (local, accurate)",
    ],
    help="Runs locally — no API cost for embeddings"
)
embedding_model_name = embedding_model.split(" ")[0]

n_results = st.sidebar.slider(
    "Documents to retrieve per question",
    min_value=3,
    max_value=15,
    value=8,
    help="More = richer context but slower + costs more tokens"
)

max_tokens = st.sidebar.slider(
    "Max response tokens",
    min_value=100,
    max_value=1000,
    value=500,
    step=50,
    help="Lower = cheaper API cost. Good for $5 budget."
)

# ── Load ChromaDB ──────────────────────────────────────
CHROMA_DB_PATH = "./chroma_db"


@st.cache_resource
def load_chroma(emb_model: str):
    """
    Load persisted ChromaDB collection.
    @st.cache_resource keeps this in memory across reruns.
    """
    if not os.path.exists(CHROMA_DB_PATH):
        return None
    try:
        ef = embedding_functions.SentenceTransformerEmbeddingFunction(
            model_name=emb_model
        )
        client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
        col = client.get_collection(
            name="pulse_knowledge",
            embedding_function=ef
        )
        return col
    except Exception as e:
        st.error(f"ChromaDB error: {e}")
        return None


collection = load_chroma(embedding_model_name)

if collection is None:
    st.warning(
        "⚠️ ChromaDB index not found. "
        "Run this command first to build the index:"
    )
    st.code("python notebooks/rag_assistant.py", language="bash")
    st.stop()

doc_count = collection.count()
st.success(f"✓ Knowledge base ready — {doc_count:,} documents indexed")

# ── LLM answer function ────────────────────────────────
def ask_llm(
    question: str,
    context:  str,
    llm:      str,
    model:    str,
    key:      str,
    tokens:   int = 500
) -> str:

    prompt = f"""You are Pulse — an AI trend intelligence assistant.
        Answer ONLY using the pipeline data below. Be specific — cite numbers.
        If the data doesn't contain the answer, say so clearly.
        Do NOT use general knowledge outside the data provided.

        === PIPELINE DATA ===
        {context}
        === END DATA ===

        Question: {question}

        Answer (based strictly on pipeline data above):"""

    # ── Gemini ──────────────────────────────────────
    if llm == "Gemini":
        import google.generativeai as genai
        genai.configure(api_key=key)
        m = genai.GenerativeModel(model)
        response = m.generate_content(prompt)
        return response.text

    # ── Groq ────────────────────────────────────────
    elif llm == "Groq":
        from groq import Groq
        client = Groq(api_key=key)
        resp = client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model=model,
            max_tokens=tokens,
            temperature=0.3   # lower = more factual
        )
        return resp.choices[0].message.content

    # ── OpenAI ──────────────────────────────────────
    elif llm == "OpenAI":
        from openai import OpenAI
        client = OpenAI(api_key=key)
        resp = client.chat.completions.create(
            model=model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=tokens,
            temperature=0.3
        )
        return resp.choices[0].message.content

    return "❌ Unknown LLM selected."


def ask_rag(question: str) -> str:
    """Retrieve relevant docs from ChromaDB then ask LLM."""
    results = collection.query(
        query_texts=[question],
        n_results=n_results,
        include=["documents", "metadatas", "distances"]
    )
    docs  = results["documents"][0]
    metas = results["metadatas"][0]
    dists = results["distances"][0]

    # Build context with relevance scores
    context = "\n\n---\n\n".join([
        f"[{round((1 - d) * 100, 1)}% relevant | "
        f"{m.get('type', 'unknown')}]\n{doc}"
        for doc, m, d in zip(docs, metas, dists)
    ])

    return ask_llm(
        question=question,
        context=context,
        llm=selected_llm,
        model=active_model,
        key=active_key,
        tokens=max_tokens
    )


# ── Guard — must validate key first ───────────────────
if not api_key_valid:
    st.info(
        f"👈 **Step 1:** Select your LLM provider from the sidebar\n\n"
        f"👈 **Step 2:** Enter your **{selected_llm} API key**\n\n"
        f"👈 **Step 3:** Click **Validate API Key** to unlock the chat"
    )
    st.stop()

# ── Suggested questions ────────────────────────────────
st.markdown("**💡 Try these questions:**")
suggestions = [
        "Which programming languages are gaining momentum?",
        "What is the most popular content this week?",
        "What are developers most excited about on HackerNews?",
        "Which source is most active — HN, NewsAPI or GitHub?",
        "Which topics are people most excited about right now?",
        "Which programming language has the most negative sentiment?",
        "Is HackerNews more positive than NewsAPI today?",
        "Show me the top content with positive sentiment only",
        "Which companies are mentioned most in trending content?",
]

# Initialise messages early — needed before button logic
if "messages" not in st.session_state:
    st.session_state["messages"] = []

cols = st.columns(3)
for i, q in enumerate(suggestions):
    if cols[i % 3].button(q, key=f"sug_{i}", use_container_width=True):
        # Only add if not already the last pending message
        last = st.session_state["messages"]
        if not last or last[-1]["content"] != q:
            st.session_state["messages"].append(
                {"role": "user", "content": q}
            )
        # Flag that we need to generate a response
        st.session_state["pending_response"] = True

st.divider()

# ── Render full chat history ───────────────────────────
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# ── Generate response if last message is unanswered ───
# This handles BOTH suggestion button clicks AND typed input
def needs_response() -> bool:
    """
    Returns True if the last message in history is from
    the user with no assistant reply yet — meaning we
    must generate an answer now.
    """
    msgs = st.session_state.get("messages", [])
    if not msgs:
        return False
    return msgs[-1]["role"] == "user"

if needs_response():
    question = st.session_state["messages"][-1]["content"]
    with st.chat_message("assistant"):
        with st.spinner(f"⏳ Asking {selected_llm} ({active_model})..."):
            try:
                answer = ask_rag(question)
            except Exception as e:
                answer = f"❌ Error: {str(e)}"
        st.markdown(answer)
    # Save assistant reply to history
    st.session_state["messages"].append(
        {"role": "assistant", "content": answer}
    )
    # Clear the pending flag
    st.session_state.pop("pending_response", None)

# ── Chat input box (typed questions) ──────────────────
if prompt := st.chat_input("Ask about your pipeline data..."):
    st.session_state["messages"].append(
        {"role": "user", "content": prompt}
    )
    # Rerun so needs_response() picks it up cleanly above
    st.rerun()

# ── Sidebar controls ───────────────────────────────────
st.sidebar.divider()
st.sidebar.markdown("### 📊 Session Info")
st.sidebar.markdown(f"**Provider :** {selected_llm}")
st.sidebar.markdown(f"**Model &nbsp;&nbsp;&nbsp;:** {active_model}")
st.sidebar.markdown(f"**Docs &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;:** {n_results} per query")
st.sidebar.markdown(f"**Max tokens:** {max_tokens}")
st.sidebar.markdown(f"**Vector DB :** ChromaDB (local)")
st.sidebar.markdown(f"**Index size :** {doc_count:,} docs")

st.sidebar.divider()
if st.sidebar.button("🗑 Clear Chat History"):
    st.session_state["messages"] = []
    st.session_state.pop("pending_response", None)
    st.rerun()

if st.sidebar.button("🔄 Rebuild ChromaDB Index"):
    st.cache_resource.clear()
    st.sidebar.info(
        "Cache cleared. Run `python notebooks/rag_assistant.py` "
        "then refresh this page."
    )