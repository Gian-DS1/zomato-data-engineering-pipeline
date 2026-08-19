import os
import time

import numpy as np
import pandas as pd
import snowflake.connector
import streamlit as st
from dotenv import load_dotenv
from google import genai
from google.genai import types

load_dotenv()

EMBEDDING_MODEL = "gemini-embedding-001"
EMBEDDING_BATCH = 20
EMBEDDING_RPM = 90  # free tier allows 100 embed requests/min; each text counts as one
MAX_RETRIES = 5
MAX_RETRY_WAIT = 120  # never sleep longer than this on a 429
CHAT_MODEL = "gemini-3.5-flash-lite"
NEW_REVIEWS = 20
TOK_K = 5
CACHE_FILE = f"review_embeddings_{NEW_REVIEWS}.parquet"

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


def read_reviews_from_snowflake():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )

    query = f"""
        SELECT REVIEW_ID, CITY, RATING, COMMENT
        FROM ZOMATO.STAGING.STG_REVIEWS
        SAMPLE ({NEW_REVIEWS} ROWS)    
    """
    df = conn.cursor().execute(query).fetch_pandas_all()
    conn.close()

    df.columns = [col.lower() for col in df.columns]
    return df


_next_call_at = 0.0


def _retry_delay(error):
    details = error.details if isinstance(error.details, dict) else {}
    for detail in details.get("error", {}).get("details", []):
        if detail.get("@type", "").endswith("RetryInfo"):
            return float(detail.get("retryDelay", "0s").rstrip("s"))
    return None


def _is_daily_quota(error):
    """True when the 429 is the per-day quota, which no amount of waiting fixes."""
    details = error.details if isinstance(error.details, dict) else {}
    for detail in details.get("error", {}).get("details", []):
        if detail.get("@type", "").endswith("QuotaFailure"):
            for violation in detail.get("violations", []):
                if "PerDay" in violation.get("quotaId", ""):
                    return True
    return False


def _embed_batch(batch, task_type):
    """Embeds one batch, pacing calls and backing off on 429 quota errors."""
    global _next_call_at

    for attempt in range(MAX_RETRIES):
        wait = _next_call_at - time.monotonic()
        if wait > 0:
            time.sleep(wait)

        try:
            response = client.models.embed_content(
                model=EMBEDDING_MODEL,
                contents=batch,
                config=types.EmbedContentConfig(task_type=task_type),
            )
            _next_call_at = time.monotonic() + 60.0 * len(batch) / EMBEDDING_RPM
            return [np.array(e.values, dtype=np.float32) for e in response.embeddings]
        except genai.errors.ClientError as e:
            if e.code != 429:
                raise
            if _is_daily_quota(e):
                raise RuntimeError(
                    f"Daily free-tier quota for {EMBEDDING_MODEL} is exhausted. "
                    f"Lower NEW_REVIEWS (currently {NEW_REVIEWS}) or wait for the "
                    f"reset at midnight Pacific Time."
                ) from e
            if attempt == MAX_RETRIES - 1:
                raise
            delay = min(_retry_delay(e) or 2**attempt, MAX_RETRY_WAIT)
            print(f"Rate limit hit, retrying in {delay:.0f}s...")
            _next_call_at = time.monotonic() + delay


def embed(texts, task_type="RETRIEVAL_DOCUMENT"):
    vectors = []

    for i in range(0, len(texts), EMBEDDING_BATCH):
        batch = texts[i : i + EMBEDDING_BATCH]
        vectors.extend(_embed_batch(batch, task_type))
        print(f"Embedded {len(vectors)}/{len(texts)} reviews")

    return vectors


@st.cache_data()
def load_reviews():
    if os.path.exists(CACHE_FILE):
        return pd.read_parquet(CACHE_FILE)

    df = read_reviews_from_snowflake()
    df["embedding"] = embed(df["comment"].tolist())

    df.to_parquet(CACHE_FILE, index=False)

    return df


st.title("Chat with your Zomato Reviews! 🍕🍔🍣")
st.caption(f"Searching {NEW_REVIEWS} review, answering with {CHAT_MODEL} model")


def consine_similarity(vec1, vec2):
    return np.dot(vec1, vec2) / (np.linalg.norm(vec1) * np.linalg.norm(vec2))


def find_similar_reviews(question, df):
    question_vector = embed([question])[0]
    score = []
    for review_vector in df["embedding"]:
        score.append(consine_similarity(question_vector, review_vector))
    df = df.copy()
    df["score"] = score
    return df.nlargest(TOK_K, "score")


def ask_llm(question, top_reviews):
    context = ""

    for _, row in top_reviews.iterrows():
        context += f" ({row['city']}, {row['rating']} stars) {row['comment']}\n"

    system_prompt = (
        "Answer ONLY using the customer reviews provided."
        "Be concise. If the reviews don't contain the answer, say \"I don't know\"."
    )

    user_prompt = f"Questions:\n{question}\n\nReviews: {context}"

    response = client.models.generate_content(
        model=CHAT_MODEL,
        contents=user_prompt,
        config=types.GenerateContentConfig(
            system_instruction=system_prompt,
            temperature=0.2,
        ),
    )
    return response.text


try:
    review_df = load_reviews()
except RuntimeError as e:
    st.error(str(e))
    st.stop()

question = st.text_input(
    "Ask a question about the reviews:",
    placeholder="e.g. What are the most common complaints?",
)

if question:
    top_reviews = find_similar_reviews(question, review_df)
    answer = ask_llm(question, top_reviews)

    st.markdown("**Answer:**")
    st.write(answer)

    with st.expander("Reviews used to build the answer"):
        st.dataframe(
            top_reviews[["city", "rating", "comment", "score"]], hide_index=True
        )
