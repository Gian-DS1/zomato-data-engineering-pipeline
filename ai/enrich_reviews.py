import os
import time
from typing import Literal, Optional

import snowflake.connector
from dotenv import load_dotenv
from google import genai
from google.genai import types
from pydantic import BaseModel

load_dotenv()

MODEL = os.getenv("GEMINI_MODEL", "gemini-3.5-flash-lite")

SAMPLE_N = 5
MAX_RETRIES = 3
TOPICS = ["food quality", "delivery", "service", "packaging", "pricing", "other"]

client = genai.Client(api_key=os.getenv("GEMINI_API_KEY"))


class ReviewLabels(BaseModel):
    sentiment_label: Literal["positive", "negative", "neutral"]
    sentiment_score: float
    topic: Literal[
        "food quality", "delivery", "service", "packaging", "pricing", "other"
    ]
    key_issue: Optional[str]


SYSTEM_PROMPT = f"""
You classify customer reviews for a food delivery app.

For the review you are given, return:
- sentiment_label: positive, negative, or neutral
- sentiment_score: a float between -1 and 1, where -1 is very negative, 0 is neutral, and 1 is very positive.
  It must agree with sentiment_label: negative reviews score below 0, positive reviews above 0.
- topic: one of {TOPICS}
- key_issue: a short phrase of 6 words or less that describes the main issue in the review, or null if there is no issue.
"""

GENERATION_CONFIG = types.GenerateContentConfig(
    system_instruction=SYSTEM_PROMPT,
    temperature=0,
    response_mime_type="application/json",
    response_schema=ReviewLabels,
)


def get_connection():
    return snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
    )


def create_output_table(cursor):
    cursor.execute("CREATE SCHEMA IF NOT EXISTS ZOMATO.AI")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS ZOMATO.AI.REVIEW_ENRICHED (
            review_id STRING PRIMARY KEY,
            sentiment_label STRING,
            sentiment_score FLOAT,
            topic STRING,
            key_issue STRING,
            model STRING,
            enriched_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """)


def get_reviews_to_enrich(cursor):
    cursor.execute(f"""
        SELECT review_id, COMMENT
        FROM ZOMATO.RAW.REVIEWS
        WHERE review_id NOT IN (SELECT review_id FROM ZOMATO.AI.REVIEW_ENRICHED)
        LIMIT {SAMPLE_N}
    """)
    return cursor.fetchall()


def classify_review(comment):
    for attempt in range(MAX_RETRIES):
        try:
            response = client.models.generate_content(
                model=MODEL,
                contents=comment,
                config=GENERATION_CONFIG,
            )
            return response.parsed
        except genai.errors.APIError as e:
            if attempt == MAX_RETRIES - 1 or e.code not in (429, 500, 503):
                raise
            time.sleep(2**attempt)


def save_results(cursor, results):
    cursor.executemany(
        """
            INSERT INTO ZOMATO.AI.REVIEW_ENRICHED (review_id, sentiment_label, sentiment_score, topic, key_issue, model)
            VALUES (%(review_id)s, %(sentiment_label)s, %(sentiment_score)s, %(topic)s, %(key_issue)s, %(model)s)
        """,
        results,
    )


def main():
    conn = get_connection()
    cursor = conn.cursor()

    create_output_table(cursor)

    reviews = get_reviews_to_enrich(cursor)

    if len(reviews) == 0:
        print("No new reviews to enrich.")
        return

    print(f"Enriching {len(reviews)} reviews with {MODEL}...")

    results = []
    for review_id, comment in reviews:
        try:
            labels = classify_review(comment)
            results.append(
                {
                    "review_id": review_id,
                    "sentiment_label": labels.sentiment_label,
                    "sentiment_score": labels.sentiment_score,
                    "topic": labels.topic,
                    "key_issue": labels.key_issue,
                    "model": MODEL,
                }
            )
        except Exception as e:
            print(f"Error occurred while classifying review {review_id}: {e}")

    if results:
        save_results(cursor, results)
        conn.commit()
        print(f"Saved {len(results)} enriched reviews.")
    else:
        print("No reviews were enriched successfully.")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
