# Zomato Data Engineering Pipeline

[![Warehouse](https://img.shields.io/badge/warehouse-Snowflake-29B5E8?logo=snowflake&logoColor=white)](https://www.snowflake.com/)
[![Transform](https://img.shields.io/badge/transform-dbt%201.12-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Orchestration](https://img.shields.io/badge/orchestration-Airflow%203-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![License](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

A batch analytics pipeline for a food-delivery marketplace: it loads seven raw
tables into Snowflake, models them into a star schema with dbt, has an LLM read
every customer review, and folds those labels back into the warehouse as another
dbt model. One Airflow DAG runs the whole thing daily.

On top of the warehouse sit two Streamlit apps: one answers questions in plain
English by writing its own SQL, the other does retrieval-augmented chat over the
review text.

![Architecture](docs/architecture.png)

---

## What runs, in order

Everything is the `zomato_batch` DAG ([`airflow/dags/zomato_batch.py`](airflow/dags/zomato_batch.py)),
scheduled `@daily`, four tasks in a straight line. The order matters — task 3 needs
the staging models from task 2, and task 4 needs the table task 3 writes.

```
reload_raw ──▶ dbt_build_core ──▶ enrich_reviews ──▶ dbt_build_ai
```

**1 · `reload_raw`** — `COPY INTO` seven tables in `ZOMATO.RAW` from an S3 external
stage. Reference data (`restaurants`, `users`, `food`, `menu`) loads with
`ON_ERROR='CONTINUE'` so one malformed row doesn't sink the batch; the
transactional tables (`orders`, `order_items`, `reviews`) load strictly, because a
silently dropped order would corrupt every revenue number downstream.

**2 · `dbt_build_core`** — `dbt build` over 7 staging views and 10 mart tables,
tests included. `build` rather than `run` so a failing test stops the DAG instead
of publishing bad marts.

**3 · `enrich_reviews`** — [`ai/enrich_reviews.py`](ai/enrich_reviews.py) sends each
un-enriched review to Gemini and writes the labels to `ZOMATO.AI.REVIEW_ENRICHED`.
It selects only reviews whose `review_id` isn't already in the output table, so the
task is **incremental and safe to re-run** — a retry costs nothing and re-processes
nothing.

**4 · `dbt_build_ai`** — `dbt build --select tag:ai`, which is just
`mart_review_insights`: the model that joins the LLM labels back onto
`stg_reviews`. Tagging it keeps the AI-dependent model out of the core build, so
an API outage in step 3 can't fail step 2's marts.

---

## The warehouse

Four schemas in the `ZOMATO` database, created by
[`snowflake/01_setup.sql`](snowflake/01_setup.sql):

| Schema | Holds | Written by |
|---|---|---|
| `RAW` | Landing tables, straight from S3 | `COPY INTO` |
| `STAGING` | Cleaned and conformed views | dbt |
| `MARTS` | Dimensions, facts, aggregates | dbt |
| `AI` | Per-review LLM labels | `enrich_reviews.py` |

Compute is one `XSMALL` warehouse with `AUTO_SUSPEND = 60`, which is what makes
the project runnable on a Snowflake trial.

### Staging — 7 views

`stg_orders`, `stg_order_items`, `stg_restaurants`, `stg_users`, `stg_food`,
`stg_menu`, `stg_reviews`.

Materialized as views: they are pure renaming and cleanup, so there's nothing to
gain from persisting them. The one piece of real logic lives in `stg_orders`,
where `restaurant_city` arrives with leading junk and gets cut down to the useful
part:

```sql
trim(coalesce(regexp_substr(restaurant_city, '[^,]+$'), restaurant_city)) as city
```

It also derives `is_delivered` from `order_status` once, so no downstream model
has to remember the magic string `'Delivered'`.

### Marts — 10 tables

**Dimensions** — `dim_customer`, `dim_restaurants`, `dim_food`, `dim_date`.
`dim_customer` buckets ages into `Gen Z / Millennial / Gen X / Boomers` with an
explicit `Unknown` for nulls. `dim_date` is a generated spine, not a source table —
1,200 days from `2024-01-01` via `table(generator())`.

**Facts** — `fct_orders`, `fct_order_items`.

**Aggregates** — the four tables the apps actually read:

| Model | Grain | Answers |
|---|---|---|
| `mart_daily_city_revenue` | day × city | GMV, AOV, cancel rate |
| `mart_restaurant_performance` | restaurant | orders, revenue, rating, cancel rate |
| `mart_delivery_sla` | city × hour of day | p50 / p90 delivery time, late rate |
| `mart_review_insights` | city × topic × sentiment | review volume, avg sentiment, flagged issues |

### How the metrics are defined

Worth stating plainly, because these are the numbers everything else rests on:

- **GMV** counts **delivered** revenue only — `sum(iff(is_delivered, sales_amount, 0))`.
  A cancelled order contributes nothing.
- **AOV** divides that GMV by delivered orders, not by all orders, so the two stay
  consistent.
- **Cancel rate** is `order_status = 'Cancelled'` over *all* orders.
- **Late rate** is delivered orders over **45 minutes**, as a share of delivered
  orders. The 45-minute SLA is the one hard-coded business assumption in the project.
- Every ratio uses `div0()`, so a city with zero orders on a given day yields `0`
  instead of a division error that would break the batch.

### Tests

Defined in [`zomato/models/marts/_marts.yml`](zomato/models/marts/_marts.yml) —
`unique` and `not_null` on `fct_orders.order_id`, a `relationships` test from
`customer_id` to `dim_customer`, and `accepted_values` on `order_status`
(`Delivered`, `Cancelled`, `Refunded`). Because the DAG runs `dbt build`, any of
these failing halts the pipeline before the marts are published.

---

## The AI layer

### Review enrichment — `ai/enrich_reviews.py`

Gemini classifies each review into a **Pydantic schema** passed to the API as
`response_schema`, so the model returns validated JSON rather than prose that
needs parsing:

```python
class ReviewLabels(BaseModel):
    sentiment_label: Literal["positive", "negative", "neutral"]
    sentiment_score: float
    topic: Literal["food quality", "delivery", "service",
                   "packaging", "pricing", "other"]
    key_issue: Optional[str]
```

`temperature=0` for reproducibility, and the prompt requires `sentiment_score` to
agree in sign with `sentiment_label` — the cheapest guard against a model that
labels a review negative and scores it `+0.8`. Retries use exponential backoff and
only on `429`, `500`, `503`; anything else raises immediately instead of burning
quota on a request that will never succeed.

The output table records the `model` name and `enriched_at` per row, so labels
stay attributable when the model version changes.

> `SAMPLE_N = 5` caps each run at five reviews — a free-tier guardrail. Raise it
> when you're running against real quota.

### Text-to-SQL — `ai/text_to_sql.py`

A Streamlit app that turns a plain-English question into Snowflake SQL. The
warehouse schema is pasted into the system prompt, and the answer comes back as
JSON (`{"sql": "..."}`) rather than a code block to parse.

Two layers keep it read-only: the prompt says SELECT-only, and — because a prompt
is not a security control — a `FORBIDDEN_WORDS` list blocks `drop`, `delete`,
`truncate`, `alter`, `update`, `insert`, `create`, `replace`, `grant` and `revoke`
before anything reaches the connector. The model is also told to prefer `MART_`
tables over raw facts when either would answer the question.

### Review chat — `ai/rag_chat.py`

Retrieval-augmented chat over review text: embeds reviews with
`gemini-embedding-001`, keeps a local parquet cache so re-runs don't re-embed, and
retrieves top-k matches for each question. Batches embeddings and rate-limits to
90 requests/minute against the free tier's 100.

---

## The stack

| Layer | Tool |
|---|---|
| Storage | S3 (external stage) |
| Warehouse | Snowflake |
| Transformation | dbt 1.12 (`dbt-snowflake`) |
| Orchestration | Airflow 3, Docker Compose |
| LLM | Google Gemini (`google-genai`) |
| Apps | Streamlit |

---

## Running it

**Prerequisites:** a Snowflake account, an S3 bucket, a Gemini API key, Docker
(for Airflow), and Python 3.11+.

### 1 · Snowflake

Run the scripts in order, as `ACCOUNTADMIN`:

```
snowflake/01_setup.sql               warehouse, database, schemas, DBT_ROLE
snowflake/02_storage_integration.sql S3 storage integration
snowflake/03_stage_and_formats.sql   external stage + file formats
snowflake/04_raw_tables.sql          the 7 RAW tables
snowflake/05_copy_into.sql           first manual load
```

Between steps 2 and 3, apply the IAM policies in [`aws/iam/`](aws/iam) — the trust
policy is a two-pass affair: apply `snowflake-role-trust-policy-initial.json`,
run `DESC INTEGRATION` in Snowflake to get the generated ARN and external ID, then
replace it with `snowflake-role-trust-policy-final.json`.

### 2 · Credentials

```bash
cp ai/.env.example ai/.env
cp airflow/.env.example airflow/.env
```

Fill in Snowflake and Gemini values. Both `.env` files are gitignored.

### 3 · dbt

```bash
pip install -r requirements.txt
cd zomato
dbt debug          # confirms the profile reaches Snowflake
dbt build
```

`profiles.yml` ships with placeholders — point dbt at your own copy rather than
editing the committed one.

### 4 · Airflow

```bash
cd airflow
docker compose up -d
```

`AIRFLOW_CONN_SNOWFLAKE_DEFAULT` is built from your `.env`, so the connection
exists as soon as the stack is up — no need to add it in the UI.

> **The `enrich_reviews` task does not run in Docker yet.** See
> [Known rough edges](#known-rough-edges) — the image and Compose file still carry
> the OpenAI-era configuration. The other three tasks work; run enrichment locally
> with `python ai/enrich_reviews.py` until the image is rebuilt.

### 5 · The apps

```bash
streamlit run ai/text_to_sql.py
streamlit run ai/rag_chat.py
```

---

## Notes on the design

**Why the AI model is tagged.** Splitting `dbt build` into a core pass and a
`tag:ai` pass means a Gemini outage degrades the pipeline instead of breaking it:
revenue, SLA and restaurant marts still build and still publish. Only review
insights go stale.

**Why enrichment is incremental.** `WHERE review_id NOT IN (SELECT review_id FROM
...ENRICHED)` makes the task idempotent. Airflow retries are free, and a failed
run costs only the reviews it hadn't reached.

**Why staging is views and marts are tables.** Staging does no aggregation, so
persisting it would spend storage to save nothing. The marts aggregate over the
full fact table and get read repeatedly by the apps, so they're materialized.

### Known rough edges

An honest list, since this is a portfolio project and someone will read the code:

- **The Airflow image predates the move from OpenAI to Gemini.**
  `airflow/Dockerfile` installs `openai` but not `google-genai`, `pydantic`,
  `python-dotenv` or `snowflake-connector-python`, and `docker-compose.yaml`
  forwards `OPENAI_API_KEY` while `ai/enrich_reviews.py` reads `GEMINI_API_KEY`.
  So `dbt_build_ai` runs on stale data unless enrichment is run outside the
  container. Fixing it means adding those packages to the image and swapping the
  forwarded variable.
- `airflow/Dockerfile` pins `dbt-snowflake==1.8.*` while `requirements.txt` pins
  `1.12.0`. The DAG runs dbt from the container venv, so the pipeline's real dbt
  version is 1.8 — the two should be reconciled.
- `snowflake/01_setup.sql` still creates a `BRONZE` schema and its comments mention
  Iceberg, Spark and OpenAI — leftovers from the same earlier design. The pipeline
  that exists uses `RAW` and Gemini. Stale comments, not stale code.
- `SAMPLE_N = 5` in `enrich_reviews.py` and `NEW_REVIEWS = 20` in `rag_chat.py` are
  free-tier limits, not production values.
- `zomato/profiles.yml` is committed with placeholder values. Convenient for
  reading, but a real deployment should keep profiles outside the repo entirely.
- There are no dbt snapshots yet, so dimensions overwrite rather than keeping SCD2
  history — `snapshots/` is scaffolded and empty.

---

## Repository layout

```
airflow/     DAG, Dockerfile, docker-compose
snowflake/   Numbered setup scripts, run in order
zomato/      dbt project — staging/, marts/, macros/
ai/          enrich_reviews.py, text_to_sql.py, rag_chat.py
aws/iam/     IAM policies for the Snowflake storage integration
docs/        Architecture diagram
```

## License

[MIT](LICENSE).
