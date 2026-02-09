# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Formatage AI des Use Cases (Silver -> Gold)
# MAGIC
# MAGIC Ce notebook formate les contenus use cases bruts (raw_json) en markdown structure
# MAGIC et en extrait des **key figures** via `AI_QUERY` (Databricks AI Functions).
# MAGIC
# MAGIC **Architecture Medallion :**
# MAGIC - **Source** : table PLT `cegid_website_plt` (contenus standardises)
# MAGIC - **Cible** : table GLD `cegid_website_gld` (contenus enrichis par IA)
# MAGIC
# MAGIC **Scope:** Etudes de cas (`study_case`).
# MAGIC
# MAGIC **Pre-requis:** Executer `study_case_importer` avant pour alimenter la table silver.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import des utilitaires communs

# COMMAND ----------

# MAGIC %run ./wordpress_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# Tables source (silver) et cible (gold)
SILVER_TABLE_FULL = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{SILVER_TABLE}"
GOLD_TABLE_FULL = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{GOLD_TABLE}"

# Type de contenu traite
CONTENT_TYPE = "study_case"

# Modele AI a utiliser
AI_MODEL = "databricks-claude-haiku-4-5"

# Taille des batchs
BATCH_SIZE = 5

# Nombre max d'items a traiter (None = tout traiter, ex: 5 pour les tests)
MAX_ITEMS = None

# System prompt for use case markdown formatting
AI_PROMPT_CONTENT = (
    "You are an expert in web content formatting. "
    "Convert this WordPress JSON of a case study into clean, well-structured markdown. "
    "Use headings (##, ###), lists, and format links properly. "
    "Highlight key results and customer testimonials. "
    "Return only the markdown, without any explanations. "
    "JSON: "
)

# System prompt for key figures extraction
AI_PROMPT_KEY_FIGURES = (
    "You are an expert in marketing content analysis. "
    "From this WordPress JSON of a case study, extract the key figures. "
    "Key figures are notable quantitative indicators: improvement percentages, "
    "time savings, cost savings, number of users, ROI, etc. "
    "Return ONLY a JSON array of strings, each string being a key figure with its context. "
    "Example: [\"30% cost reduction\", \"2x faster\", \"500 users deployed\"]. "
    "If no key figures are found, return an empty array: []. "
    "Return nothing other than the JSON array. "
    "JSON: "
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Initialisation de la table gold

# COMMAND ----------

# Cree la table gold si necessaire
create_delta_table(
    catalog=DATABRICKS_CATALOG,
    schema=DATABRICKS_SCHEMA,
    table_name=GOLD_TABLE,
    spark_schema=GOLD_SCHEMA,
    partition_by=["site_id", "content_type"]
)

# Synchronise les donnees silver -> gold
upsert_gold_from_silver(
    catalog=DATABRICKS_CATALOG,
    schema=DATABRICKS_SCHEMA,
    gold_table=GOLD_TABLE,
    silver_table=SILVER_TABLE,
    content_type=CONTENT_TYPE
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Identification des elements a traiter

# COMMAND ----------

def get_items_to_process(gold_table: str) -> list:
    """
    Identifie les study cases dans la table gold qui necessitent un formatage AI.
    """
    query = f"""
    SELECT
        id,
        title,
        slug,
        date_modified,
        CASE
            WHEN content_text IS NULL OR content_text = '' THEN 'Nouveau'
            ELSE 'Update'
        END AS statut
    FROM {gold_table}
    WHERE
        raw_json IS NOT NULL AND raw_json != ''
        AND content_type = '{CONTENT_TYPE}'
        AND (
            content_text IS NULL
            OR content_text = ''
            OR (date_modified >= CURRENT_DATE() - INTERVAL 7 DAYS
                AND date_modified > COALESCE(date_formatted, '1900-01-01'))
        )
    ORDER BY date_modified DESC
    """
    return spark.sql(query)


df_to_process = get_items_to_process(GOLD_TABLE_FULL)
total_count = df_to_process.count()

print(f"{total_count} use case(s) to process (gold)")
display(df_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Traitement par batch

# COMMAND ----------

def process_batch(gold_table: str, batch_ids: list, ai_model: str,
                  ai_prompt_content: str, ai_prompt_key_figures: str):
    """
    Traite un batch de study cases via AI_QUERY et met a jour la table gold via MERGE.
    - Genere le content_text (markdown) via le prompt contenu
    - Extrait les key_figures (array JSON) via le prompt key figures
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)

    # Echapper les apostrophes pour SQL (' -> '')
    ai_prompt_content = ai_prompt_content.replace("'", "''")
    ai_prompt_key_figures = ai_prompt_key_figures.replace("'", "''")

    merge_query = f"""
    MERGE INTO {gold_table} AS target
    USING (
        SELECT
            id,
            AI_QUERY(
                '{ai_model}',
                CONCAT(
                    '{ai_prompt_content}',
                    raw_json
                )
            ) AS new_content_text,
            AI_QUERY(
                '{ai_model}',
                CONCAT(
                    '{ai_prompt_key_figures}',
                    raw_json
                )
            ) AS new_key_figures_raw,
            CURRENT_TIMESTAMP() AS new_date_formatted
        FROM {gold_table}
        WHERE id IN ({ids_str})
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        content_text = source.new_content_text,
        key_figures = FROM_JSON(source.new_key_figures_raw, 'ARRAY<STRING>'),
        date_formatted = source.new_date_formatted
    """

    spark.sql(merge_query)


def run_ai_formatting(gold_table: str = GOLD_TABLE_FULL,
                      batch_size: int = BATCH_SIZE,
                      max_items: int = MAX_ITEMS,
                      ai_model: str = AI_MODEL,
                      ai_prompt_content: str = AI_PROMPT_CONTENT,
                      ai_prompt_key_figures: str = AI_PROMPT_KEY_FIGURES):
    """
    Execute le formatage AI sur tous les study cases en attente dans la table gold.
    max_items: nombre max d'items a traiter (None = tout traiter).
    """
    df = get_items_to_process(gold_table)
    all_ids = [row["id"] for row in df.select("id").collect()]

    if max_items is not None:
        all_ids = all_ids[:max_items]

    total = len(all_ids)

    if total == 0:
        print("No use cases to process.")
        return 0

    batches = [all_ids[i:i + batch_size] for i in range(0, total, batch_size)]
    nb_batches = len(batches)

    print(f"Processing {total} use case(s) in {nb_batches} batch(es) of {batch_size} max")
    print(f"Model: {ai_model}")
    print(f"Gold table: {gold_table}")
    print(f"{'='*60}")

    processed = 0

    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"\nBatch {idx}/{nb_batches} ({batch_len} item(s))...")

        try:
            process_batch(gold_table, batch_ids, ai_model,
                         ai_prompt_content, ai_prompt_key_figures)
            processed += batch_len
            print(f"  OK - {processed}/{total} processed")
        except Exception as e:
            print(f"  ERROR on batch {idx}: {e}")
            print(f"  IDs: {batch_ids}")
            continue

    print(f"\n{'='*60}")
    print(f"Formatting completed: {processed}/{total} use case(s) processed")

    return processed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Execution

# COMMAND ----------

total_processed = run_ai_formatting()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verification

# COMMAND ----------

# Apercu des derniers use cases formates (gold)
display(spark.sql(f"""
    SELECT
        id,
        title,
        LEFT(content_text, 300) AS content_preview,
        key_figures,
        date_formatted,
        date_modified
    FROM {GOLD_TABLE_FULL}
    WHERE date_formatted IS NOT NULL
        AND content_type = '{CONTENT_TYPE}'
    ORDER BY date_formatted DESC
    LIMIT 20
"""))

# COMMAND ----------

# Statistiques de formatage des use cases (gold)
display(spark.sql(f"""
    SELECT
        site_id,
        COUNT(*) AS total,
        COUNT(date_formatted) AS formatted,
        COUNT(*) - COUNT(date_formatted) AS remaining,
        COUNT(key_figures) AS with_key_figures
    FROM {GOLD_TABLE_FULL}
    WHERE content_type = '{CONTENT_TYPE}'
    GROUP BY site_id
    ORDER BY site_id
"""))
