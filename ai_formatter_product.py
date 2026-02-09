# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Formatage AI des Produits WordPress (Silver -> Gold)
# MAGIC
# MAGIC Ce notebook formate les contenus produits bruts (raw_json) en markdown structure
# MAGIC via `AI_QUERY` (Databricks AI Functions).
# MAGIC
# MAGIC **Architecture Medallion :**
# MAGIC - **Source** : table PLT `cegid_website_plt` (contenus standardises)
# MAGIC - **Cible** : table GLD `cegid_website_gld` (contenus enrichis par IA)
# MAGIC
# MAGIC **Scope:** Produits (`product`) uniquement.
# MAGIC
# MAGIC **Pre-requis:** Executer `product_importer` avant pour alimenter la table silver.

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
CONTENT_TYPE = "product"

# Modele AI a utiliser
AI_MODEL = "databricks-claude-haiku-4-5"

# Taille des batchs
BATCH_SIZE = 5

# Nombre max d'items a traiter (None = tout traiter, ex: 5 pour les tests)
MAX_ITEMS = None

# Prompt systeme pour le formatage des produits
AI_PROMPT = (
    "Tu es un expert en formatage de contenu web. "
    "Convertis ce JSON WordPress d'une page produit en markdown propre et structure. "
    "Le contenu provient de blocs ACF flexibles (header, arguments produit, features). "
    "Utilise des titres (##, ###), des listes, et formate correctement les liens. "
    "Mets en valeur les fonctionnalites cles et les arguments produit. "
    "Retourne uniquement le markdown, sans explications. "
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
    Identifie les produits dans la table gold qui necessitent un formatage AI.
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

print(f"{total_count} produit(s) a traiter (gold)")
display(df_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Traitement par batch

# COMMAND ----------

def process_batch(gold_table: str, batch_ids: list, ai_model: str, ai_prompt: str):
    """
    Traite un batch d'elements via AI_QUERY et met a jour la table gold via MERGE.
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)

    # Echapper les apostrophes pour SQL (' -> '')
    ai_prompt = ai_prompt.replace("'", "''")

    merge_query = f"""
    MERGE INTO {gold_table} AS target
    USING (
        SELECT
            id,
            AI_QUERY(
                '{ai_model}',
                CONCAT(
                    '{ai_prompt}',
                    raw_json
                )
            ) AS new_content_text,
            CURRENT_TIMESTAMP() AS new_date_formatted
        FROM {gold_table}
        WHERE id IN ({ids_str})
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        content_text = source.new_content_text,
        date_formatted = source.new_date_formatted
    """

    spark.sql(merge_query)


def run_ai_formatting(gold_table: str = GOLD_TABLE_FULL,
                      batch_size: int = BATCH_SIZE,
                      max_items: int = MAX_ITEMS,
                      ai_model: str = AI_MODEL,
                      ai_prompt: str = AI_PROMPT):
    """
    Execute le formatage AI sur tous les produits en attente dans la table gold.
    max_items: nombre max d'items a traiter (None = tout traiter).
    """
    df = get_items_to_process(gold_table)
    all_ids = [row["id"] for row in df.select("id").collect()]

    if max_items is not None:
        all_ids = all_ids[:max_items]

    total = len(all_ids)

    if total == 0:
        print("Aucun produit a traiter.")
        return 0

    batches = [all_ids[i:i + batch_size] for i in range(0, total, batch_size)]
    nb_batches = len(batches)

    print(f"Traitement de {total} produit(s) en {nb_batches} batch(s) de {batch_size} max")
    print(f"Modele: {ai_model}")
    print(f"Table gold: {gold_table}")
    print(f"{'='*60}")

    processed = 0

    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"\nBatch {idx}/{nb_batches} ({batch_len} element(s))...")

        try:
            process_batch(gold_table, batch_ids, ai_model, ai_prompt)
            processed += batch_len
            print(f"  OK - {processed}/{total} traite(s)")
        except Exception as e:
            print(f"  ERREUR sur le batch {idx}: {e}")
            print(f"  IDs concernes: {batch_ids}")
            continue

    print(f"\n{'='*60}")
    print(f"Formatage termine: {processed}/{total} produit(s) traite(s)")

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

# Apercu des derniers produits formates (gold)
display(spark.sql(f"""
    SELECT
        id,
        title,
        LEFT(content_text, 300) AS content_preview,
        date_formatted,
        date_modified
    FROM {GOLD_TABLE_FULL}
    WHERE date_formatted IS NOT NULL
        AND content_type = '{CONTENT_TYPE}'
    ORDER BY date_formatted DESC
    LIMIT 20
"""))

# COMMAND ----------

# Statistiques de formatage des produits (gold)
display(spark.sql(f"""
    SELECT
        site_id,
        COUNT(*) AS total,
        COUNT(date_formatted) AS formatted,
        COUNT(*) - COUNT(date_formatted) AS remaining
    FROM {GOLD_TABLE_FULL}
    WHERE content_type = '{CONTENT_TYPE}'
    GROUP BY site_id
    ORDER BY site_id
"""))
