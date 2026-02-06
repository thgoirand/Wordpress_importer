# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Formatage AI des contenus Blog WordPress
# MAGIC
# MAGIC Ce notebook formate les contenus blog bruts (raw_json) en markdown structure
# MAGIC via `AI_QUERY` (Databricks AI Functions).
# MAGIC
# MAGIC **Scope:** Articles de blog (`post`), landing pages, produits.
# MAGIC Les study cases sont traites par `ai_formatter_use_case`.
# MAGIC
# MAGIC **Pourquoi un notebook separe ?**
# MAGIC Les appels AI_QUERY sur de gros volumes provoquent des timeouts du serverless compute.
# MAGIC Ce notebook traite les elements par batch (defaut: 5) pour eviter ce probleme.
# MAGIC
# MAGIC **Pre-requis:** Executer `blog_importer` avant pour alimenter la table `cegid_website_pages`.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import des utilitaires communs

# COMMAND ----------

# MAGIC %run ./wordpress_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

# Table source / cible
SOURCE_TABLE = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.cegid_website_pages"

# Modele AI a utiliser
AI_MODEL = "databricks-claude-haiku-4-5"

# Taille des batchs (nombre d'elements traites par requete AI_QUERY)
BATCH_SIZE = 5

# Prompt systeme pour le formatage
AI_PROMPT = (
    "Tu es un expert en formatage de contenu web. "
    "Convertis ce JSON WordPress en markdown propre et structure. "
    "Utilise des titres (##, ###), des listes, et formate correctement les liens. "
    "Retourne uniquement le markdown, sans explications. "
    "JSON: "
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Identification des elements a traiter

# COMMAND ----------

def get_items_to_process(source_table: str) -> list:
    """
    Identifie les elements qui necessitent un formatage AI.
    Criteres:
    - raw_json non vide
    - content_text vide (nouveau) OU modifie recemment sans re-formatage
    Retourne la liste des IDs a traiter.
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
    FROM {source_table}
    WHERE
        raw_json IS NOT NULL AND raw_json != ''
        AND content_type != 'study_case'
        AND (
            content_text IS NULL
            OR content_text = ''
            OR (date_modified >= CURRENT_DATE() - INTERVAL 7 DAYS
                AND date_modified > COALESCE(date_formatted, '1900-01-01'))
        )
    ORDER BY date_modified DESC
    """
    return spark.sql(query)


df_to_process = get_items_to_process(SOURCE_TABLE)
total_count = df_to_process.count()

print(f"{total_count} element(s) a traiter")
display(df_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Traitement par batch

# COMMAND ----------

def process_batch(source_table: str, batch_ids: list, ai_model: str, ai_prompt: str):
    """
    Traite un batch d'elements via AI_QUERY et met a jour la table via MERGE.

    Args:
        source_table: Nom complet de la table Delta
        batch_ids: Liste des IDs (LongType) a traiter dans ce batch
        ai_model: Nom du modele AI Databricks
        ai_prompt: Prompt de formatage
    """
    # Construit la clause IN avec les IDs du batch
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)

    merge_query = f"""
    MERGE INTO {source_table} AS target
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
        FROM {source_table}
        WHERE id IN ({ids_str})
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        content_text = source.new_content_text,
        date_formatted = source.new_date_formatted
    """

    spark.sql(merge_query)


def run_ai_formatting(source_table: str = SOURCE_TABLE,
                      batch_size: int = BATCH_SIZE,
                      ai_model: str = AI_MODEL,
                      ai_prompt: str = AI_PROMPT):
    """
    Execute le formatage AI sur tous les elements en attente, par batch.

    Args:
        source_table: Nom complet de la table Delta
        batch_size: Nombre d'elements par batch
        ai_model: Nom du modele AI Databricks
        ai_prompt: Prompt de formatage

    Returns:
        Nombre total d'elements traites
    """
    # Recupere les IDs a traiter
    df = get_items_to_process(source_table)
    all_ids = [row["id"] for row in df.select("id").collect()]
    total = len(all_ids)

    if total == 0:
        print("Aucun element a traiter.")
        return 0

    # Decoupe en batchs
    batches = [all_ids[i:i + batch_size] for i in range(0, total, batch_size)]
    nb_batches = len(batches)

    print(f"Traitement de {total} element(s) en {nb_batches} batch(s) de {batch_size} max")
    print(f"Modele: {ai_model}")
    print(f"{'='*60}")

    processed = 0

    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"\nBatch {idx}/{nb_batches} ({batch_len} element(s))...")

        try:
            process_batch(source_table, batch_ids, ai_model, ai_prompt)
            processed += batch_len
            print(f"  OK - {processed}/{total} traite(s)")
        except Exception as e:
            print(f"  ERREUR sur le batch {idx}: {e}")
            print(f"  IDs concernes: {batch_ids}")
            # Continue avec le batch suivant
            continue

    print(f"\n{'='*60}")
    print(f"Formatage termine: {processed}/{total} element(s) traite(s)")

    return processed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execution

# COMMAND ----------

# =============================================================================
# Lancer le formatage AI par batch
# =============================================================================

total_processed = run_ai_formatting()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verification

# COMMAND ----------

# Apercu des derniers elements formates
display(spark.sql(f"""
    SELECT
        id,
        title,
        LEFT(content_text, 300) AS content_preview,
        date_formatted,
        date_modified
    FROM {SOURCE_TABLE}
    WHERE date_formatted IS NOT NULL
    ORDER BY date_formatted DESC
    LIMIT 20
"""))

# COMMAND ----------

# Statistiques de formatage
display(spark.sql(f"""
    SELECT
        site_id,
        content_type,
        COUNT(*) AS total,
        COUNT(date_formatted) AS formatted,
        COUNT(*) - COUNT(date_formatted) AS remaining
    FROM {SOURCE_TABLE}
    GROUP BY site_id, content_type
    ORDER BY site_id, content_type
"""))
