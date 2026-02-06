# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Formatage AI des Use Cases (Study Cases)
# MAGIC
# MAGIC Ce notebook formate les contenus use cases bruts (raw_json) en markdown structure
# MAGIC et en extrait des **key figures** via `AI_QUERY` (Databricks AI Functions).
# MAGIC
# MAGIC **Scope:** Etudes de cas (`study_case`).
# MAGIC Les articles de blog, landing pages et produits sont traites par `ai_formatter_blog`.
# MAGIC
# MAGIC **Pourquoi un notebook separe ?**
# MAGIC Les appels AI_QUERY sur de gros volumes provoquent des timeouts du serverless compute.
# MAGIC Ce notebook traite les elements par batch (defaut: 5) pour eviter ce probleme.
# MAGIC
# MAGIC **Pre-requis:** Executer `study_case_importer` avant pour alimenter la table `cegid_website_pages`.

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

# Prompt pour le formatage du contenu markdown
AI_PROMPT_CONTENT = (
    "Tu es un expert en formatage de contenu web. "
    "Convertis ce JSON WordPress d'une etude de cas en markdown propre et structure. "
    "Utilise des titres (##, ###), des listes, et formate correctement les liens. "
    "Mets en valeur les resultats cles et les temoignages clients. "
    "Retourne uniquement le markdown, sans explications. "
    "JSON: "
)

# Prompt pour l'extraction des key figures
AI_PROMPT_KEY_FIGURES = (
    "Tu es un expert en analyse de contenu marketing. "
    "A partir de ce JSON WordPress d'une etude de cas, extrais les chiffres cles (key figures). "
    "Les key figures sont des indicateurs quantitatifs marquants : pourcentages d'amelioration, "
    "gains de temps, economies realisees, nombre d'utilisateurs, ROI, etc. "
    "Retourne UNIQUEMENT un JSON array de strings, chaque string etant un chiffre cle avec son contexte. "
    "Exemple: [\"30% de reduction des couts\", \"2x plus rapide\", \"500 utilisateurs deployes\"]. "
    "Si aucun chiffre cle n'est trouve, retourne un array vide: []. "
    "Ne retourne rien d'autre que le JSON array. "
    "JSON: "
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Migration de schema (ajout colonne key_figures)

# COMMAND ----------

def ensure_key_figures_column(source_table: str):
    """
    Ajoute la colonne key_figures a la table si elle n'existe pas deja.
    Type: ARRAY<STRING> pour stocker les chiffres cles extraits par l'IA.
    """
    try:
        # Verifie si la colonne existe deja
        columns = [col.name for col in spark.table(source_table).schema]
        if "key_figures" not in columns:
            spark.sql(f"ALTER TABLE {source_table} ADD COLUMN key_figures ARRAY<STRING>")
            print("Colonne 'key_figures' ajoutee a la table")
        else:
            print("Colonne 'key_figures' deja presente")
    except Exception as e:
        print(f"Verification/ajout colonne key_figures: {e}")


ensure_key_figures_column(SOURCE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Identification des elements a traiter

# COMMAND ----------

def get_items_to_process(source_table: str) -> list:
    """
    Identifie les study cases qui necessitent un formatage AI.
    Criteres:
    - content_type = 'study_case'
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
        AND content_type = 'study_case'
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

print(f"{total_count} use case(s) a traiter")
display(df_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Traitement par batch

# COMMAND ----------

def process_batch(source_table: str, batch_ids: list, ai_model: str,
                  ai_prompt_content: str, ai_prompt_key_figures: str):
    """
    Traite un batch de study cases via AI_QUERY et met a jour la table via MERGE.
    - Genere le content_text (markdown) via le prompt contenu
    - Extrait les key_figures (array JSON) via le prompt key figures

    Args:
        source_table: Nom complet de la table Delta
        batch_ids: Liste des IDs (LongType) a traiter dans ce batch
        ai_model: Nom du modele AI Databricks
        ai_prompt_content: Prompt de formatage contenu
        ai_prompt_key_figures: Prompt d'extraction des key figures
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
        FROM {source_table}
        WHERE id IN ({ids_str})
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        content_text = source.new_content_text,
        key_figures = FROM_JSON(source.new_key_figures_raw, 'ARRAY<STRING>'),
        date_formatted = source.new_date_formatted
    """

    spark.sql(merge_query)


def run_ai_formatting(source_table: str = SOURCE_TABLE,
                      batch_size: int = BATCH_SIZE,
                      ai_model: str = AI_MODEL,
                      ai_prompt_content: str = AI_PROMPT_CONTENT,
                      ai_prompt_key_figures: str = AI_PROMPT_KEY_FIGURES):
    """
    Execute le formatage AI sur tous les study cases en attente, par batch.

    Args:
        source_table: Nom complet de la table Delta
        batch_size: Nombre d'elements par batch
        ai_model: Nom du modele AI Databricks
        ai_prompt_content: Prompt de formatage contenu
        ai_prompt_key_figures: Prompt d'extraction des key figures

    Returns:
        Nombre total d'elements traites
    """
    # Recupere les IDs a traiter
    df = get_items_to_process(source_table)
    all_ids = [row["id"] for row in df.select("id").collect()]
    total = len(all_ids)

    if total == 0:
        print("Aucun use case a traiter.")
        return 0

    # Decoupe en batchs
    batches = [all_ids[i:i + batch_size] for i in range(0, total, batch_size)]
    nb_batches = len(batches)

    print(f"Traitement de {total} use case(s) en {nb_batches} batch(s) de {batch_size} max")
    print(f"Modele: {ai_model}")
    print(f"{'='*60}")

    processed = 0

    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"\nBatch {idx}/{nb_batches} ({batch_len} element(s))...")

        try:
            process_batch(source_table, batch_ids, ai_model,
                         ai_prompt_content, ai_prompt_key_figures)
            processed += batch_len
            print(f"  OK - {processed}/{total} traite(s)")
        except Exception as e:
            print(f"  ERREUR sur le batch {idx}: {e}")
            print(f"  IDs concernes: {batch_ids}")
            # Continue avec le batch suivant
            continue

    print(f"\n{'='*60}")
    print(f"Formatage termine: {processed}/{total} use case(s) traite(s)")

    return processed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Execution

# COMMAND ----------

# =============================================================================
# Lancer le formatage AI par batch pour les use cases
# =============================================================================

total_processed = run_ai_formatting()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verification

# COMMAND ----------

# Apercu des derniers use cases formates
display(spark.sql(f"""
    SELECT
        id,
        title,
        LEFT(content_text, 300) AS content_preview,
        key_figures,
        date_formatted,
        date_modified
    FROM {SOURCE_TABLE}
    WHERE date_formatted IS NOT NULL
        AND content_type = 'study_case'
    ORDER BY date_formatted DESC
    LIMIT 20
"""))

# COMMAND ----------

# Statistiques de formatage des use cases
display(spark.sql(f"""
    SELECT
        site_id,
        COUNT(*) AS total,
        COUNT(date_formatted) AS formatted,
        COUNT(*) - COUNT(date_formatted) AS remaining,
        COUNT(key_figures) AS with_key_figures
    FROM {SOURCE_TABLE}
    WHERE content_type = 'study_case'
    GROUP BY site_id
    ORDER BY site_id
"""))
