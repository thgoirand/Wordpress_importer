# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Formatage AI des contenus Blog WordPress
# MAGIC
# MAGIC Ce notebook formate les contenus blog bruts (raw_json) en markdown structure
# MAGIC et classifie le contenu via `AI_QUERY` (Databricks AI Functions).
# MAGIC
# MAGIC **Scope:** Articles de blog (`post`) uniquement.
# MAGIC Chaque content type dispose de son propre formatter.
# MAGIC
# MAGIC **Pipeline AI en 2 etapes (CTE) :**
# MAGIC 1. Generation du markdown a partir du `raw_json`
# MAGIC 2. Classification du **markdown genere** (et non du JSON brut) :
# MAGIC    - `has_regulatory_content` : reference a une reglementation ou texte de loi
# MAGIC    - `has_country_specific_context` : contexte specifique a un pays ou marche
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

# Prompt pour la classification reglementaire (analyse le markdown genere)
AI_PROMPT_REGULATORY = (
    "Tu es un expert en analyse de contenu. "
    "Analyse ce contenu markdown d'un article de blog et determine s'il contient des references "
    "a une reglementation, un texte de loi, une directive, une norme, un decret ou tout cadre juridique/legal. "
    "Exemples : RGPD, DORA, loi de finances, code du travail, directive europeenne, norme ISO, etc. "
    "Reponds UNIQUEMENT par 'true' ou 'false', sans aucune explication. "
    "Contenu: "
)

# Prompt pour la classification contexte pays/marche (analyse le markdown genere)
AI_PROMPT_COUNTRY_SPECIFIC = (
    "Tu es un expert en analyse de contenu. "
    "Analyse ce contenu markdown d'un article de blog et determine s'il presente un contexte "
    "specifique a un pays ou un marche particulier (par exemple : fiscalite francaise, marche espagnol, "
    "legislation italienne, systeme de paie britannique, etc.). "
    "Un contenu generique ou international sans ancrage national doit retourner false. "
    "Reponds UNIQUEMENT par 'true' ou 'false', sans aucune explication. "
    "Contenu: "
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Migration de schema (ajout colonnes de classification)

# COMMAND ----------

def ensure_classification_columns(source_table: str):
    """
    Ajoute les colonnes de classification a la table si elles n'existent pas deja.
    - has_regulatory_content (BOOLEAN) : contient des references reglementaires/legales
    - has_country_specific_context (BOOLEAN) : contexte specifique a un pays/marche
    """
    columns = [col.name for col in spark.table(source_table).schema]

    for col_name in ["has_regulatory_content", "has_country_specific_context"]:
        if col_name not in columns:
            spark.sql(f"ALTER TABLE {source_table} ADD COLUMN {col_name} BOOLEAN")
            print(f"Colonne '{col_name}' ajoutee a la table")
        else:
            print(f"Colonne '{col_name}' deja presente")


ensure_classification_columns(SOURCE_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Identification des elements a traiter

# COMMAND ----------

def get_items_to_process(source_table: str) -> list:
    """
    Identifie les articles de blog qui necessitent un formatage AI.
    Criteres:
    - content_type = 'post'
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
        AND content_type = 'post'
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

print(f"{total_count} article(s) de blog a traiter")
display(df_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Traitement par batch

# COMMAND ----------

def process_batch(source_table: str, batch_ids: list, ai_model: str,
                  ai_prompt: str, ai_prompt_regulatory: str,
                  ai_prompt_country_specific: str):
    """
    Traite un batch d'elements via AI_QUERY et met a jour la table via MERGE.
    Utilise un CTE en 2 etapes :
    1. Genere le content_text (markdown) a partir du raw_json
    2. Classifie le markdown genere (regulatory + country_specific)

    Les classifications sont ainsi plus precises car elles analysent du contenu
    structure plutot que du JSON brut, et consomment moins de tokens.

    Args:
        source_table: Nom complet de la table Delta
        batch_ids: Liste des IDs (LongType) a traiter dans ce batch
        ai_model: Nom du modele AI Databricks
        ai_prompt: Prompt de formatage
        ai_prompt_regulatory: Prompt de classification reglementaire
        ai_prompt_country_specific: Prompt de classification contexte pays
    """
    # Construit la clause IN avec les IDs du batch
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)

    # Etape 1 (CTE) : generer le markdown a partir du raw_json
    # Etape 2 : utiliser le markdown genere pour les classifications regulatory et country_specific
    merge_query = f"""
    MERGE INTO {source_table} AS target
    USING (
        WITH markdown_generated AS (
            SELECT
                id,
                AI_QUERY(
                    '{ai_model}',
                    CONCAT(
                        '{ai_prompt}',
                        raw_json
                    )
                ) AS new_content_text
            FROM {source_table}
            WHERE id IN ({ids_str})
        )
        SELECT
            mg.id,
            mg.new_content_text,
            AI_QUERY(
                '{ai_model}',
                CONCAT(
                    '{ai_prompt_regulatory}',
                    mg.new_content_text
                )
            ) AS new_regulatory_raw,
            AI_QUERY(
                '{ai_model}',
                CONCAT(
                    '{ai_prompt_country_specific}',
                    mg.new_content_text
                )
            ) AS new_country_specific_raw,
            CURRENT_TIMESTAMP() AS new_date_formatted
        FROM markdown_generated mg
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        content_text = source.new_content_text,
        has_regulatory_content = CASE LOWER(TRIM(source.new_regulatory_raw)) WHEN 'true' THEN true ELSE false END,
        has_country_specific_context = CASE LOWER(TRIM(source.new_country_specific_raw)) WHEN 'true' THEN true ELSE false END,
        date_formatted = source.new_date_formatted
    """

    spark.sql(merge_query)


def run_ai_formatting(source_table: str = SOURCE_TABLE,
                      batch_size: int = BATCH_SIZE,
                      ai_model: str = AI_MODEL,
                      ai_prompt: str = AI_PROMPT,
                      ai_prompt_regulatory: str = AI_PROMPT_REGULATORY,
                      ai_prompt_country_specific: str = AI_PROMPT_COUNTRY_SPECIFIC):
    """
    Execute le formatage AI et la classification sur tous les elements en attente, par batch.

    Args:
        source_table: Nom complet de la table Delta
        batch_size: Nombre d'elements par batch
        ai_model: Nom du modele AI Databricks
        ai_prompt: Prompt de formatage
        ai_prompt_regulatory: Prompt de classification reglementaire
        ai_prompt_country_specific: Prompt de classification contexte pays

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
            process_batch(source_table, batch_ids, ai_model, ai_prompt,
                         ai_prompt_regulatory, ai_prompt_country_specific)
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
# MAGIC ## 6. Execution

# COMMAND ----------

# =============================================================================
# Lancer le formatage AI par batch
# =============================================================================

total_processed = run_ai_formatting()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verification

# COMMAND ----------

# Apercu des derniers articles formates
display(spark.sql(f"""
    SELECT
        id,
        title,
        LEFT(content_text, 300) AS content_preview,
        has_regulatory_content,
        has_country_specific_context,
        date_formatted,
        date_modified
    FROM {SOURCE_TABLE}
    WHERE date_formatted IS NOT NULL
        AND content_type = 'post'
    ORDER BY date_formatted DESC
    LIMIT 20
"""))

# COMMAND ----------

# Statistiques de formatage des articles
display(spark.sql(f"""
    SELECT
        site_id,
        COUNT(*) AS total,
        COUNT(date_formatted) AS formatted,
        COUNT(*) - COUNT(date_formatted) AS remaining,
        SUM(CASE WHEN has_regulatory_content = true THEN 1 ELSE 0 END) AS with_regulatory,
        SUM(CASE WHEN has_country_specific_context = true THEN 1 ELSE 0 END) AS with_country_specific
    FROM {SOURCE_TABLE}
    WHERE content_type = 'post'
    GROUP BY site_id
    ORDER BY site_id
"""))
