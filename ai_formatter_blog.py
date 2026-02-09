# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Formatage AI des contenus Blog WordPress (Silver -> Gold)
# MAGIC
# MAGIC Ce notebook formate les contenus blog bruts (raw_json) en markdown structure
# MAGIC et classifie le contenu via `AI_QUERY` (Databricks AI Functions).
# MAGIC
# MAGIC **Architecture Medallion :**
# MAGIC - **Source** : table silver `cegid_website_pages` (contenus standardises)
# MAGIC - **Cible** : table gold `gold_cegid_website_pages` (contenus enrichis par AI)
# MAGIC
# MAGIC **Scope:** Articles de blog (`post`) uniquement.
# MAGIC
# MAGIC **Pipeline AI en 2 etapes (CTE) :**
# MAGIC 1. Generation du markdown a partir du `raw_json`
# MAGIC 2. Classification du **markdown genere** :
# MAGIC    - `has_regulatory_content` : reference a une reglementation ou texte de loi
# MAGIC    - `has_country_specific_context` : contexte specifique a un pays ou marche
# MAGIC    - `funnel_stage` : stade du funnel marketing (TOFU / MOFU / BOFU)
# MAGIC
# MAGIC **Pre-requis:** Executer `blog_importer` avant pour alimenter la table silver.

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
CONTENT_TYPE = "post"

# Modele AI a utiliser
AI_MODEL = "databricks-claude-haiku-4-5"

# Taille des batchs
BATCH_SIZE = 5

# Nombre max d'items a traiter (None = tout traiter, ex: 5 pour les tests)
MAX_ITEMS = None

# Prompt systeme pour le formatage
AI_PROMPT = (
    "Tu es un expert en formatage de contenu web. "
    "Convertis ce JSON WordPress en markdown propre et structure. "
    "Utilise des titres (##, ###), des listes, et formate correctement les liens. "
    "Retourne uniquement le markdown, sans explications. "
    "JSON: "
)

# Prompt pour la classification reglementaire
AI_PROMPT_REGULATORY = (
    "Tu es un expert en analyse de contenu. "
    "Analyse ce contenu markdown d'un article de blog et determine s'il contient des references "
    "a une reglementation, un texte de loi, une directive, une norme, un decret ou tout cadre juridique/legal. "
    "Exemples : RGPD, DORA, loi de finances, code du travail, directive europeenne, norme ISO, etc. "
    "Reponds UNIQUEMENT par 'true' ou 'false', sans aucune explication. "
    "Contenu: "
)

# Prompt pour la classification contexte pays/marche
AI_PROMPT_COUNTRY_SPECIFIC = (
    "Tu es un expert en analyse de contenu. "
    "Analyse ce contenu markdown d'un article de blog et determine s'il presente un contexte "
    "specifique a un pays ou un marche particulier (par exemple : fiscalite francaise, marche espagnol, "
    "legislation italienne, systeme de paie britannique, etc.). "
    "Un contenu generique ou international sans ancrage national doit retourner false. "
    "Reponds UNIQUEMENT par 'true' ou 'false', sans aucune explication. "
    "Contenu: "
)

# Prompt pour la classification funnel stage (TOFU / MOFU / BOFU)
AI_PROMPT_FUNNEL_STAGE = (
    "Tu es un expert en marketing B2B et en strategie de contenu pour l'achat de logiciels en ligne. "
    "Analyse ce contenu markdown d'un article de blog et determine a quel stade du funnel marketing "
    "il s'adresse pour un prospect dans un parcours d'achat de logiciel. "
    "Les 3 stades possibles sont : "
    "- TOFU (Top of Funnel) : contenu de sensibilisation et decouverte. Le prospect cherche a comprendre "
    "un sujet, une problematique ou une tendance. Exemples : articles educatifs, guides generaux, "
    "definitions de concepts, tendances du marche, bonnes pratiques generales. "
    "- MOFU (Middle of Funnel) : contenu d'evaluation et de consideration. Le prospect a identifie son besoin "
    "et compare les solutions possibles. Exemples : comparatifs de solutions, guides de choix, "
    "criteres de selection, cas d'usage detailles, webinars thematiques, livres blancs approfondis. "
    "- BOFU (Bottom of Funnel) : contenu de decision et de conversion. Le prospect est pret a acheter "
    "et cherche a valider son choix. Exemples : demos produit, etudes de cas clients avec resultats chiffres, "
    "temoignages clients, fiches produit detaillees, offres d'essai, ROI et benefices concrets. "
    "Reponds UNIQUEMENT par 'TOFU', 'MOFU' ou 'BOFU', sans aucune explication. "
    "Contenu: "
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

# Synchronise les donnees silver -> gold (insere les nouveaux, met a jour les metadonnees)
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
    Identifie les articles de blog dans la table gold qui necessitent un formatage AI.
    Criteres:
    - content_type = 'post'
    - raw_json non vide
    - content_text vide (nouveau) OU modifie recemment sans re-formatage
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

print(f"{total_count} article(s) de blog a traiter (gold)")
display(df_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Traitement par batch

# COMMAND ----------

def process_batch(gold_table: str, batch_ids: list, ai_model: str,
                  ai_prompt: str, ai_prompt_regulatory: str,
                  ai_prompt_country_specific: str,
                  ai_prompt_funnel_stage: str):
    """
    Traite un batch d'elements via AI_QUERY et met a jour la table gold via MERGE.
    Utilise un CTE en 2 etapes :
    1. Genere le content_text (markdown) a partir du raw_json
    2. Classifie le markdown genere (regulatory + country_specific + funnel_stage)
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)

    # Echapper les apostrophes pour SQL (' -> '')
    ai_prompt = ai_prompt.replace("'", "''")
    ai_prompt_regulatory = ai_prompt_regulatory.replace("'", "''")
    ai_prompt_country_specific = ai_prompt_country_specific.replace("'", "''")
    ai_prompt_funnel_stage = ai_prompt_funnel_stage.replace("'", "''")

    merge_query = f"""
    MERGE INTO {gold_table} AS target
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
            FROM {gold_table}
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
            AI_QUERY(
                '{ai_model}',
                CONCAT(
                    '{ai_prompt_funnel_stage}',
                    mg.new_content_text
                )
            ) AS new_funnel_stage_raw,
            CURRENT_TIMESTAMP() AS new_date_formatted
        FROM markdown_generated mg
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        content_text = source.new_content_text,
        has_regulatory_content = CASE LOWER(TRIM(source.new_regulatory_raw)) WHEN 'true' THEN true ELSE false END,
        has_country_specific_context = CASE LOWER(TRIM(source.new_country_specific_raw)) WHEN 'true' THEN true ELSE false END,
        funnel_stage = CASE UPPER(TRIM(source.new_funnel_stage_raw)) WHEN 'TOFU' THEN 'TOFU' WHEN 'MOFU' THEN 'MOFU' WHEN 'BOFU' THEN 'BOFU' ELSE NULL END,
        date_formatted = source.new_date_formatted
    """

    spark.sql(merge_query)


def run_ai_formatting(gold_table: str = GOLD_TABLE_FULL,
                      batch_size: int = BATCH_SIZE,
                      max_items: int = MAX_ITEMS,
                      ai_model: str = AI_MODEL,
                      ai_prompt: str = AI_PROMPT,
                      ai_prompt_regulatory: str = AI_PROMPT_REGULATORY,
                      ai_prompt_country_specific: str = AI_PROMPT_COUNTRY_SPECIFIC,
                      ai_prompt_funnel_stage: str = AI_PROMPT_FUNNEL_STAGE):
    """
    Execute le formatage AI et la classification sur tous les elements en attente dans la table gold.
    max_items: nombre max d'items a traiter (None = tout traiter).
    """
    df = get_items_to_process(gold_table)
    all_ids = [row["id"] for row in df.select("id").collect()]

    if max_items is not None:
        all_ids = all_ids[:max_items]

    total = len(all_ids)

    if total == 0:
        print("Aucun element a traiter.")
        return 0

    batches = [all_ids[i:i + batch_size] for i in range(0, total, batch_size)]
    nb_batches = len(batches)

    print(f"Traitement de {total} element(s) en {nb_batches} batch(s) de {batch_size} max")
    print(f"Modele: {ai_model}")
    print(f"Table gold: {gold_table}")
    print(f"{'='*60}")

    processed = 0

    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"\nBatch {idx}/{nb_batches} ({batch_len} element(s))...")

        try:
            process_batch(gold_table, batch_ids, ai_model, ai_prompt,
                         ai_prompt_regulatory, ai_prompt_country_specific,
                         ai_prompt_funnel_stage)
            processed += batch_len
            print(f"  OK - {processed}/{total} traite(s)")
        except Exception as e:
            print(f"  ERREUR sur le batch {idx}: {e}")
            print(f"  IDs concernes: {batch_ids}")
            continue

    print(f"\n{'='*60}")
    print(f"Formatage termine: {processed}/{total} element(s) traite(s)")

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

# Apercu des derniers articles formates (gold)
display(spark.sql(f"""
    SELECT
        id,
        title,
        LEFT(content_text, 300) AS content_preview,
        has_regulatory_content,
        has_country_specific_context,
        funnel_stage,
        date_formatted,
        date_modified
    FROM {GOLD_TABLE_FULL}
    WHERE date_formatted IS NOT NULL
        AND content_type = '{CONTENT_TYPE}'
    ORDER BY date_formatted DESC
    LIMIT 20
"""))

# COMMAND ----------

# Statistiques de formatage des articles (gold)
display(spark.sql(f"""
    SELECT
        site_id,
        COUNT(*) AS total,
        COUNT(date_formatted) AS formatted,
        COUNT(*) - COUNT(date_formatted) AS remaining,
        SUM(CASE WHEN has_regulatory_content = true THEN 1 ELSE 0 END) AS with_regulatory,
        SUM(CASE WHEN has_country_specific_context = true THEN 1 ELSE 0 END) AS with_country_specific,
        SUM(CASE WHEN funnel_stage = 'TOFU' THEN 1 ELSE 0 END) AS funnel_tofu,
        SUM(CASE WHEN funnel_stage = 'MOFU' THEN 1 ELSE 0 END) AS funnel_mofu,
        SUM(CASE WHEN funnel_stage = 'BOFU' THEN 1 ELSE 0 END) AS funnel_bofu
    FROM {GOLD_TABLE_FULL}
    WHERE content_type = '{CONTENT_TYPE}'
    GROUP BY site_id
    ORDER BY site_id
"""))
