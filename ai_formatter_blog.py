# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Formatage AI des contenus Blog WordPress (Silver -> Gold)
# MAGIC
# MAGIC Ce notebook formate les contenus blog bruts (raw_json) en markdown structure
# MAGIC et classifie le contenu via `AI_QUERY` (Databricks AI Functions).
# MAGIC
# MAGIC **Architecture Medallion :**
# MAGIC - **Source** : table PLT `cegid_website_plt` (contenus standardises)
# MAGIC - **Cible** : table GLD `cegid_website_gld` (contenus enrichis par IA)
# MAGIC
# MAGIC **Scope:** Articles de blog (`post`) uniquement.
# MAGIC
# MAGIC **Pipeline AI unifie (1 seul appel par item) :**
# MAGIC - Un prompt unique analyse le `raw_json` complet et retourne un JSON contenant :
# MAGIC   - La **classification** (funnel_stage, has_regulatory_content, has_country_specific_context)
# MAGIC   - Le **contenu markdown** formate
# MAGIC - Logique de classification waterfall : BOFU > MOFU > TOFU > fallback TOFU.
# MAGIC
# MAGIC **Champs enrichis :**
# MAGIC - `content_text` : contenu formate en markdown
# MAGIC - `has_regulatory_content` : reference a une reglementation ou texte de loi
# MAGIC - `has_country_specific_context` : contexte specifique a un pays ou marche
# MAGIC - `funnel_stage` : stade du funnel marketing (TOFU / MOFU / BOFU)
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

# --- Prompt unifie : markdown + classification en un seul appel AI ---
AI_PROMPT_UNIFIED = (
    "Role: You are an expert in B2B content marketing for Cegid (software vendor) "
    "and a web formatting specialist.\n\n"

    "Task: Analyze the provided WordPress JSON content (Title, Description, and Body) "
    "to perform two simultaneous actions:\n"
    "1. CLASSIFY the content strategically based on the Full Body analysis.\n"
    "2. CONVERT the body content into clean, well-structured Markdown.\n\n"

    "--- PART 1: CLASSIFICATION LOGIC (Waterfall) ---\n"
    "Apply these rules in strict order. Stop at the first match.\n\n"

    "1. Check for BOFU (Decision & Brand):\n"
    "   - Does the text explicitly pitch 'Cegid' or specific products "
    "(XRP, Flex, Talentsoft, Loop, Notilus, Echo, etc.)?\n"
    "   - Is it a customer success story, product update, price list, demo offer, "
    "or webinar replay about a product?\n"
    "   -> If YES: funnel_stage = 'BOFU'\n\n"

    "2. Check for MOFU (Consideration & Solution):\n"
    "   - Does the text recommend using 'a software', 'an ERP', 'a SIRH', "
    "'digital tools', or 'automation' to solve a problem?\n"
    "   - Is it a comparison (vs), a selection checklist, or a guide on "
    "'How to choose/digitize'?\n"
    "   -> If YES: funnel_stage = 'MOFU'\n\n"

    "3. Check for TOFU (Awareness & Education):\n"
    "   - Is the text purely educational (legal news, definitions, management tips, trends) "
    "without pushing a software solution?\n"
    "   - Does it explain 'Why' or broad concepts?\n"
    "   -> If YES: funnel_stage = 'TOFU'\n\n"

    "4. Fallback Rule:\n"
    "   - If ambiguous between general advice and software promotion, "
    "default to 'TOFU'.\n\n"

    "--- ADDITIONAL METADATA ---\n"
    "- has_regulatory_content (true/false): References regulations, laws, directives, decrees "
    "(GDPR, DORA, Finance Law, Labor Code, ISO, etc.).\n"
    "- has_country_specific_context (true/false): References a specific country or national market "
    "(e.g., French taxation, Spanish market).\n\n"

    "--- PART 2: MARKDOWN FORMATTING ---\n"
    "- Convert the HTML/JSON body content into clean Markdown.\n"
    "- Use proper headings (##, ###), bullet points, and format links [text](url).\n"
    "- Remove WordPress shortcodes or inline styles.\n\n"

    "--- OUTPUT FORMAT ---\n"
    "Return ONLY a single valid JSON object with the following structure "
    "(no markdown fences, no explanations):\n"
    '{"classification": {"funnel_stage": "TOFU", "has_regulatory_content": true, '
    '"has_country_specific_context": false}, '
    '"markdown_content": "# Title\\n\\n## Subheading\\n\\nContent here..."}\n\n'

    "Input JSON:\n"
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

print(f"{total_count} blog article(s) to process (gold)")
display(df_to_process)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Traitement par batch

# COMMAND ----------

def clean_ai_json(raw_response: str) -> str:
    """
    Nettoie la reponse AI qui peut contenir des fences markdown (```json ... ```)
    ou des espaces/retours en debut/fin.
    Extrait le JSON valide entre les premieres accolades { }.
    """
    if raw_response is None:
        return None
    text = raw_response.strip()
    # Supprime les fences markdown (```json ... ``` ou ``` ... ```)
    if text.startswith("```"):
        first_newline = text.find("\n")
        if first_newline != -1:
            text = text[first_newline + 1:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
    return text


def process_batch_unified(gold_table: str, batch_ids: list, ai_model: str,
                          ai_prompt: str):
    """
    Traitement unifie : classification + generation markdown en un seul appel AI.
    Le prompt demande au modele de retourner un JSON contenant :
    - classification.funnel_stage (TOFU/MOFU/BOFU)
    - classification.has_regulatory_content (true/false)
    - classification.has_country_specific_context (true/false)
    - markdown_content (contenu formate en markdown)
    Met a jour tous les champs enrichis dans la table gold.

    Strategie de parsing : on utilise clean_ai_json (UDF) pour nettoyer les
    eventuelles fences markdown avant GET_JSON_OBJECT.
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)
    ai_prompt_sql = ai_prompt.replace("'", "''")

    # Enregistre le UDF pour nettoyage des reponses AI
    spark.udf.register("clean_ai_json", clean_ai_json)

    merge_query = f"""
    MERGE INTO {gold_table} AS target
    USING (
        WITH ai_result AS (
            SELECT
                id,
                clean_ai_json(
                    AI_QUERY(
                        '{ai_model}',
                        CONCAT('{ai_prompt_sql}', raw_json)
                    )
                ) AS ai_json
            FROM {gold_table}
            WHERE id IN ({ids_str})
        )
        SELECT
            ar.id,
            GET_JSON_OBJECT(ar.ai_json, '$.markdown_content') AS new_content_text,
            GET_JSON_OBJECT(ar.ai_json, '$.classification.has_regulatory_content') AS regulatory_raw,
            GET_JSON_OBJECT(ar.ai_json, '$.classification.has_country_specific_context') AS country_raw,
            GET_JSON_OBJECT(ar.ai_json, '$.classification.funnel_stage') AS funnel_raw,
            CURRENT_TIMESTAMP() AS new_date_formatted
        FROM ai_result ar
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        content_text = source.new_content_text,
        has_regulatory_content = CASE LOWER(TRIM(source.regulatory_raw))
            WHEN 'true' THEN true ELSE false END,
        has_country_specific_context = CASE LOWER(TRIM(source.country_raw))
            WHEN 'true' THEN true ELSE false END,
        funnel_stage = CASE UPPER(TRIM(source.funnel_raw))
            WHEN 'TOFU' THEN 'TOFU'
            WHEN 'MOFU' THEN 'MOFU'
            WHEN 'BOFU' THEN 'BOFU'
            ELSE 'TOFU'
        END,
        date_formatted = source.new_date_formatted
    """
    spark.sql(merge_query)


def run_ai_formatting(gold_table: str = GOLD_TABLE_FULL,
                      batch_size: int = BATCH_SIZE,
                      max_items: int = MAX_ITEMS,
                      ai_model: str = AI_MODEL,
                      ai_prompt: str = AI_PROMPT_UNIFIED):
    """
    Execute le pipeline AI unifie en une seule passe :
    - Classification (funnel_stage, regulatory, country) + generation markdown
      en un seul appel AI par item.

    max_items: nombre max d'items a traiter (None = tout traiter).
    """
    df = get_items_to_process(gold_table)
    all_ids = [row["id"] for row in df.select("id").collect()]

    if max_items is not None:
        all_ids = all_ids[:max_items]

    total = len(all_ids)

    if total == 0:
        print("No items to process.")
        return 0

    batches = [all_ids[i:i + batch_size] for i in range(0, total, batch_size)]
    nb_batches = len(batches)

    print(f"Processing {total} item(s) in {nb_batches} batch(es) of {batch_size} max")
    print(f"Model: {ai_model}")
    print(f"Gold table: {gold_table}")
    print(f"{'='*60}")

    # --- Single pass: Markdown + Classification ---
    print(f"\n--- Unified AI pass: markdown + classification ---")
    processed = 0
    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"  Batch {idx}/{nb_batches} ({batch_len} item(s))...")
        try:
            process_batch_unified(gold_table, batch_ids, ai_model, ai_prompt)
            processed += batch_len
            print(f"    OK - {processed}/{total} processed")
        except Exception as e:
            print(f"    ERROR batch {idx}: {e}")
            print(f"    IDs: {batch_ids}")
            continue

    print(f"\n{'='*60}")
    print(f"Pipeline completed: {processed}/{total} item(s) processed")
    print(f"{'='*60}")

    return processed

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Execution

# COMMAND ----------

total_processed = run_ai_formatting()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Diagnostic - Analyse des reponses AI
# MAGIC
# MAGIC Ce bloc permet de verifier que le parsing JSON fonctionne correctement.
# MAGIC Il affiche les items recemment traites et identifie ceux ou le parsing a echoue
# MAGIC (content_text NULL malgre un date_formatted renseigne).

# COMMAND ----------

# Diagnostic : items traites recemment avec detail du parsing
df_diag = spark.sql(f"""
    SELECT
        id,
        title,
        CASE WHEN content_text IS NULL OR content_text = '' THEN 'EMPTY' ELSE 'OK' END AS markdown_status,
        CASE WHEN funnel_stage IS NULL THEN 'NULL' ELSE funnel_stage END AS funnel_status,
        has_regulatory_content,
        has_country_specific_context,
        LENGTH(content_text) AS content_length,
        date_formatted
    FROM {GOLD_TABLE_FULL}
    WHERE date_formatted IS NOT NULL
        AND content_type = '{CONTENT_TYPE}'
    ORDER BY date_formatted DESC
    LIMIT 20
""")

# Compte les items avec parsing echoue
total_diag = df_diag.count()
failed_markdown = df_diag.filter("markdown_status = 'EMPTY'").count()
failed_funnel = df_diag.filter("funnel_status = 'NULL'").count()

print(f"Diagnostic sur les {total_diag} derniers items traites :")
print(f"  - Markdown vide : {failed_markdown}/{total_diag}")
print(f"  - Funnel NULL   : {failed_funnel}/{total_diag}")
if failed_markdown > 0:
    print(f"  ATTENTION : {failed_markdown} item(s) avec content_text vide apres traitement AI.")
    print(f"  Cause probable : la reponse AI contient des fences markdown ou un format JSON inattendu.")
display(df_diag)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Debug approfondi (reponse AI brute)
# MAGIC
# MAGIC Decommenter et executer ce bloc pour voir la reponse brute de AI_QUERY
# MAGIC sur un item specifique. Utile pour diagnostiquer les problemes de parsing.

# COMMAND ----------

# # --- DECOMMENTER POUR DEBUG ---
# # Remplacer <ID> par l'id d'un item problematique
# debug_id = "<ID>"
# ai_prompt_sql = AI_PROMPT_UNIFIED.replace("'", "''")
# spark.udf.register("clean_ai_json", clean_ai_json)
#
# df_debug = spark.sql(f"""
#     SELECT
#         id,
#         title,
#         AI_QUERY(
#             '{AI_MODEL}',
#             CONCAT('{ai_prompt_sql}', raw_json)
#         ) AS raw_ai_response,
#         clean_ai_json(
#             AI_QUERY(
#                 '{AI_MODEL}',
#                 CONCAT('{ai_prompt_sql}', raw_json)
#             )
#         ) AS cleaned_ai_response
#     FROM {GOLD_TABLE_FULL}
#     WHERE id = {debug_id}
# """)
# display(df_debug)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification

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

# Statistiques de formatage et classification des articles (gold)
# Note: funnel_uncertain inclut les items pas encore traites (funnel_stage IS NULL)
display(spark.sql(f"""
    SELECT
        site_id,
        COUNT(*) AS total,
        COUNT(date_formatted) AS formatted,
        COUNT(*) - COUNT(date_formatted) AS remaining,
        SUM(CASE WHEN has_regulatory_content = true THEN 1 ELSE 0 END) AS with_regulatory,
        SUM(CASE WHEN has_regulatory_content IS NULL THEN 1 ELSE 0 END) AS regulatory_unknown,
        SUM(CASE WHEN has_country_specific_context = true THEN 1 ELSE 0 END) AS with_country_specific,
        SUM(CASE WHEN has_country_specific_context IS NULL THEN 1 ELSE 0 END) AS country_unknown,
        SUM(CASE WHEN funnel_stage = 'TOFU' THEN 1 ELSE 0 END) AS funnel_tofu,
        SUM(CASE WHEN funnel_stage = 'MOFU' THEN 1 ELSE 0 END) AS funnel_mofu,
        SUM(CASE WHEN funnel_stage = 'BOFU' THEN 1 ELSE 0 END) AS funnel_bofu,
        SUM(CASE WHEN funnel_stage = 'UNCERTAIN' OR funnel_stage IS NULL THEN 1 ELSE 0 END) AS funnel_uncertain
    FROM {GOLD_TABLE_FULL}
    WHERE content_type = '{CONTENT_TYPE}'
    GROUP BY site_id
    ORDER BY site_id
"""))
