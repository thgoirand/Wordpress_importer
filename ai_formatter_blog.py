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
# MAGIC **Pipeline AI en 2 passes :**
# MAGIC - **Passe 1 - Markdown** : convertit le `raw_json` en markdown propre et structure
# MAGIC   -> stocke dans `content_text`
# MAGIC - **Passe 2 - Classification** : analyse le `content_text` (markdown nettoye) pour classifier
# MAGIC   -> stocke `funnel_stage`, `has_regulatory_content`, `has_country_specific_context`
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
AI_MODEL = "databricks-llama-maverick-4"

# Taille des batchs
BATCH_SIZE = 5

# Nombre max d'items a traiter (None = tout traiter, ex: 5 pour les tests)
MAX_ITEMS = None

# --- Prompt Passe 1 : conversion raw_json -> markdown ---
AI_PROMPT_MARKDOWN = (
    "Role: You are a web formatting specialist.\n\n"

    "Task: Convert the provided WordPress JSON content (Title, Description, and Body) "
    "into clean, well-structured Markdown.\n\n"

    "--- FORMATTING RULES ---\n"
    "- Use proper headings (##, ###) to reflect the document structure.\n"
    "- Use bullet points and numbered lists where appropriate.\n"
    "- Format links as [text](url).\n"
    "- Remove WordPress shortcodes, inline styles, and HTML artifacts.\n"
    "- Preserve the original content meaning and hierarchy.\n"
    "- Do NOT add any commentary, analysis, or metadata.\n\n"

    "--- OUTPUT FORMAT ---\n"
    "Return ONLY the markdown content. No JSON wrapping, no explanations.\n"
    "Your response must start directly with the formatted markdown.\n\n"

    "WordPress JSON:\n"
)

# --- Prompt Passe 2 : classification a partir du markdown nettoye ---
AI_PROMPT_CLASSIFICATION = (
    "Role: You are an expert in B2B content marketing for Cegid (software vendor).\n\n"

    "Task: Analyze the following markdown article to classify it strategically.\n\n"

    "--- CLASSIFICATION LOGIC (Waterfall) ---\n"
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

    "--- OUTPUT FORMAT ---\n"
    "Return ONLY a valid JSON object. Your entire response must start with the "
    "opening { and end with the closing }.\n"
    "NEVER wrap the JSON in ```json or ``` markdown fences.\n"
    "NEVER add any text, comment or explanation before or after the JSON.\n\n"

    "JSON structure:\n"
    "{\n"
    '  "funnel_stage": "TOFU",\n'
    '  "has_regulatory_content": true,\n'
    '  "has_country_specific_context": false\n'
    "}\n\n"

    "Markdown article:\n"
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

def get_items_to_classify(gold_table: str) -> list:
    """
    Identifie les articles qui ont deja un content_text (markdown) mais pas encore
    de classification (funnel_stage NULL).
    """
    query = f"""
    SELECT id, title, slug
    FROM {gold_table}
    WHERE
        content_type = '{CONTENT_TYPE}'
        AND content_text IS NOT NULL AND content_text != ''
        AND funnel_stage IS NULL
    ORDER BY date_modified DESC
    """
    return spark.sql(query)


def process_batch_markdown(gold_table: str, batch_ids: list, ai_model: str,
                           ai_prompt: str, debug: bool = False):
    """
    Passe 1 : Conversion raw_json -> markdown via AI_QUERY.
    Le prompt demande au modele de retourner uniquement du markdown brut (pas de JSON).
    Met a jour content_text et date_formatted dans la table gold.

    Si debug=True, affiche la reponse brute sans effectuer le MERGE.
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)
    ai_prompt_sql = ai_prompt.replace("'", "''")

    if debug:
        debug_query = f"""
        SELECT
            id,
            title,
            AI_QUERY(
                '{ai_model}',
                CONCAT('{ai_prompt_sql}', raw_json)
            ) AS ai_raw_response
        FROM {gold_table}
        WHERE id IN ({ids_str})
        """
        print("    [DEBUG] Raw AI_QUERY response (markdown):")
        df_debug = spark.sql(debug_query)
        for row in df_debug.collect():
            print(f"      ID={row['id']} | title={row['title']}")
            print(f"      response length: {len(str(row['ai_raw_response']))} chars")
            print(f"      preview: {str(row['ai_raw_response'])[:200]}...")
            print()
        return

    merge_query = f"""
    MERGE INTO {gold_table} AS target
    USING (
        SELECT
            id,
            AI_QUERY(
                '{ai_model}',
                CONCAT('{ai_prompt_sql}', raw_json)
            ) AS new_content_text,
            CURRENT_TIMESTAMP() AS new_date_formatted
        FROM {gold_table}
        WHERE id IN ({ids_str})
    ) AS source
    ON target.id = source.id
    WHEN MATCHED AND source.new_content_text IS NOT NULL AND source.new_content_text != '' THEN UPDATE SET
        content_text = source.new_content_text,
        date_formatted = source.new_date_formatted
    """
    spark.sql(merge_query)


def process_batch_classification(gold_table: str, batch_ids: list, ai_model: str,
                                 ai_prompt: str, debug: bool = False):
    """
    Passe 2 : Classification a partir du content_text (markdown nettoye) via AI_QUERY.
    Le prompt demande au modele de retourner un JSON avec funnel_stage, has_regulatory_content,
    has_country_specific_context.

    Note: la reponse AI est nettoyee des markdown fences (```json...```)
    avant le parsing JSON, car certains modeles les ajoutent malgre les instructions.

    Si debug=True, affiche la reponse brute sans effectuer le MERGE.
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)
    ai_prompt_sql = ai_prompt.replace("'", "''")

    # Expression SQL pour nettoyer les markdown fences de la reponse AI
    def clean_json_expr(col_name):
        return f"TRIM(REGEXP_REPLACE(REGEXP_REPLACE({col_name}, '^\\\\s*```[a-z]*\\\\s*', ''), '\\\\s*```\\\\s*$', ''))"

    clean_ai_json = clean_json_expr("ai_raw_response")

    if debug:
        debug_query = f"""
        WITH ai_result AS (
            SELECT
                id,
                title,
                AI_QUERY(
                    '{ai_model}',
                    CONCAT('{ai_prompt_sql}', content_text)
                ) AS ai_raw_response
            FROM {gold_table}
            WHERE id IN ({ids_str})
        )
        SELECT
            id,
            title,
            ai_raw_response,
            {clean_ai_json} AS ai_json_cleaned,
            GET_JSON_OBJECT(
                {clean_ai_json}, '$.funnel_stage'
            ) AS parsed_funnel_stage,
            GET_JSON_OBJECT(
                {clean_ai_json}, '$.has_regulatory_content'
            ) AS parsed_regulatory,
            GET_JSON_OBJECT(
                {clean_ai_json}, '$.has_country_specific_context'
            ) AS parsed_country
        FROM ai_result
        """
        print("    [DEBUG] Raw AI_QUERY response (classification):")
        df_debug = spark.sql(debug_query)
        for row in df_debug.collect():
            print(f"      ID={row['id']} | title={row['title']}")
            print(f"      ai_raw_response: {str(row['ai_raw_response'])}")
            print(f"      ai_json_cleaned: {str(row['ai_json_cleaned'])}")
            print(f"      parsed_funnel_stage: {row['parsed_funnel_stage']}")
            print(f"      parsed_regulatory: {row['parsed_regulatory']}")
            print(f"      parsed_country: {row['parsed_country']}")
            print()
        return

    merge_query = f"""
    MERGE INTO {gold_table} AS target
    USING (
        WITH ai_result AS (
            SELECT
                id,
                {clean_json_expr(
                    "AI_QUERY('" + ai_model + "', CONCAT('" + ai_prompt_sql + "', content_text))"
                )} AS ai_json
            FROM {gold_table}
            WHERE id IN ({ids_str})
        )
        SELECT
            ar.id,
            GET_JSON_OBJECT(ar.ai_json, '$.has_regulatory_content') AS regulatory_raw,
            GET_JSON_OBJECT(ar.ai_json, '$.has_country_specific_context') AS country_raw,
            GET_JSON_OBJECT(ar.ai_json, '$.funnel_stage') AS funnel_raw
        FROM ai_result ar
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        has_regulatory_content = CASE LOWER(TRIM(source.regulatory_raw))
            WHEN 'true' THEN true ELSE false END,
        has_country_specific_context = CASE LOWER(TRIM(source.country_raw))
            WHEN 'true' THEN true ELSE false END,
        funnel_stage = CASE UPPER(TRIM(source.funnel_raw))
            WHEN 'TOFU' THEN 'TOFU'
            WHEN 'MOFU' THEN 'MOFU'
            WHEN 'BOFU' THEN 'BOFU'
            ELSE 'TOFU'
        END
    """
    spark.sql(merge_query)


def run_ai_formatting(gold_table: str = GOLD_TABLE_FULL,
                      batch_size: int = BATCH_SIZE,
                      max_items: int = MAX_ITEMS,
                      ai_model: str = AI_MODEL,
                      debug: bool = False):
    """
    Execute le pipeline AI en 2 passes :
    - Passe 1 : Conversion raw_json -> markdown (content_text)
    - Passe 2 : Classification du markdown (funnel_stage, regulatory, country)

    La passe 2 s'execute sur les items ayant un content_text mais pas encore de
    classification, ce qui inclut les items venant d'etre traites en passe 1
    ainsi que ceux eventuellement restes sans classification.

    max_items: nombre max d'items a traiter par passe (None = tout traiter).
    debug: si True, affiche les reponses brutes AI_QUERY sans effectuer les MERGE.
    """

    # === PASSE 1 : Markdown ===
    df_markdown = get_items_to_process(gold_table)
    md_ids = [row["id"] for row in df_markdown.select("id").collect()]
    if max_items is not None:
        md_ids = md_ids[:max_items]

    total_md = len(md_ids)

    print(f"{'='*60}")
    print(f"PASSE 1 - MARKDOWN : {total_md} item(s) to convert")
    print(f"Model: {ai_model} | Gold table: {gold_table}")
    if debug:
        print(f"*** DEBUG MODE: responses will be displayed, NO data will be written ***")
    print(f"{'='*60}")

    processed_md = 0
    if total_md > 0:
        batches_md = [md_ids[i:i + batch_size] for i in range(0, total_md, batch_size)]
        for idx, batch_ids in enumerate(batches_md, start=1):
            batch_len = len(batch_ids)
            print(f"  Batch {idx}/{len(batches_md)} ({batch_len} item(s))...")
            try:
                process_batch_markdown(gold_table, batch_ids, ai_model,
                                       AI_PROMPT_MARKDOWN, debug=debug)
                processed_md += batch_len
                print(f"    OK - {processed_md}/{total_md} processed")
            except Exception as e:
                print(f"    ERROR batch {idx}: {e}")
                print(f"    IDs: {batch_ids}")
                continue
    else:
        print("  No items need markdown conversion.")

    # === PASSE 2 : Classification ===
    df_classif = get_items_to_classify(gold_table)
    cl_ids = [row["id"] for row in df_classif.select("id").collect()]
    if max_items is not None:
        cl_ids = cl_ids[:max_items]

    total_cl = len(cl_ids)

    print(f"\n{'='*60}")
    print(f"PASSE 2 - CLASSIFICATION : {total_cl} item(s) to classify")
    print(f"{'='*60}")

    processed_cl = 0
    if total_cl > 0:
        batches_cl = [cl_ids[i:i + batch_size] for i in range(0, total_cl, batch_size)]
        for idx, batch_ids in enumerate(batches_cl, start=1):
            batch_len = len(batch_ids)
            print(f"  Batch {idx}/{len(batches_cl)} ({batch_len} item(s))...")
            try:
                process_batch_classification(gold_table, batch_ids, ai_model,
                                             AI_PROMPT_CLASSIFICATION, debug=debug)
                processed_cl += batch_len
                print(f"    OK - {processed_cl}/{total_cl} classified")
            except Exception as e:
                print(f"    ERROR batch {idx}: {e}")
                print(f"    IDs: {batch_ids}")
                continue
    else:
        print("  No items need classification.")

    print(f"\n{'='*60}")
    print(f"Pipeline completed:")
    print(f"  Passe 1 (markdown)       : {processed_md}/{total_md} item(s)")
    print(f"  Passe 2 (classification) : {processed_cl}/{total_cl} item(s)")
    print(f"{'='*60}")

    return processed_md + processed_cl

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

# Statistiques de formatage et classification des articles (gold)
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Diagnostic - Articles avec content_text NULL ou vide

# COMMAND ----------

# Diagnostic 1 : Items formtes (date_formatted non NULL) mais content_text vide
# => Indique un probleme AI_QUERY (JSON malform, GET_JSON_OBJECT retourne NULL)
print("=== Items marques comme formates MAIS content_text NULL/vide ===")
df_ghost = spark.sql(f"""
    SELECT
        id,
        title,
        site_id,
        date_formatted,
        date_modified,
        raw_json AS raw_json_preview,
        content_text IS NULL AS is_null,
        CASE WHEN content_text = '' THEN true ELSE false END AS is_empty
    FROM {GOLD_TABLE_FULL}
    WHERE content_type = '{CONTENT_TYPE}'
        AND date_formatted IS NOT NULL
        AND (content_text IS NULL OR content_text = '')
    ORDER BY date_formatted DESC
""")
print(f"Nombre d'items fantomes (formates mais sans contenu) : {df_ghost.count()}")
display(df_ghost)

# COMMAND ----------

# Diagnostic 2 : Items jamais traites (ni content_text, ni date_formatted)
print("=== Items jamais traites par l'AI ===")
df_never = spark.sql(f"""
    SELECT
        id,
        title,
        site_id,
        date_modified,
        CASE WHEN raw_json IS NULL OR raw_json = '' THEN 'MISSING' ELSE 'OK' END AS raw_json_status,
        LENGTH(raw_json) AS raw_json_length
    FROM {GOLD_TABLE_FULL}
    WHERE content_type = '{CONTENT_TYPE}'
        AND date_formatted IS NULL
        AND (content_text IS NULL OR content_text = '')
    ORDER BY date_modified DESC
""")
print(f"Nombre d'items jamais traites : {df_never.count()}")
display(df_never)

# COMMAND ----------

# Diagnostic 3 : Résumé global par état de traitement
print("=== Resume global par etat ===")
display(spark.sql(f"""
    SELECT
        CASE
            WHEN date_formatted IS NOT NULL AND content_text IS NOT NULL AND content_text != ''
                THEN 'OK - Formate avec contenu'
            WHEN date_formatted IS NOT NULL AND (content_text IS NULL OR content_text = '')
                THEN 'ERREUR - Formate mais contenu NULL'
            WHEN date_formatted IS NULL AND (content_text IS NULL OR content_text = '')
                THEN 'EN ATTENTE - Jamais traite'
            WHEN date_formatted IS NULL AND content_text IS NOT NULL AND content_text != ''
                THEN 'PARTIEL - Contenu present mais non marque formate'
        END AS etat,
        COUNT(*) AS nombre,
        COLLECT_SET(site_id) AS sites_concernes
    FROM {GOLD_TABLE_FULL}
    WHERE content_type = '{CONTENT_TYPE}'
    GROUP BY 1
    ORDER BY nombre DESC
"""))

# COMMAND ----------

# Diagnostic 4 : Test AI_QUERY sur un item specifique
# Affiche la reponse brute + le resultat du parsing JSON
sample_id = df_ghost.first()["id"] if df_ghost.count() > 0 else None
if sample_id is None:
    # Pas d'items fantomes, on prend un item quelconque a traiter
    df_any = get_items_to_process(GOLD_TABLE_FULL)
    if df_any.count() > 0:
        sample_id = df_any.first()["id"]

if sample_id:
    print(f"Test AI_QUERY pour l'item ID={sample_id}")

    # Test Passe 1 : Markdown
    ai_prompt_md_escaped = AI_PROMPT_MARKDOWN.replace("'", "''")
    print(f"\n--- Test Passe 1 : Markdown (raw_json -> markdown) ---")
    df_test_md = spark.sql(f"""
        SELECT
            id,
            title,
            AI_QUERY(
                '{AI_MODEL}',
                CONCAT('{ai_prompt_md_escaped}', raw_json)
            ) AS ai_raw_response
        FROM {GOLD_TABLE_FULL}
        WHERE id = {sample_id}
    """)
    for row in df_test_md.collect():
        raw = str(row["ai_raw_response"])
        print(f"ID={row['id']} | title={row['title']}")
        print(f"Response length: {len(raw)} chars")
        print(f"Preview: {raw[:500]}...")

    # Test Passe 2 : Classification (utilise content_text si disponible)
    ai_prompt_cl_escaped = AI_PROMPT_CLASSIFICATION.replace("'", "''")
    print(f"\n--- Test Passe 2 : Classification (markdown -> JSON) ---")
    df_test_cl = spark.sql(f"""
        SELECT
            id,
            title,
            AI_QUERY(
                '{AI_MODEL}',
                CONCAT('{ai_prompt_cl_escaped}', COALESCE(content_text, ''))
            ) AS ai_raw_response
        FROM {GOLD_TABLE_FULL}
        WHERE id = {sample_id}
    """)
    for row in df_test_cl.collect():
        raw = str(row["ai_raw_response"])
        print(f"ID={row['id']} | title={row['title']}")
        print(f"Raw response: {raw}")
        import re
        cleaned = re.sub(r'^\s*```[a-z]*\s*', '', raw)
        cleaned = re.sub(r'\s*```\s*$', '', cleaned)
        print(f"Cleaned: {cleaned}")
        cleaned_escaped = cleaned.replace("'", "''")
        df_parse = spark.sql(f"""
            SELECT
                GET_JSON_OBJECT('{cleaned_escaped}', '$.funnel_stage') AS funnel_stage,
                GET_JSON_OBJECT('{cleaned_escaped}', '$.has_regulatory_content') AS has_regulatory,
                GET_JSON_OBJECT('{cleaned_escaped}', '$.has_country_specific_context') AS has_country
        """)
        display(df_parse)
else:
    print("Aucun item disponible pour le test AI_QUERY.")
