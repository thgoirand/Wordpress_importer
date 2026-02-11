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
# MAGIC **Pipeline AI en 2 passes distinctes :**
# MAGIC - **Passe 1 - Markdown** : convertit le `raw_json` en contenu markdown propre et structure
# MAGIC - **Passe 2 - Classification** : analyse le markdown nettoye pour determiner :
# MAGIC   - `funnel_stage` (TOFU / MOFU / BOFU) via logique waterfall
# MAGIC   - `has_regulatory_content` (reference reglementaire)
# MAGIC   - `has_country_specific_context` (contexte pays)
# MAGIC
# MAGIC **Avantage :** chaque appel AI a une tache simple et focalisee, ce qui ameliore
# MAGIC la fiabilite des resultats avec les modeles locaux (Llama).
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

# --- Prompt Passe 1 : Conversion raw_json -> Markdown ---
AI_PROMPT_MARKDOWN = (
    "Role: You are an expert web content formatting specialist.\n\n"

    "Task: Convert the provided WordPress JSON content (Title, Description, and Body) "
    "into clean, well-structured Markdown.\n\n"

    "--- FORMATTING RULES ---\n"
    "- Start with the article title as a level-1 heading (# Title).\n"
    "- Use proper headings hierarchy (##, ###) to structure sections.\n"
    "- Use bullet points and numbered lists where appropriate.\n"
    "- Format links as [text](url).\n"
    "- Remove all WordPress shortcodes, inline styles, and HTML artifacts.\n"
    "- Preserve the logical structure and meaning of the original content.\n"
    "- Do NOT add any commentary, analysis, or content that is not in the original.\n\n"

    "--- OUTPUT FORMAT ---\n"
    "Return ONLY the Markdown content. No wrapping, no JSON, no explanation.\n"
    "Your response must start directly with the formatted Markdown.\n\n"

    "Input JSON:\n"
)

# --- Prompt Passe 2 : Classification a partir du contenu Markdown ---
AI_PROMPT_CLASSIFICATION = (
    "Role: You are an expert in B2B content marketing for Cegid (software vendor).\n\n"

    "Task: Analyze the following Markdown content of a blog article "
    "and classify it according to the rules below.\n\n"

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

    "Markdown content to classify:\n"
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

def process_batch_markdown(gold_table: str, batch_ids: list, ai_model: str,
                           ai_prompt: str, debug: bool = False):
    """
    Passe 1 : Conversion raw_json -> Markdown.
    Le prompt demande au modele de retourner directement du markdown (pas de JSON).
    Met a jour content_text et date_formatted dans la table gold.

    Si debug=True, affiche la reponse brute de AI_QUERY sans effectuer le MERGE.
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
        print("    [DEBUG] Passe 1 - Markdown - Raw AI_QUERY response:")
        df_debug = spark.sql(debug_query)
        for row in df_debug.collect():
            raw = str(row["ai_raw_response"])
            print(f"      ID={row['id']} | title={row['title']}")
            print(f"      response length: {len(raw)} chars")
            print(f"      response preview: {raw[:500]}...")
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
    Passe 2 : Classification a partir du contenu markdown (content_text).
    Le prompt analyse le markdown nettoye et retourne un JSON de classification.
    Met a jour funnel_stage, has_regulatory_content, has_country_specific_context.

    Pre-requis : content_text doit etre rempli (passe 1 executee).

    Note: la reponse AI est nettoyee des markdown fences (```json...```)
    avant le parsing JSON, car certains modeles les ajoutent malgre les instructions.

    Si debug=True, affiche la reponse brute de AI_QUERY sans effectuer le MERGE.
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)
    ai_prompt_sql = ai_prompt.replace("'", "''")

    # Expression SQL pour nettoyer les markdown fences de la reponse AI
    def clean_json_expr(col_name):
        return f"TRIM(REGEXP_REPLACE(REGEXP_REPLACE({col_name}, '^\\\\s*```[a-z]*\\\\s*', ''), '\\\\s*```\\\\s*$', ''))"

    if debug:
        clean_ai_json = clean_json_expr("ai_raw_response")
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
        print("    [DEBUG] Passe 2 - Classification - Raw AI_QUERY response:")
        df_debug = spark.sql(debug_query)
        for row in df_debug.collect():
            raw = str(row["ai_raw_response"])
            print(f"      ID={row['id']} | title={row['title']}")
            print(f"      ai_raw_response: {raw}")
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


def get_items_to_classify(gold_table: str) -> list:
    """
    Identifie les articles de blog qui ont un content_text (markdown)
    mais pas encore de classification (funnel_stage NULL).
    Utilise apres la passe 1 pour alimenter la passe 2.
    """
    query = f"""
    SELECT id
    FROM {gold_table}
    WHERE content_type = '{CONTENT_TYPE}'
        AND content_text IS NOT NULL AND content_text != ''
        AND (funnel_stage IS NULL OR funnel_stage = '')
    ORDER BY date_modified DESC
    """
    return [row["id"] for row in spark.sql(query).collect()]


def run_ai_formatting(gold_table: str = GOLD_TABLE_FULL,
                      batch_size: int = BATCH_SIZE,
                      max_items: int = MAX_ITEMS,
                      ai_model: str = AI_MODEL,
                      debug: bool = False):
    """
    Execute le pipeline AI en 2 passes distinctes :
    - Passe 1 : Conversion raw_json -> Markdown (met a jour content_text)
    - Passe 2 : Classification du markdown (met a jour funnel_stage, regulatory, country)

    max_items: nombre max d'items a traiter (None = tout traiter).
    debug: si True, affiche les reponses brutes AI_QUERY sans effectuer les MERGE.
    """
    # === PASSE 1 : Markdown ===
    df = get_items_to_process(gold_table)
    all_ids = [row["id"] for row in df.select("id").collect()]

    if max_items is not None:
        all_ids = all_ids[:max_items]

    total_md = len(all_ids)

    print(f"{'='*60}")
    print(f"Model: {ai_model}")
    print(f"Gold table: {gold_table}")
    if debug:
        print(f"*** DEBUG MODE: responses will be displayed, NO data will be written ***")
    print(f"{'='*60}")

    processed_md = 0
    if total_md > 0:
        batches_md = [all_ids[i:i + batch_size] for i in range(0, total_md, batch_size)]
        nb_batches_md = len(batches_md)

        print(f"\n--- Passe 1/2 : Markdown ({total_md} item(s), {nb_batches_md} batch(es)) ---")
        for idx, batch_ids in enumerate(batches_md, start=1):
            batch_len = len(batch_ids)
            print(f"  Batch {idx}/{nb_batches_md} ({batch_len} item(s))...")
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
        print("\n--- Passe 1/2 : Markdown - No items to process ---")

    # === PASSE 2 : Classification ===
    # On recupere les items qui ont un markdown mais pas encore de classification
    ids_to_classify = get_items_to_classify(gold_table)
    if max_items is not None:
        ids_to_classify = ids_to_classify[:max_items]

    total_cls = len(ids_to_classify)
    processed_cls = 0

    if total_cls > 0:
        batches_cls = [ids_to_classify[i:i + batch_size]
                       for i in range(0, total_cls, batch_size)]
        nb_batches_cls = len(batches_cls)

        print(f"\n--- Passe 2/2 : Classification ({total_cls} item(s), {nb_batches_cls} batch(es)) ---")
        for idx, batch_ids in enumerate(batches_cls, start=1):
            batch_len = len(batch_ids)
            print(f"  Batch {idx}/{nb_batches_cls} ({batch_len} item(s))...")
            try:
                process_batch_classification(gold_table, batch_ids, ai_model,
                                             AI_PROMPT_CLASSIFICATION, debug=debug)
                processed_cls += batch_len
                print(f"    OK - {processed_cls}/{total_cls} classified")
            except Exception as e:
                print(f"    ERROR batch {idx}: {e}")
                print(f"    IDs: {batch_ids}")
                continue
    else:
        print("\n--- Passe 2/2 : Classification - No items to classify ---")

    print(f"\n{'='*60}")
    print(f"Pipeline completed:")
    print(f"  Passe 1 (Markdown)       : {processed_md}/{total_md} item(s)")
    print(f"  Passe 2 (Classification) : {processed_cls}/{total_cls} item(s)")
    print(f"{'='*60}")

    return processed_md, processed_cls

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

# Diagnostic 4 : Test AI_QUERY sur un item specifique (les 2 passes)
sample_id = df_ghost.first()["id"] if df_ghost.count() > 0 else None
if sample_id is None:
    df_any = get_items_to_process(GOLD_TABLE_FULL)
    if df_any.count() > 0:
        sample_id = df_any.first()["id"]

if sample_id:
    # --- Test Passe 1 : Markdown ---
    print(f"=== Test Passe 1 (Markdown) pour l'item ID={sample_id} ===")
    ai_prompt_md_escaped = AI_PROMPT_MARKDOWN.replace("'", "''")
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
    markdown_result = None
    for row in df_test_md.collect():
        markdown_result = str(row["ai_raw_response"])
        print(f"\n--- Reponse Passe 1 (ID={row['id']}, title={row['title']}) ---")
        print(f"Length: {len(markdown_result)} chars")
        print(markdown_result[:1000])
        if len(markdown_result) > 1000:
            print("... (truncated)")

    # --- Test Passe 2 : Classification ---
    if markdown_result:
        print(f"\n=== Test Passe 2 (Classification) pour l'item ID={sample_id} ===")
        ai_prompt_cls_escaped = AI_PROMPT_CLASSIFICATION.replace("'", "''")
        markdown_escaped = markdown_result.replace("'", "''")
        df_test_cls = spark.sql(f"""
            SELECT
                AI_QUERY(
                    '{AI_MODEL}',
                    CONCAT('{ai_prompt_cls_escaped}', '{markdown_escaped}')
                ) AS ai_raw_response
        """)
        for row in df_test_cls.collect():
            raw = str(row["ai_raw_response"])
            print(f"\n--- Reponse brute Classification ---")
            print(raw)
            import re
            cleaned = re.sub(r'^\s*```[a-z]*\s*', '', raw)
            cleaned = re.sub(r'\s*```\s*$', '', cleaned)
            print(f"\n--- Reponse nettoyee ---")
            print(cleaned)
            print(f"\n--- Parsing JSON ---")
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
