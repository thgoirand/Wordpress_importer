# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Formatage AI des contenus Blog WordPress (Silver -> Gold)
# MAGIC
# MAGIC Ce notebook formate les contenus blog bruts (raw_json) en markdown structure,
# MAGIC nettoie les appels a l'action et classifie le contenu via `AI_QUERY` (Databricks AI Functions).
# MAGIC
# MAGIC **Architecture Medallion :**
# MAGIC - **Source** : table PLT `cegid_website_plt` (contenus standardises)
# MAGIC - **Cible** : table GLD `cegid_website_gld` (contenus enrichis par IA)
# MAGIC
# MAGIC **Scope:** Articles de blog (`post`) uniquement.
# MAGIC
# MAGIC **Pipeline AI en 3 etapes (3 appels par item) :**
# MAGIC 1. Standardiser le contenu `raw_json` en **markdown propre**.
# MAGIC 2. **Nettoyer** le markdown : suppression des CTA (blocs demo, ebook, webinar...),
# MAGIC    des sommaires, et reinsertion de l'excerpt en debut de contenu.
# MAGIC 3. **Classifier** le contenu nettoye (funnel_stage, regulatory, country, infographics).
# MAGIC    - Logique de classification waterfall : BOFU > MOFU > TOFU > fallback TOFU.
# MAGIC    - Retourne aussi le titre, la meta_description et le contenu nettoye.
# MAGIC
# MAGIC **Champs enrichis :**
# MAGIC - `content_text` : contenu nettoye en markdown (sans CTA ni sommaire)
# MAGIC - `has_regulatory_content` : reference a une reglementation ou texte de loi
# MAGIC - `has_country_specific_context` : contexte specifique a un pays ou marche
# MAGIC - `contains_infographics` : contient des infographies ou visuels de donnees
# MAGIC - `funnel_stage` : stade du funnel marketing (TOFU / MOFU / BOFU)
# MAGIC - `localizable` : synthese true/false, false des qu'un des 3 flags booleens est true
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

# --- Prompt 1 : standardisation raw_json -> markdown ---
AI_PROMPT_MARKDOWN = (
    "You are an expert in web content formatting. "
    "Convert this WordPress JSON (title, description, body) into clean, well-structured markdown. "
    "The input JSON can be a single object, an array, or multiple content blocks that together form one global article. "
    "Use headings (##, ###), bullet points, and properly formatted links [text](url). "
    "Remove WordPress shortcodes, inline styles, scripts and irrelevant technical artifacts. "
    "Keep only meaningful business content for reading and analysis. "
    "Return only the markdown, with no explanation. "
    "JSON: "
)

# --- Prompt 2 : nettoyage du contenu (suppression CTA, sommaires) ---
AI_PROMPT_CLEANUP = (
    "Role: You are a strict content editor. Your ONLY task is to remove specific elements "
    "from the markdown below. You MUST NOT rewrite, rephrase, summarize, or modify any other "
    "part of the content.\n\n"

    "Elements to REMOVE entirely:\n"
    "- Calls to action (CTA): demo request blocks, free trial offers, contact forms, "
    "newsletter signup prompts\n"
    "- Promotional blocks: ebook highlights/downloads, webinar invitations or replays, "
    "whitepaper offers, any 'discover our solution' or 'request a demo' sections\n"
    "- Table of contents / summary sections (sommaires)\n"
    "- 'Read also' / 'Related articles' / 'You might also like' sections\n"
    "- Banner or sidebar-style promotional content embedded in the article\n\n"

    "STRICT RULES:\n"
    "- Do NOT rewrite or rephrase any sentence\n"
    "- Do NOT add any new content\n"
    "- Do NOT summarize or shorten paragraphs\n"
    "- Do NOT change the heading structure or hierarchy\n"
    "- Do NOT modify links, formatting, or layout beyond removing the listed elements\n"
    "- Preserve ALL editorial content exactly as-is\n\n"

    "Return ONLY the cleaned markdown, with no explanation.\n\n"
    "Markdown to clean:\n"
)

# --- Prompt 3 : classification basee sur contenu nettoye ---
AI_PROMPT_CLASSIFICATION = (
    "Role: You are an expert in B2B content marketing for Cegid (software vendor).\n\n"
    "Task: Analyze the provided content (title, meta_description, and cleaned markdown body) "
    "and classify it using the following waterfall rules.\n"
    "Apply the rules in strict order and stop at the first match.\n\n"

    "1. BOFU (Decision & Brand):\n"
    "   - Explicitly promotes Cegid or specific products (XRP, Flex, Talentsoft, Loop, Notilus, Echo, etc.)\n"
    "   - Customer success story, product update, pricing, demo offer, or product webinar replay\n"
    "   -> funnel_stage = 'BOFU'\n\n"

    "2. MOFU (Consideration & Solution):\n"
    "   - Recommends software/ERP/SIRH/digital tools/automation as a solution\n"
    "   - Comparison, checklist, or guide to choose/digitize\n"
    "   -> funnel_stage = 'MOFU'\n\n"

    "3. TOFU (Awareness & Education):\n"
    "   - Educational content (legal news, definitions, management tips, trends)\n"
    "   - No clear software push\n"
    "   -> funnel_stage = 'TOFU'\n\n"

    "4. Fallback:\n"
    "   - If ambiguous, default to 'TOFU'.\n\n"

    "Additional flags:\n"
    "- has_regulatory_content (true/false): mentions regulations/laws/directives/decrees "
    "(GDPR, DORA, Finance Law, Labor Code, ISO, etc.)\n"
    "- has_country_specific_context (true/false): mentions a specific country/national market\n"
    "- contains_infographics (true/false): the content references, embeds, or describes infographics, "
    "data visualizations, charts, or statistical diagrams as part of the article\n\n"

    "Output format requirements:\n"
    "- Return ONLY a valid JSON object\n"
    "- No markdown fences, no commentary\n"
    "- Include the title, meta_description, and content_cleaned fields as provided\n"
    "- Use exactly this structure:\n"
    "{\n"
    '  "title": "<the title as provided>",\n'
    '  "meta_description": "<the meta_description as provided>",\n'
    '  "content_cleaned": "<the full cleaned markdown body as provided>",\n'
    '  "funnel_stage": "TOFU",\n'
    '  "has_regulatory_content": true,\n'
    '  "has_country_specific_context": false,\n'
    '  "contains_infographics": false\n'
    "}\n\n"

    "Content to analyze:\n"
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

# Enrichit les noms d'occupation depuis la table taxonomies
enrich_gold_occupation_names(
    catalog=DATABRICKS_CATALOG,
    schema=DATABRICKS_SCHEMA,
    gold_table=GOLD_TABLE,
    taxonomy_table=TAXONOMY_TABLE,
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

def process_batch_three_steps(gold_table: str, batch_ids: list, ai_model: str,
                              ai_prompt_markdown: str,
                              ai_prompt_cleanup: str,
                              ai_prompt_classification: str,
                              debug: bool = False):
    """
    Traitement en 3 etapes :
    1) raw_json -> markdown brut
    2) markdown brut -> markdown nettoye (suppression CTA, sommaires, reinsertion excerpt)
    3) titre + meta_description + markdown nettoye -> classification JSON

    Met a jour les champs enrichis dans la table gold.
    Le champ localizable est calcule comme NOT(has_regulatory OR has_country_specific OR contains_infographics).

    Note: la reponse de classification est nettoyee des markdown fences (```json...```)
    avant le parsing JSON, car certains modeles les ajoutent malgre les instructions.

    Si debug=True, affiche la reponse brute de AI_QUERY avant le MERGE
    pour diagnostiquer les problemes de parsing JSON.
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)
    ai_prompt_markdown_sql = ai_prompt_markdown.replace("'", "''")
    ai_prompt_cleanup_sql = ai_prompt_cleanup.replace("'", "''")
    ai_prompt_classification_sql = ai_prompt_classification.replace("'", "''")

    # Expression SQL pour nettoyer les markdown fences de la reponse AI
    # Supprime ```json au debut et ``` a la fin si presents
    def clean_json_expr(col_name):
        return f"TRIM(REGEXP_REPLACE(REGEXP_REPLACE({col_name}, '^\\\\s*```[a-z]*\\\\s*', ''), '\\\\s*```\\\\s*$', ''))"

    clean_ai_json = clean_json_expr("classification_raw_response")

    # Expression SQL pour prepender l'excerpt au contenu si present
    # L'excerpt est reinscrit en debut de contenu markdown nettoye
    excerpt_prefix = "CASE WHEN excerpt IS NOT NULL AND excerpt != '' THEN CONCAT(excerpt, '\\n\\n', cleaned_content) ELSE cleaned_content END"

    # --- Mode debug : afficher la reponse brute AI_QUERY ---
    if debug:
        debug_query = f"""
        WITH markdown_result AS (
            SELECT
                id,
                title,
                meta_description,
                excerpt,
                AI_QUERY(
                    '{ai_model}',
                    CONCAT('{ai_prompt_markdown_sql}', raw_json)
                ) AS markdown_content
            FROM {gold_table}
            WHERE id IN ({ids_str})
        ),
        cleanup_result AS (
            SELECT
                id,
                title,
                meta_description,
                excerpt,
                markdown_content,
                AI_QUERY(
                    '{ai_model}',
                    CONCAT('{ai_prompt_cleanup_sql}', markdown_content)
                ) AS cleaned_content
            FROM markdown_result
        ),
        with_excerpt AS (
            SELECT
                id,
                title,
                meta_description,
                excerpt,
                markdown_content,
                cleaned_content,
                {excerpt_prefix} AS final_content
            FROM cleanup_result
        ),
        classification_result AS (
            SELECT
                we.*,
                AI_QUERY(
                    '{ai_model}',
                    CONCAT(
                        '{ai_prompt_classification_sql}',
                        'Title: ', COALESCE(title, ''), '\\n',
                        'Meta description: ', COALESCE(meta_description, ''), '\\n',
                        'Content:\\n', final_content
                    )
                ) AS classification_raw_response
            FROM with_excerpt we
        )
        SELECT
            id,
            title,
            meta_description,
            excerpt,
            markdown_content,
            cleaned_content,
            final_content,
            classification_raw_response,
            {clean_ai_json} AS ai_json_cleaned,
            GET_JSON_OBJECT(
                {clean_ai_json}, '$.funnel_stage'
            ) AS parsed_funnel_stage,
            GET_JSON_OBJECT(
                {clean_ai_json}, '$.contains_infographics'
            ) AS parsed_contains_infographics
        FROM classification_result
        """
        print("    [DEBUG] Raw AI_QUERY responses:")
        df_debug = spark.sql(debug_query)
        for row in df_debug.collect():
            print(f"      ID={row['id']} | title={row['title']}")
            print(f"      excerpt: {str(row['excerpt'])[:100]}")
            print(f"      markdown_content: {str(row['markdown_content'])[:200]}")
            print(f"      cleaned_content: {str(row['cleaned_content'])[:200]}")
            print(f"      final_content (with excerpt): {str(row['final_content'])[:200]}")
            print(f"      classification_raw_response: {str(row['classification_raw_response'])}")
            print(f"      ai_json_cleaned: {str(row['ai_json_cleaned'])}")
            print(f"      parsed_funnel_stage: {row['parsed_funnel_stage']}")
            print(f"      parsed_contains_infographics: {row['parsed_contains_infographics']}")
            print()
        return  # En mode debug, on ne fait pas le MERGE

    merge_query = f"""
    MERGE INTO {gold_table} AS target
    USING (
        WITH markdown_result AS (
            SELECT
                id,
                title,
                meta_description,
                excerpt,
                AI_QUERY(
                    '{ai_model}',
                    CONCAT('{ai_prompt_markdown_sql}', raw_json)
                ) AS markdown_content
            FROM {gold_table}
            WHERE id IN ({ids_str})
        ),
        cleanup_result AS (
            SELECT
                id,
                title,
                meta_description,
                excerpt,
                AI_QUERY(
                    '{ai_model}',
                    CONCAT('{ai_prompt_cleanup_sql}', markdown_content)
                ) AS cleaned_content
            FROM markdown_result
        ),
        with_excerpt AS (
            SELECT
                id,
                title,
                meta_description,
                {excerpt_prefix} AS final_content
            FROM cleanup_result
        ),
        classification_result AS (
            SELECT
                we.id,
                we.final_content,
                {clean_json_expr(
                    "AI_QUERY('" + ai_model + "', CONCAT('" + ai_prompt_classification_sql + "', "
                    "'Title: ', COALESCE(we.title, ''), '\\n', "
                    "'Meta description: ', COALESCE(we.meta_description, ''), '\\n', "
                    "'Content:\\n', we.final_content))"
                )} AS classification_json
            FROM with_excerpt we
        )
        SELECT
            cr.id,
            cr.final_content AS new_content_text,
            GET_JSON_OBJECT(cr.classification_json, '$.has_regulatory_content') AS regulatory_raw,
            GET_JSON_OBJECT(cr.classification_json, '$.has_country_specific_context') AS country_raw,
            GET_JSON_OBJECT(cr.classification_json, '$.contains_infographics') AS infographics_raw,
            GET_JSON_OBJECT(cr.classification_json, '$.funnel_stage') AS funnel_raw,
            CURRENT_TIMESTAMP() AS new_date_formatted
        FROM classification_result cr
    ) AS source
    ON target.id = source.id
    WHEN MATCHED AND source.new_content_text IS NOT NULL AND source.new_content_text != '' THEN UPDATE SET
        content_text = source.new_content_text,
        has_regulatory_content = CASE LOWER(TRIM(source.regulatory_raw))
            WHEN 'true' THEN true ELSE false END,
        has_country_specific_context = CASE LOWER(TRIM(source.country_raw))
            WHEN 'true' THEN true ELSE false END,
        contains_infographics = CASE LOWER(TRIM(source.infographics_raw))
            WHEN 'true' THEN true ELSE false END,
        funnel_stage = CASE UPPER(TRIM(source.funnel_raw))
            WHEN 'TOFU' THEN 'TOFU'
            WHEN 'MOFU' THEN 'MOFU'
            WHEN 'BOFU' THEN 'BOFU'
            ELSE 'TOFU'
        END,
        localizable = CASE
            WHEN LOWER(TRIM(source.regulatory_raw)) = 'true'
                OR LOWER(TRIM(source.country_raw)) = 'true'
                OR LOWER(TRIM(source.infographics_raw)) = 'true'
            THEN false
            ELSE true
        END,
        date_formatted = source.new_date_formatted
    """
    spark.sql(merge_query)


def run_ai_formatting(gold_table: str = GOLD_TABLE_FULL,
                      batch_size: int = BATCH_SIZE,
                      max_items: int = MAX_ITEMS,
                      ai_model: str = AI_MODEL,
                      ai_prompt_markdown: str = AI_PROMPT_MARKDOWN,
                      ai_prompt_cleanup: str = AI_PROMPT_CLEANUP,
                      ai_prompt_classification: str = AI_PROMPT_CLASSIFICATION,
                      debug: bool = False):
    """
    Execute le pipeline AI en trois passes :
    - Passe 1: generation markdown depuis raw_json
    - Passe 2: nettoyage du markdown (suppression CTA, sommaires, reinsertion excerpt)
    - Passe 3: classification depuis contenu nettoye (avec titre et meta_description)

    max_items: nombre max d'items a traiter (None = tout traiter).
    debug: si True, affiche la reponse brute AI_QUERY sans effectuer le MERGE.
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
    if debug:
        print(f"*** DEBUG MODE: responses will be displayed, NO data will be written ***")
    print(f"{'='*60}")

    print(f"\n--- AI pass 1+2+3: markdown -> cleanup -> classification ---")
    processed = 0
    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"  Batch {idx}/{nb_batches} ({batch_len} item(s))...")
        try:
            process_batch_three_steps(
                gold_table,
                batch_ids,
                ai_model,
                ai_prompt_markdown,
                ai_prompt_cleanup,
                ai_prompt_classification,
                debug=debug
            )
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
# MAGIC ## 7. Verification

# COMMAND ----------

# Apercu des derniers articles formates (gold)
display(spark.sql(f"""
    SELECT
        id,
        title,
        LEFT(content_text, 300) AS content_preview,
        occupation_names,
        has_regulatory_content,
        has_country_specific_context,
        contains_infographics,
        funnel_stage,
        localizable,
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
        SUM(CASE WHEN contains_infographics = true THEN 1 ELSE 0 END) AS with_infographics,
        SUM(CASE WHEN contains_infographics IS NULL THEN 1 ELSE 0 END) AS infographics_unknown,
        SUM(CASE WHEN funnel_stage = 'TOFU' THEN 1 ELSE 0 END) AS funnel_tofu,
        SUM(CASE WHEN funnel_stage = 'MOFU' THEN 1 ELSE 0 END) AS funnel_mofu,
        SUM(CASE WHEN funnel_stage = 'BOFU' THEN 1 ELSE 0 END) AS funnel_bofu,
        SUM(CASE WHEN funnel_stage = 'UNCERTAIN' OR funnel_stage IS NULL THEN 1 ELSE 0 END) AS funnel_uncertain,
        SUM(CASE WHEN localizable = true THEN 1 ELSE 0 END) AS localizable_yes,
        SUM(CASE WHEN localizable = false THEN 1 ELSE 0 END) AS localizable_no
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
    print(f"Test AI_QUERY 3 etapes pour l'item ID={sample_id}")
    ai_prompt_markdown_escaped = AI_PROMPT_MARKDOWN.replace("'", "''")
    ai_prompt_cleanup_escaped = AI_PROMPT_CLEANUP.replace("'", "''")
    ai_prompt_classification_escaped = AI_PROMPT_CLASSIFICATION.replace("'", "''")

    df_test = spark.sql(f"""
        WITH markdown_result AS (
            SELECT
                id,
                title,
                meta_description,
                excerpt,
                AI_QUERY(
                    '{AI_MODEL}',
                    CONCAT('{ai_prompt_markdown_escaped}', raw_json)
                ) AS markdown_content
            FROM {GOLD_TABLE_FULL}
            WHERE id = {sample_id}
        ),
        cleanup_result AS (
            SELECT
                id,
                title,
                meta_description,
                excerpt,
                markdown_content,
                AI_QUERY(
                    '{AI_MODEL}',
                    CONCAT('{ai_prompt_cleanup_escaped}', markdown_content)
                ) AS cleaned_content
            FROM markdown_result
        ),
        with_excerpt AS (
            SELECT
                *,
                CASE WHEN excerpt IS NOT NULL AND excerpt != ''
                    THEN CONCAT(excerpt, '\\n\\n', cleaned_content)
                    ELSE cleaned_content
                END AS final_content
            FROM cleanup_result
        )
        SELECT
            id,
            title,
            meta_description,
            excerpt,
            markdown_content,
            cleaned_content,
            final_content,
            AI_QUERY(
                '{AI_MODEL}',
                CONCAT(
                    '{ai_prompt_classification_escaped}',
                    'Title: ', COALESCE(title, ''), '\\n',
                    'Meta description: ', COALESCE(meta_description, ''), '\\n',
                    'Content:\\n', final_content
                )
            ) AS classification_raw_response
        FROM with_excerpt
    """)

    for row in df_test.collect():
        markdown = str(row["markdown_content"])
        cleaned = str(row["cleaned_content"])
        final = str(row["final_content"])
        raw_classification = str(row["classification_raw_response"])
        print(f"\n--- Markdown brut (ID={row['id']}, title={row['title']}) ---")
        print(markdown[:500])
        print(f"\n--- Excerpt ---")
        print(str(row["excerpt"]))
        print(f"\n--- Contenu nettoye (CTA/sommaires supprimes) ---")
        print(cleaned[:500])
        print(f"\n--- Contenu final (avec excerpt reinscrit) ---")
        print(final[:500])
        print(f"\n--- Reponse brute classification ---")
        print(raw_classification)

        # Nettoyage des markdown fences avant parsing JSON
        import re
        cleaned_json = re.sub(r'^\s*```[a-z]*\s*', '', raw_classification)
        cleaned_json = re.sub(r'\s*```\s*$', '', cleaned_json)
        print(f"\n--- Classification nettoyee (fences supprimees) ---")
        print(cleaned_json)
        print(f"\n--- Tentative de parsing GET_JSON_OBJECT ---")
        cleaned_escaped = cleaned_json.replace("'", "''")
        df_parse = spark.sql(f"""
            SELECT
                GET_JSON_OBJECT('{cleaned_escaped}', '$.funnel_stage') AS funnel_stage,
                GET_JSON_OBJECT('{cleaned_escaped}', '$.has_regulatory_content') AS has_regulatory,
                GET_JSON_OBJECT('{cleaned_escaped}', '$.has_country_specific_context') AS has_country,
                GET_JSON_OBJECT('{cleaned_escaped}', '$.contains_infographics') AS has_infographics
        """)
        display(df_parse)
else:
    print("Aucun item disponible pour le test AI_QUERY.")
