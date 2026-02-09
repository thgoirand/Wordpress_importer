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
# MAGIC **Pipeline AI en 2 passes de classification :**
# MAGIC 1. Generation du markdown a partir du `raw_json`
# MAGIC 2. **Pass 1 - Scan leger** : classification sur titre + description uniquement (econome en tokens).
# MAGIC    Retourne UNCERTAIN si confiance < 80%.
# MAGIC 3. **Pass 2 - Analyse profonde** : uniquement pour les items UNCERTAIN,
# MAGIC    analyse du contenu complet avec logique waterfall.
# MAGIC
# MAGIC **Champs enrichis :**
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

# Prompt systeme pour le formatage markdown
AI_PROMPT = (
    "Tu es un expert en formatage de contenu web. "
    "Convertis ce JSON WordPress en markdown propre et structure. "
    "Utilise des titres (##, ###), des listes, et formate correctement les liens. "
    "Retourne uniquement le markdown, sans explications. "
    "JSON: "
)

# --- PASS 1 : Classification legere sur titre + description (econome en tokens) ---
AI_PROMPT_CLASSIFICATION_LIGHT = (
    "Role: You are an expert in B2B content marketing for Cegid (software vendor).\n\n"
    "Task: Analyze the following Title and Short Description to categorize the content's marketing intent.\n\n"
    "Constraint: If the inputs are too vague, generic, or if you lack confidence (>80%) "
    "regarding any field, you MUST return \"UNCERTAIN\" for that field.\n\n"
    "Classification Logic for funnel_stage:\n"
    "- BOFU (Decision & Brand): Mentions \"Cegid\", specific products "
    "(XRP, Flex, Talentsoft, Loop, Notilus, etc.), \"Price\", \"Demo\", \"Trial\", "
    "\"Case Study\", or \"Client Success\".\n"
    "- MOFU (Consideration & Solution): Mentions \"Software\", \"Tool\", \"Platform\", "
    "\"Comparison\" (vs), \"How to choose\", \"Automation of [process]\", or \"Digitization\".\n"
    "- TOFU (Awareness & Education): Mentions broad concepts, specific laws/regulations, "
    "definitions, \"Guide to [Topic]\", \"Trends\", or \"Management tips\" "
    "without implying a software solution.\n"
    "- UNCERTAIN: The title is ambiguous (e.g., \"Optimizing Performance\") "
    "and the description does not clarify if the solution is a software or a methodology.\n\n"
    "Classification Logic for has_regulatory_content (true/false/UNCERTAIN):\n"
    "- true: Title or description references a regulation, law, directive, standard, decree, "
    "or legal framework (GDPR, DORA, finance law, labor code, ISO standard, etc.).\n"
    "- false: No regulatory reference detected.\n"
    "- UNCERTAIN: Cannot determine from title and description alone.\n\n"
    "Classification Logic for has_country_specific_context (true/false/UNCERTAIN):\n"
    "- true: Title or description references a specific country or national market "
    "(French taxation, Spanish market, Italian legislation, etc.).\n"
    "- false: Generic or international content.\n"
    "- UNCERTAIN: Cannot determine from title and description alone.\n\n"
    "Respond ONLY with a valid JSON object (no markdown, no explanation), in this exact format:\n"
    '{\"has_regulatory_content\": true, \"has_country_specific_context\": false, \"funnel_stage\": \"TOFU\"}\n\n'
    "Allowed values:\n"
    "- has_regulatory_content: true, false, or \"UNCERTAIN\"\n"
    "- has_country_specific_context: true, false, or \"UNCERTAIN\"\n"
    "- funnel_stage: \"TOFU\", \"MOFU\", \"BOFU\", or \"UNCERTAIN\"\n\n"
    "Input:\n"
)

# --- PASS 2 : Classification profonde sur contenu complet (waterfall) ---
AI_PROMPT_CLASSIFICATION_DEEP = (
    "Role: You are an expert in content analysis. The previous analysis based on the title "
    "was inconclusive. Now, analyze the FULL BODY CONTENT to make a definitive classification.\n\n"
    "Decision Hierarchy (Waterfall Logic): Apply these rules in strict order. "
    "Stop at the first match.\n\n"
    "1. Check for BOFU (Brand/Product Focused):\n"
    "   - Does the text explicitly pitch \"Cegid\" or its specific products as the solution?\n"
    "   - Is it a customer success story, a product update, or a webinar replay about a product?\n"
    "   -> If YES: classify as BOFU.\n\n"
    "2. Check for MOFU (Solution Focused):\n"
    "   - Does the text recommend using \"a software\", \"an ERP\", \"a SIRH\", "
    "or \"digital tools\" to solve a problem?\n"
    "   - Is it a comparison, a selection checklist, or a guide on "
    "\"How to automate/digitize X\"?\n"
    "   -> If YES: classify as MOFU.\n\n"
    "3. Check for TOFU (Problem/Education Focused):\n"
    "   - Is the text purely educational (legal news, definitions, manual management tips, trends)?\n"
    "   - Does it explain \"Why\" or \"What\" without pushing a software solution immediately?\n"
    "   -> If YES: classify as TOFU.\n\n"
    "4. Fallback Rule: If the content remains ambiguous between general advice and software "
    "promotion, default to TOFU. It is safer to assume the user is still learning "
    "than to assume they are ready to buy.\n\n"
    "Also determine:\n"
    "- has_regulatory_content (true/false): Does the content reference any regulation, law, "
    "directive, standard, decree, or legal framework?\n"
    "- has_country_specific_context (true/false): Is the content specific to a particular "
    "country or national market?\n\n"
    "Respond ONLY with a valid JSON object (no markdown, no explanation), in this exact format:\n"
    '{\"has_regulatory_content\": true, \"has_country_specific_context\": false, \"funnel_stage\": \"TOFU\"}\n\n'
    "Allowed values (no UNCERTAIN allowed in this pass - you MUST decide):\n"
    "- has_regulatory_content: true or false\n"
    "- has_country_specific_context: true or false\n"
    "- funnel_stage: \"TOFU\", \"MOFU\", or \"BOFU\"\n\n"
    "Content:\n"
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

def process_batch_markdown(gold_table: str, batch_ids: list, ai_model: str,
                           ai_prompt: str):
    """
    Etape 0 : Genere le content_text (markdown) a partir du raw_json pour un batch.
    Met a jour la table gold avec le markdown genere.
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)
    ai_prompt_sql = ai_prompt.replace("'", "''")

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
    WHEN MATCHED THEN UPDATE SET
        content_text = source.new_content_text,
        date_formatted = source.new_date_formatted
    """
    spark.sql(merge_query)


def process_batch_classify_light(gold_table: str, batch_ids: list, ai_model: str,
                                  ai_prompt_light: str):
    """
    Pass 1 : Classification legere sur titre + description uniquement.
    Econome en tokens. Retourne UNCERTAIN si confiance insuffisante.
    Met a jour les champs de classification dans la table gold.
    Les valeurs UNCERTAIN sont stockees temporairement pour identification en pass 2.
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)
    ai_prompt_sql = ai_prompt_light.replace("'", "''")

    merge_query = f"""
    MERGE INTO {gold_table} AS target
    USING (
        WITH light_classified AS (
            SELECT
                id,
                AI_QUERY(
                    '{ai_model}',
                    CONCAT(
                        '{ai_prompt_sql}',
                        'Title: ', COALESCE(title, ''),
                        '\\nDescription: ', COALESCE(meta_description, COALESCE(excerpt, ''))
                    )
                ) AS classification_json
            FROM {gold_table}
            WHERE id IN ({ids_str})
        )
        SELECT
            lc.id,
            lc.classification_json,
            GET_JSON_OBJECT(lc.classification_json, '$.has_regulatory_content') AS regulatory_raw,
            GET_JSON_OBJECT(lc.classification_json, '$.has_country_specific_context') AS country_raw,
            GET_JSON_OBJECT(lc.classification_json, '$.funnel_stage') AS funnel_raw
        FROM light_classified lc
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
        has_regulatory_content = CASE
            WHEN UPPER(TRIM(source.regulatory_raw)) = 'UNCERTAIN' THEN NULL
            WHEN LOWER(TRIM(source.regulatory_raw)) = 'true' THEN true
            ELSE false
        END,
        has_country_specific_context = CASE
            WHEN UPPER(TRIM(source.country_raw)) = 'UNCERTAIN' THEN NULL
            WHEN LOWER(TRIM(source.country_raw)) = 'true' THEN true
            ELSE false
        END,
        funnel_stage = CASE UPPER(TRIM(source.funnel_raw))
            WHEN 'TOFU' THEN 'TOFU'
            WHEN 'MOFU' THEN 'MOFU'
            WHEN 'BOFU' THEN 'BOFU'
            WHEN 'UNCERTAIN' THEN 'UNCERTAIN'
            ELSE 'UNCERTAIN'
        END
    """
    spark.sql(merge_query)


def get_uncertain_ids(gold_table: str, candidate_ids: list) -> list:
    """
    Identifie les items ayant au moins un champ UNCERTAIN apres la pass 1.
    Un item est UNCERTAIN si :
    - funnel_stage = 'UNCERTAIN'
    - has_regulatory_content IS NULL (etait UNCERTAIN)
    - has_country_specific_context IS NULL (etait UNCERTAIN)
    """
    ids_str = ", ".join(str(id_val) for id_val in candidate_ids)
    query = f"""
    SELECT id
    FROM {gold_table}
    WHERE id IN ({ids_str})
      AND (
          funnel_stage = 'UNCERTAIN'
          OR has_regulatory_content IS NULL
          OR has_country_specific_context IS NULL
      )
    """
    rows = spark.sql(query).collect()
    return [row["id"] for row in rows]


def process_batch_classify_deep(gold_table: str, batch_ids: list, ai_model: str,
                                 ai_prompt_deep: str):
    """
    Pass 2 : Classification profonde sur le contenu complet (content_text).
    Uniquement pour les items dont la pass 1 a retourne UNCERTAIN.
    Utilise une logique waterfall : BOFU > MOFU > TOFU > fallback TOFU.
    Aucun UNCERTAIN n'est autorise en sortie de cette pass.
    """
    ids_str = ", ".join(str(id_val) for id_val in batch_ids)
    ai_prompt_sql = ai_prompt_deep.replace("'", "''")

    merge_query = f"""
    MERGE INTO {gold_table} AS target
    USING (
        WITH deep_classified AS (
            SELECT
                id,
                AI_QUERY(
                    '{ai_model}',
                    CONCAT(
                        '{ai_prompt_sql}',
                        content_text
                    )
                ) AS classification_json
            FROM {gold_table}
            WHERE id IN ({ids_str})
        )
        SELECT
            dc.id,
            dc.classification_json,
            GET_JSON_OBJECT(dc.classification_json, '$.has_regulatory_content') AS regulatory_raw,
            GET_JSON_OBJECT(dc.classification_json, '$.has_country_specific_context') AS country_raw,
            GET_JSON_OBJECT(dc.classification_json, '$.funnel_stage') AS funnel_raw
        FROM deep_classified dc
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
                      ai_prompt: str = AI_PROMPT,
                      ai_prompt_light: str = AI_PROMPT_CLASSIFICATION_LIGHT,
                      ai_prompt_deep: str = AI_PROMPT_CLASSIFICATION_DEEP):
    """
    Execute le pipeline AI complet en 3 phases :
    1. Generation du markdown (raw_json -> content_text)
    2. Pass 1 : Classification legere (titre + description, econome en tokens)
    3. Pass 2 : Classification profonde (contenu complet, uniquement pour les UNCERTAIN)

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

    # --- Phase 1 : Generation markdown ---
    print(f"\n--- Phase 1/3 : Generation markdown ---")
    processed = 0
    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"  Batch {idx}/{nb_batches} ({batch_len} element(s))...")
        try:
            process_batch_markdown(gold_table, batch_ids, ai_model, ai_prompt)
            processed += batch_len
            print(f"    OK - {processed}/{total} markdown genere(s)")
        except Exception as e:
            print(f"    ERREUR batch {idx}: {e}")
            print(f"    IDs: {batch_ids}")
            continue

    # --- Phase 2 : Classification legere (titre + description) ---
    print(f"\n--- Phase 2/3 : Classification legere (titre + description) ---")
    classified_light = 0
    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"  Batch {idx}/{nb_batches} ({batch_len} element(s))...")
        try:
            process_batch_classify_light(gold_table, batch_ids, ai_model, ai_prompt_light)
            classified_light += batch_len
            print(f"    OK - {classified_light}/{total} classifie(s) (pass 1)")
        except Exception as e:
            print(f"    ERREUR batch {idx}: {e}")
            print(f"    IDs: {batch_ids}")
            continue

    # --- Phase 3 : Classification profonde (contenu complet, UNCERTAIN seulement) ---
    uncertain_ids = get_uncertain_ids(gold_table, all_ids)
    nb_uncertain = len(uncertain_ids)
    print(f"\n--- Phase 3/3 : Classification profonde ({nb_uncertain} item(s) UNCERTAIN) ---")

    if nb_uncertain == 0:
        print("  Aucun item UNCERTAIN. Pass 2 non necessaire.")
    else:
        deep_batches = [uncertain_ids[i:i + batch_size]
                        for i in range(0, nb_uncertain, batch_size)]
        nb_deep_batches = len(deep_batches)
        classified_deep = 0

        for idx, batch_ids in enumerate(deep_batches, start=1):
            batch_len = len(batch_ids)
            print(f"  Batch {idx}/{nb_deep_batches} ({batch_len} element(s))...")
            try:
                process_batch_classify_deep(gold_table, batch_ids, ai_model, ai_prompt_deep)
                classified_deep += batch_len
                print(f"    OK - {classified_deep}/{nb_uncertain} re-classifie(s) (pass 2)")
            except Exception as e:
                print(f"    ERREUR batch {idx}: {e}")
                print(f"    IDs: {batch_ids}")
                continue

    print(f"\n{'='*60}")
    print(f"Pipeline termine:")
    print(f"  - Markdown genere : {processed}/{total}")
    print(f"  - Classification pass 1 : {classified_light}/{total}")
    print(f"  - Items UNCERTAIN -> pass 2 : {nb_uncertain}")
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
