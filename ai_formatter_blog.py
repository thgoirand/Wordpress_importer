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
# MAGIC 2. **Pass 1 - Classification SEO** : classification sur titre + description par intent signals
# MAGIC    (BOFU > MOFU > TOFU par priorite). Retourne UNCERTAIN uniquement si input vide ou hors-sujet.
# MAGIC 3. **Pass 2 - Fallback Content Audit** : uniquement pour les items UNCERTAIN,
# MAGIC    analyse du contenu complet avec methodologie auteur + tie-breaker rule.
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

# System prompt for markdown formatting
AI_PROMPT = (
    "You are an expert in web content formatting. "
    "Convert this WordPress JSON into clean, well-structured markdown. "
    "Use headings (##, ###), lists, and format links properly. "
    "Return only the markdown, without any explanations. "
    "JSON: "
)

# --- PASS 1 : Classification legere sur titre + description (econome en tokens) ---
AI_PROMPT_CLASSIFICATION_LIGHT = (
    "Role: You are a Lead SEO Specialist for Cegid. "
    "You categorize content based on user intent signals found in Titles and Meta-Descriptions.\n\n"
    "Task: Assign the most probable funnel_stage to the provided input. "
    "Do not be afraid to infer the intent from verbs and context.\n\n"
    "DECISION GUIDELINES (Priority Order):\n\n"
    "1. BOFU (Intent: BUY/DECIDE)\n"
    "   Signals: Brand names (Cegid, XRP, Talentsoft, Loop, Notilus, Flex, etc.), "
    "\"Price\", \"Demo\", \"Case Study\", \"Success Story\", \"Webinar Product\", \"New Features\".\n"
    "   Logic: If it talks about the Vendor or the Product directly -> BOFU.\n\n"
    "2. MOFU (Intent: EVALUATE/ACT)\n"
    "   Signals: \"Software\", \"Tool\", \"Solution\", \"Comparison\", \"Checklist\", \"Template\".\n"
    "   Verbs of Action/Change: \"Automate\", \"Digitize\", \"Switch\", \"Choose\", "
    "\"Implement\", \"Optimize with...\".\n"
    "   Logic: If the user wants to change a process or find a tool -> MOFU.\n\n"
    "3. TOFU (Intent: LEARN/UNDERSTAND)\n"
    "   Signals: \"Definition\", \"Law\", \"Regulation\", \"Trends\", \"Challenges\", "
    "\"Why...\", \"Understanding...\".\n"
    "   Verbs of Management: \"Manage\", \"Deal with\", \"Anticipate\", \"Comply\".\n"
    "   Logic: If the user has a problem but is not looking for a tool yet -> TOFU.\n\n"
    "IMPORTANT - DEALING WITH AMBIGUITY:\n"
    "- If a title is generic (e.g., \"Optimizing HR\"), assume the user is in the TOFU (Learning) phase "
    "unless a software solution is explicitly mentioned.\n"
    "- ONLY return \"UNCERTAIN\" if the input is empty, meaningless, or completely unrelated "
    "to business software (e.g., \"Error 404\", \"Home Page\").\n\n"
    "Also determine:\n"
    "- has_regulatory_content (true/false/UNCERTAIN): Does the title or description reference "
    "a regulation, law, directive, standard, decree, or legal framework "
    "(GDPR, DORA, finance law, labor code, ISO standard, etc.)?\n"
    "- has_country_specific_context (true/false/UNCERTAIN): Does the title or description reference "
    "a specific country or national market (French taxation, Spanish market, Italian legislation, etc.)?\n"
    "  Return UNCERTAIN only if truly impossible to determine from title and description alone.\n\n"
    "Respond ONLY with a valid JSON object (no markdown, no explanation), in this exact format:\n"
    '{\"funnel_stage\": \"TOFU\", \"has_regulatory_content\": true, \"has_country_specific_context\": false}\n\n'
    "Allowed values:\n"
    "- funnel_stage: \"TOFU\", \"MOFU\", \"BOFU\", or \"UNCERTAIN\"\n"
    "- has_regulatory_content: true, false, or \"UNCERTAIN\"\n"
    "- has_country_specific_context: true, false, or \"UNCERTAIN\"\n\n"
    "Input:\n"
)

# --- PASS 2 : Classification profonde sur contenu complet (fallback) ---
AI_PROMPT_CLASSIFICATION_DEEP = (
    "Role: You are a Content Auditor. Your goal is to definitively classify a blog article "
    "based on its full content. You cannot answer \"Uncertain\".\n\n"
    "METHODOLOGY:\n"
    "Read the content and determine the primary goal of the author.\n\n"
    "1. Is it a Sales Pitch? (BOFU)\n"
    "   - Does the article mention \"Cegid\" products more than 3 times?\n"
    "   - Is the Call-to-Action (CTA) about a Demo or a Quote?\n"
    "   -> Verdict: BOFU\n\n"
    "2. Is it a Methodology/Tool Guide? (MOFU)\n"
    "   - Does the article explain how to solve a problem using technology/digitalization?\n"
    "   - Does it compare methods (e.g., Excel vs Software)?\n"
    "   -> Verdict: MOFU\n\n"
    "3. Is it Educational/News? (TOFU)\n"
    "   - Does the article explain a new law, a concept, or general HR/Finance advice?\n"
    "   - Could this article appear on a general news site without promoting Cegid?\n"
    "   -> Verdict: TOFU\n\n"
    "THE TIE-BREAKER RULE (Crucial):\n"
    "If the content gives general advice (TOFU) but mentions a specific Cegid product "
    "as the solution at the end:\n"
    "- If the product is mentioned only in the conclusion -> Classify as TOFU "
    "(It is an educational hook).\n"
    "- If the product is mentioned throughout the body -> Classify as MOFU "
    "(It is a product-led article).\n\n"
    "Also determine:\n"
    "- has_regulatory_content (true/false): Does the content reference any regulation, law, "
    "directive, standard, decree, or legal framework?\n"
    "- has_country_specific_context (true/false): Is the content specific to a particular "
    "country or national market?\n\n"
    "Respond ONLY with a valid JSON object (no markdown, no explanation), in this exact format:\n"
    '{\"funnel_stage\": \"TOFU\", \"has_regulatory_content\": true, \"has_country_specific_context\": false}\n\n'
    "Allowed values (no UNCERTAIN allowed - you MUST decide):\n"
    "- funnel_stage: \"TOFU\", \"MOFU\", or \"BOFU\"\n"
    "- has_regulatory_content: true or false\n"
    "- has_country_specific_context: true or false\n\n"
    "Full Content to analyze:\n"
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
    Identifies items with at least one UNCERTAIN field after pass 1.
    An item is UNCERTAIN if:
    - funnel_stage = 'UNCERTAIN'
    - has_regulatory_content IS NULL (was UNCERTAIN)
    - has_country_specific_context IS NULL (was UNCERTAIN)
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


def display_uncertain_details(gold_table: str, candidate_ids: list):
    """
    Displays detailed information about UNCERTAIN items after pass 1,
    showing which fields are uncertain for each item.
    Useful to evaluate whether the two-pass approach is relevant.
    """
    ids_str = ", ".join(str(id_val) for id_val in candidate_ids)
    query = f"""
    SELECT
        id,
        site_id,
        title,
        LEFT(COALESCE(meta_description, excerpt, ''), 120) AS description_preview,
        CASE WHEN has_regulatory_content IS NULL THEN 'UNCERTAIN' ELSE CAST(has_regulatory_content AS STRING) END AS regulatory,
        CASE WHEN has_country_specific_context IS NULL THEN 'UNCERTAIN' ELSE CAST(has_country_specific_context AS STRING) END AS country_specific,
        funnel_stage
    FROM {gold_table}
    WHERE id IN ({ids_str})
      AND (
          funnel_stage = 'UNCERTAIN'
          OR has_regulatory_content IS NULL
          OR has_country_specific_context IS NULL
      )
    ORDER BY site_id, id
    """
    df_uncertain = spark.sql(query)
    nb = df_uncertain.count()

    if nb == 0:
        print("  No UNCERTAIN items found.")
        return

    print(f"\n  Detail of {nb} UNCERTAIN item(s):")
    print(f"  {'-'*80}")

    rows = df_uncertain.collect()
    for row in rows:
        uncertain_fields = []
        if row["regulatory"] == "UNCERTAIN":
            uncertain_fields.append("regulatory")
        if row["country_specific"] == "UNCERTAIN":
            uncertain_fields.append("country_specific")
        if row["funnel_stage"] == "UNCERTAIN":
            uncertain_fields.append("funnel_stage")

        print(f"  ID {row['id']} [{row['site_id']}] - {row['title']}")
        print(f"    Description: {row['description_preview']}")
        print(f"    Uncertain fields: {', '.join(uncertain_fields)}")
        print(f"    Values -> regulatory={row['regulatory']}, country={row['country_specific']}, funnel={row['funnel_stage']}")

    print(f"  {'-'*80}")
    display(df_uncertain)


def process_batch_classify_deep(gold_table: str, batch_ids: list, ai_model: str,
                                 ai_prompt_deep: str):
    """
    Pass 2 : Classification par Content Audit sur le contenu complet (content_text).
    Uniquement pour les items dont la pass 1 a retourne UNCERTAIN.
    Methodologie : analyse du but primaire de l'auteur + tie-breaker rule.
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
        print("No items to process.")
        return 0

    batches = [all_ids[i:i + batch_size] for i in range(0, total, batch_size)]
    nb_batches = len(batches)

    print(f"Processing {total} item(s) in {nb_batches} batch(es) of {batch_size} max")
    print(f"Model: {ai_model}")
    print(f"Gold table: {gold_table}")
    print(f"{'='*60}")

    # --- Phase 1: Markdown generation ---
    print(f"\n--- Phase 1/3: Markdown generation ---")
    processed = 0
    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"  Batch {idx}/{nb_batches} ({batch_len} item(s))...")
        try:
            process_batch_markdown(gold_table, batch_ids, ai_model, ai_prompt)
            processed += batch_len
            print(f"    OK - {processed}/{total} markdown generated")
        except Exception as e:
            print(f"    ERROR batch {idx}: {e}")
            print(f"    IDs: {batch_ids}")
            continue

    # --- Phase 2: Light classification (title + description) ---
    print(f"\n--- Phase 2/3: Light classification (title + description) ---")
    classified_light = 0
    for idx, batch_ids in enumerate(batches, start=1):
        batch_len = len(batch_ids)
        print(f"  Batch {idx}/{nb_batches} ({batch_len} item(s))...")
        try:
            process_batch_classify_light(gold_table, batch_ids, ai_model, ai_prompt_light)
            classified_light += batch_len
            print(f"    OK - {classified_light}/{total} classified (pass 1)")
        except Exception as e:
            print(f"    ERROR batch {idx}: {e}")
            print(f"    IDs: {batch_ids}")
            continue

    # --- Uncertain items detail (to evaluate two-pass relevance) ---
    uncertain_ids = get_uncertain_ids(gold_table, all_ids)
    nb_uncertain = len(uncertain_ids)

    print(f"\n--- Uncertain items detail after pass 1: {nb_uncertain}/{total} ---")
    display_uncertain_details(gold_table, all_ids)

    # --- Phase 3: Deep classification (full content, UNCERTAIN only) ---
    print(f"\n--- Phase 3/3: Deep classification ({nb_uncertain} UNCERTAIN item(s)) ---")

    if nb_uncertain == 0:
        print("  No UNCERTAIN items. Pass 2 not needed.")
    else:
        deep_batches = [uncertain_ids[i:i + batch_size]
                        for i in range(0, nb_uncertain, batch_size)]
        nb_deep_batches = len(deep_batches)
        classified_deep = 0

        for idx, batch_ids in enumerate(deep_batches, start=1):
            batch_len = len(batch_ids)
            print(f"  Batch {idx}/{nb_deep_batches} ({batch_len} item(s))...")
            try:
                process_batch_classify_deep(gold_table, batch_ids, ai_model, ai_prompt_deep)
                classified_deep += batch_len
                print(f"    OK - {classified_deep}/{nb_uncertain} re-classified (pass 2)")
            except Exception as e:
                print(f"    ERROR batch {idx}: {e}")
                print(f"    IDs: {batch_ids}")
                continue

    print(f"\n{'='*60}")
    print(f"Pipeline completed:")
    print(f"  - Markdown generated: {processed}/{total}")
    print(f"  - Classification pass 1: {classified_light}/{total}")
    print(f"  - UNCERTAIN items -> pass 2: {nb_uncertain}")
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
