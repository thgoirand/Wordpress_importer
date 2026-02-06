# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import des Study Cases WordPress
# MAGIC
# MAGIC Ce notebook permet de :
# MAGIC - Recuperer les study cases via l'API WordPress REST `/wp-json/wp/v2/study-case`
# MAGIC - Stocker les contenus dans la table `cegid_website_pages` avec gestion des offsets
# MAGIC - Supporter l'incremental via le tracking des IDs deja importes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import des utilitaires communs

# COMMAND ----------

# MAGIC %run ./wordpress_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration specifique aux study cases

# COMMAND ----------

# Configuration Databricks pour les study cases (meme table que posts/pages)
STUDY_CASE_TABLE_CONFIG = {
    "catalog": DATABRICKS_CATALOG,
    "schema": DATABRICKS_SCHEMA,
    "table_name": "cegid_website_pages"
}

# Type de contenu et endpoint API
STUDY_CASE_CONTENT_TYPE = "study_case"
STUDY_CASE_ENDPOINT = "/study-case"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema de la table cegid_website_pages
# MAGIC
# MAGIC Le schema est identique a celui utilise par le blog_importer (posts/pages).
# MAGIC Les study cases sont stockes dans la meme table avec `content_type = 'study_case'`.

# COMMAND ----------

# Mapping des champs WordPress vers le schema
STUDY_CASE_FIELD_MAPPING = {
    # --- IDENTIFIANTS ---
    "id": None,                          # Calcule via calculate_composite_id()
    "wp_id": "id",
    "content_type": None,                # "study_case"
    "site_id": None,                     # Config du site

    # --- METADONNEES ---
    "slug": "slug",
    "url": "link",
    "title": "title.rendered",

    # --- SEO ---
    "meta_description": "yoast_head_json.description",
    "meta_title": "yoast_head_json.title",
    "meta_keyword": "_yoast_wpseo_focuskw",             # Focus keyword Yoast (top-level field)
    "noindex": "yoast_head_json.robots.index",       # Robot noindex

    # --- CONTENU ---
    "content_raw": None,                 # Assemble depuis acf.vah_flexible_main (wysiwyg blocks)
    "content_text": None,                # Calcule (HTML nettoye)
    "excerpt": "excerpt.rendered",

    # --- TAXONOMIES ---
    "categories": None,                  # Study cases n'ont pas de categories
    "tags": None,                        # Study cases n'ont pas de tags
    "custom_taxonomies": None,           # occupation, company, solution, secteur, product_type

    # --- DATES ---
    "date_published": "date",
    "date_modified": "modified",
    "date_imported": None,               # datetime.now()

    # --- AUTEUR & STATUT ---
    "status": "status",
    "author_id": "author",

    # --- MEDIA ---
    "featured_image_url": "yoast_head_json.og_image.0.url",  # Fallback fiable vs _embedded

    # --- LANGUE ---
    "language": "lang",

    # --- DONNEES BRUTES ---
    "raw_json": None,
}

# Schema PySpark pour les contenus (identique au blog_importer)
STUDY_CASE_SCHEMA = StructType([
    # --- IDENTIFIANTS ---
    StructField("id", LongType(), False),
    StructField("wp_id", IntegerType(), False),
    StructField("content_type", StringType(), False),
    StructField("site_id", StringType(), False),

    # --- METADONNEES ---
    StructField("slug", StringType(), True),
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),

    # --- SEO ---
    StructField("meta_description", StringType(), True),
    StructField("meta_title", StringType(), True),
    StructField("meta_keyword", StringType(), True),
    StructField("noindex", BooleanType(), True),

    # --- CONTENU ---
    StructField("content_raw", StringType(), True),
    StructField("content_text", StringType(), True),
    StructField("excerpt", StringType(), True),

    # --- TAXONOMIES ---
    StructField("categories", ArrayType(IntegerType()), True),
    StructField("tags", ArrayType(IntegerType()), True),
    StructField("custom_taxonomies", MapType(StringType(), ArrayType(IntegerType())), True),

    # --- DATES ---
    StructField("date_published", TimestampType(), True),
    StructField("date_modified", TimestampType(), True),
    StructField("date_imported", TimestampType(), False),

    # --- AUTEUR & STATUT ---
    StructField("status", StringType(), True),
    StructField("author_id", IntegerType(), True),

    # --- MEDIA ---
    StructField("featured_image_url", StringType(), True),

    # --- LANGUE ---
    StructField("language", StringType(), True),

    # --- KEY FIGURES (use cases) ---
    StructField("key_figures", ArrayType(StringType()), True),

    # --- DONNEES BRUTES ---
    StructField("raw_json", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fonction de transformation des study cases

# COMMAND ----------

def _build_study_case_content_raw(acf_main):
    """
    Assemble le contenu HTML brut d'un study case a partir des blocs ACF.
    Extrait le contenu des blocs wysiwyg-section (contenu editorial principal).
    Les blocs testimonial-cards et product-section sont ignores (donnees structurees
    conservees dans raw_json).
    """
    parts = []

    for block in acf_main:
        layout = block.get('acf_fc_layout', '')

        if layout == 'wysiwyg-section':
            content = block.get('wysiwyg', '')
            if content:
                parts.append(content)

    return "\n".join(parts) if parts else None


def transform_study_case_item(item: Dict, site_id: str, site_config: Dict) -> Dict:
    """
    Transforme un item study case WordPress en format standardise pour Databricks.

    Schema cible: gdp_cdt_dev_04_gld.sandbox_mkt.cegid_website_pages
    Content type: study_case

    Note: Le contenu principal est dans acf.vah_flexible_main (blocs wysiwyg-section),
    pas dans content.rendered qui est vide pour les study cases.
    """
    wp_id = item.get('id')
    content_type = STUDY_CASE_CONTENT_TYPE

    # === CONTENU: Assemble depuis les blocs ACF ===
    acf_main = get_nested_value(item, 'acf.vah_flexible_main', [])
    content_raw = _build_study_case_content_raw(acf_main)
    content_text = clean_html_content(content_raw) if content_raw else None
    excerpt_raw = get_nested_value(item, 'excerpt.rendered', '')

    # === MEDIA: og_image plus fiable que _embedded ===
    featured_image_url = get_nested_value(item, 'yoast_head_json.og_image.0.url')

    # === LANGUE ===
    language = item.get('lang') or site_config.get("language", "fr")

    # === SEO: noindex ===
    robots_index = get_nested_value(item, 'yoast_head_json.robots.index')
    noindex = robots_index == 'noindex' if robots_index else None

    # === SEO: meta_keyword (champ racine, absent de yoast_head_json) ===
    meta_keyword = item.get('_yoast_wpseo_focuskw') or None

    # === TAXONOMIES CUSTOM ===
    custom_taxonomies = {}
    for key in ['occupation', 'company', 'solution', 'secteur', 'product_type']:
        if key in item and isinstance(item[key], list) and item[key]:
            custom_taxonomies[key] = item[key]

    return {
        # --- IDENTIFIANTS ---
        "id": calculate_composite_id(wp_id, content_type, site_id),
        "wp_id": wp_id,
        "content_type": content_type,
        "site_id": site_id,

        # --- METADONNEES ---
        "slug": item.get('slug'),
        "url": item.get('link'),
        "title": html.unescape(get_nested_value(item, 'title.rendered', '')),

        # --- SEO ---
        "meta_description": get_nested_value(item, 'yoast_head_json.description'),
        "meta_title": get_nested_value(item, 'yoast_head_json.title'),
        "meta_keyword": item.get('_yoast_wpseo_focuskw') or get_nested_value(item, 'yoast_head_json.focuskw'),
        "noindex": noindex,

        # --- CONTENU ---
        "content_raw": content_raw,
        "content_text": content_text,
        "excerpt": clean_html_content(excerpt_raw),

        # --- TAXONOMIES ---
        "categories": [],
        "tags": [],
        "custom_taxonomies": custom_taxonomies if custom_taxonomies else None,

        # --- DATES ---
        "date_published": parse_wp_date(item.get('date')),
        "date_modified": parse_wp_date(item.get('modified')),
        "date_imported": datetime.now(),

        # --- AUTEUR & STATUT ---
        "status": item.get('status'),
        "author_id": item.get('author'),

        # --- MEDIA ---
        "featured_image_url": featured_image_url,

        # --- LANGUE ---
        "language": language,

        # --- KEY FIGURES (renseigne par ai_formatter_use_case) ---
        "key_figures": None,

        # --- DONNEES BRUTES ---
        "raw_json": json.dumps(item, ensure_ascii=False),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fonction d'upsert des study cases

# COMMAND ----------

def upsert_study_cases(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Upsert (MERGE) des study cases dans la table Delta cegid_website_pages.
    Met a jour les contenus existants, insere les nouveaux.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    # Cree une vue temporaire
    df.createOrReplaceTempView("new_study_cases")

    # MERGE pour upsert
    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING new_study_cases AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                title = source.title,
                content_raw = source.content_raw,
                content_text = source.content_text,
                excerpt = source.excerpt,
                meta_description = source.meta_description,
                categories = source.categories,
                tags = source.tags,
                custom_taxonomies = source.custom_taxonomies,
                date_modified = source.date_modified,
                date_imported = source.date_imported,
                status = source.status,
                featured_image_url = source.featured_image_url,
                raw_json = source.raw_json
        WHEN NOT MATCHED THEN
            INSERT (id, wp_id, content_type, site_id, slug, url, title,
                    meta_description, meta_title, meta_keyword, noindex,
                    content_raw, content_text, excerpt,
                    categories, tags, custom_taxonomies,
                    date_published, date_modified, date_imported,
                    status, author_id, featured_image_url, language, raw_json)
            VALUES (source.id, source.wp_id, source.content_type, source.site_id, source.slug, source.url, source.title,
                    source.meta_description, source.meta_title, source.meta_keyword, source.noindex,
                    source.content_raw, source.content_text, source.excerpt,
                    source.categories, source.tags, source.custom_taxonomies,
                    source.date_published, source.date_modified, source.date_imported,
                    source.status, source.author_id, source.featured_image_url, source.language, source.raw_json)
    """)

    print(f"Upsert study cases termine dans {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pipeline principal d'import des study cases

# COMMAND ----------

def run_study_case_import_pipeline(sites_to_import: List[str] = WP_SITES_TO_IMPORT,
                                    incremental: bool = True):
    """
    Execute le pipeline d'import des study cases pour un ou plusieurs sites.

    Route API: /wp-json/wp/v2/study-case
    Table cible: gdp_cdt_dev_04_gld.sandbox_mkt.cegid_website_pages (content_type = 'study_case')

    Args:
        sites_to_import: Liste des site_id a importer (ex: ["fr", "es"])
        incremental: Si True, importe seulement les nouveaux contenus
    """
    catalog = STUDY_CASE_TABLE_CONFIG["catalog"]
    schema = STUDY_CASE_TABLE_CONFIG["schema"]
    table_name = STUDY_CASE_TABLE_CONFIG["table_name"]

    # Cree la table si necessaire
    create_delta_table(
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        spark_schema=STUDY_CASE_SCHEMA,
        partition_by=["site_id", "content_type"]
    )

    total_imported = 0

    # Boucle sur les sites
    for site_id in sites_to_import:
        if site_id not in WP_SITES:
            print(f"Site '{site_id}' non configure, ignore")
            continue

        site_config = WP_SITES[site_id]
        site_label = site_config.get("label", site_id)

        print(f"\n{'#'*60}")
        print(f"SITE: {site_label} ({site_id})")
        print(f"   URL: {WORDPRESS_CONFIG['base_url']}/{site_config.get('prefix', '')}")
        print(f"{'#'*60}")

        # Initialise le connecteur pour ce site
        connector = WordPressConnector(site_id, site_config)

        print(f"\n{'='*50}")
        print(f"[{site_label}] Import: Study Cases (study_case)")
        print(f"{'='*50}")

        # Recupere la derniere date de modification pour import incremental
        modified_after = None
        if incremental:
            modified_after = get_last_modified_date(catalog, schema, table_name, site_id, STUDY_CASE_CONTENT_TYPE)
            if modified_after:
                print(f"Mode incremental - contenus modifies apres: {modified_after}")

        # Recupere les study cases WordPress via /wp-json/wp/v2/study-case
        items = connector.fetch_all_content(
            content_type=STUDY_CASE_CONTENT_TYPE,
            endpoint=STUDY_CASE_ENDPOINT,
            modified_after=modified_after
        )

        if not items:
            print(f"Aucun nouveau study case a importer pour {site_label}")
            continue

        # Transforme les items
        transformed_items = [
            transform_study_case_item(item, site_id, site_config)
            for item in items
        ]

        # Cree le DataFrame
        df = spark.createDataFrame(transformed_items, STUDY_CASE_SCHEMA)

        # Upsert dans la table
        upsert_study_cases(df, catalog, schema, table_name)

        total_imported += len(transformed_items)
        print(f"[{site_label}] {len(transformed_items)} study case(s) importe(s)")

    print(f"\n{'#'*60}")
    print(f"Import study cases termine! Total: {total_imported} study cases importes")
    print(f"   Sites traites: {', '.join(sites_to_import)}")
    print(f"{'#'*60}")

    return total_imported

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Execution

# COMMAND ----------

# =============================================================================
# EXEMPLES D'EXECUTION
# =============================================================================

# Import des study cases d'un seul site (FR)
# run_study_case_import_pipeline(sites_to_import=["fr"], incremental=False)

# Import incremental des study cases d'un seul site
run_study_case_import_pipeline(sites_to_import=["fr"], incremental=True)

# Import des study cases de plusieurs sites
# run_study_case_import_pipeline(sites_to_import=["fr", "es", "uk"], incremental=False)

# Import des study cases de TOUS les sites configures
# run_study_case_import_pipeline(sites_to_import=list(WP_SITES.keys()), incremental=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification des donnees

# COMMAND ----------

# Affiche un apercu des study cases importes par site
full_table = f"{STUDY_CASE_TABLE_CONFIG['catalog']}.{STUDY_CASE_TABLE_CONFIG['schema']}.{STUDY_CASE_TABLE_CONFIG['table_name']}"

display(spark.sql(f"""
    SELECT
        site_id,
        content_type,
        language,
        COUNT(*) as nb_items,
        MIN(date_published) as oldest,
        MAX(date_published) as newest,
        MAX(date_imported) as last_import
    FROM {full_table}
    WHERE content_type = '{STUDY_CASE_CONTENT_TYPE}'
    GROUP BY site_id, content_type, language
    ORDER BY site_id
"""))

# COMMAND ----------

# Apercu des derniers study cases par site
display(spark.sql(f"""
    SELECT
        site_id,
        id,
        wp_id,
        title,
        url,
        language,
        LEFT(content_text, 200) as content_preview,
        date_published
    FROM {full_table}
    WHERE content_type = '{STUDY_CASE_CONTENT_TYPE}'
    ORDER BY site_id, date_published DESC
    LIMIT 20
"""))

