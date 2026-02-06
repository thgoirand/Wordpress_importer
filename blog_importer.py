# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import des Articles WordPress
# MAGIC
# MAGIC Ce notebook permet de :
# MAGIC - Recuperer les articles (posts) via l'API WordPress REST
# MAGIC - Stocker les contenus dans la table `cegid_website` avec gestion des offsets
# MAGIC - Supporter l'incremental via le tracking des IDs deja importes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import des utilitaires communs

# COMMAND ----------

# MAGIC %run ./wordpress_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration specifique aux articles

# COMMAND ----------

# Configuration Databricks pour les contenus (posts)
CONTENT_TABLE_CONFIG = {
    "catalog": DATABRICKS_CATALOG,
    "schema": DATABRICKS_SCHEMA,
    "table_name": "cegid_website"
}

# Configuration du type de contenu
CONTENT_TYPE = "post"
CONTENT_ENDPOINT = "/posts"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema de la table cegid_website

# COMMAND ----------

# Mapping des champs WordPress vers le schema
FIELD_MAPPING = {
    # --- IDENTIFIANTS ---
    "id": None,                          # Calcule via calculate_composite_id()
    "wp_id": "id",
    "content_type": None,                # Passe en parametre
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
    "content_raw": "content.rendered",
    "content_text": None,                # Calcule (HTML nettoye)
    "excerpt": "excerpt.rendered",

    # --- TAXONOMIES ---
    "categories": "categories",
    "tags": "tags",
    "custom_taxonomies": None,           # Extraction custom

    # --- DATES ---
    "date_published": "date",
    "date_modified": "modified",
    "date_imported": None,               # datetime.now()

    # --- AUTEUR & STATUT ---
    "status": "status",
    "author_id": "author",

    # --- MEDIA ---
    "featured_image_url": "_embedded.wp:featuredmedia.0.source_url",

    # --- LANGUE ---
    "language": "lang",

    # --- DONNEES BRUTES ---
    "raw_json": None,
}

# Schema PySpark pour les contenus
CONTENT_SCHEMA = StructType([
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

    # --- DONNEES BRUTES ---
    StructField("raw_json", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fonction de transformation des contenus

# COMMAND ----------

def transform_content_item(item: Dict, content_type: str, site_id: str, site_config: Dict) -> Dict:
    """
    Transforme un item WordPress en format standardise pour Databricks.

    Schema cible: gdp_cdt_dev_04_gld.sandbox_mkt.cegid_website
    """
    wp_id = item.get('id')

    # === CONTENU ===
    content_raw = get_nested_value(item, 'content.rendered', '')
    content_text = clean_html_content(content_raw)
    excerpt_raw = get_nested_value(item, 'excerpt.rendered', '')

    # === MEDIA ===
    featured_image_url = get_nested_value(item, '_embedded.wp:featuredmedia.0.source_url')

    # === LANGUE ===
    language = item.get('lang') or site_config.get("language", "fr")

    # === SEO: noindex ===
    # Yoast stocke "index" ou "noindex" dans robots.index
    robots_index = get_nested_value(item, 'yoast_head_json.robots.index')
    noindex = robots_index == 'noindex' if robots_index else None

    # === TAXONOMIES CUSTOM ===
    custom_taxonomies = {}
    # Collecte toutes les taxonomies custom (occupation, etc.)
    for key in ['occupation', 'solution', 'secteur', 'product_type']:
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
        "categories": item.get('categories', []),
        "tags": item.get('tags', []),
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

        # --- DONNEES BRUTES ---
        "raw_json": json.dumps(item, ensure_ascii=False),
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fonction d'upsert des contenus

# COMMAND ----------

def upsert_content(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Upsert (MERGE) des contenus dans la table Delta.
    Met a jour les contenus existants, insere les nouveaux.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    # Cree une vue temporaire
    df.createOrReplaceTempView("new_content")

    # MERGE pour upsert
    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING new_content AS source
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
            INSERT *
    """)

    print(f"Upsert termine dans {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pipeline principal d'import

# COMMAND ----------

def run_import_pipeline(sites_to_import: List[str] = WP_SITES_TO_IMPORT,
                        incremental: bool = True):
    """
    Execute le pipeline d'import des articles (posts) pour un ou plusieurs sites.

    Args:
        sites_to_import: Liste des site_id a importer (ex: ["fr", "es"])
        incremental: Si True, importe seulement les nouveaux contenus
    """
    catalog = CONTENT_TABLE_CONFIG["catalog"]
    schema = CONTENT_TABLE_CONFIG["schema"]
    table_name = CONTENT_TABLE_CONFIG["table_name"]

    # Cree la table si necessaire
    create_delta_table(
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        spark_schema=CONTENT_SCHEMA,
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
        print(f"[{site_label}] Import: articles ({CONTENT_TYPE})")
        print(f"{'='*50}")

        # Recupere la derniere date de modification pour import incremental
        modified_after = None
        if incremental:
            modified_after = get_last_modified_date(catalog, schema, table_name, site_id, CONTENT_TYPE)
            if modified_after:
                print(f"Mode incremental - contenus modifies apres: {modified_after}")

        # Recupere les articles WordPress
        items = connector.fetch_all_content(
            content_type=CONTENT_TYPE,
            endpoint=CONTENT_ENDPOINT,
            modified_after=modified_after
        )

        if not items:
            print(f"Aucun nouvel article a importer")
            continue

        # Transforme les items
        transformed_items = [
            transform_content_item(item, CONTENT_TYPE, site_id, site_config)
            for item in items
        ]

        # Cree le DataFrame
        df = spark.createDataFrame(transformed_items, CONTENT_SCHEMA)

        # Upsert dans la table
        upsert_content(df, catalog, schema, table_name)

        total_imported += len(transformed_items)
        print(f"[{site_label}] {len(transformed_items)} article(s) importe(s)")

    print(f"\n{'#'*60}")
    print(f"Import termine! Total: {total_imported} articles importes")
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

# Import d'un seul site (FR), tous les articles
# run_import_pipeline(sites_to_import=["fr"], incremental=False)

# Import incremental d'un seul site
run_import_pipeline(sites_to_import=["fr"], incremental=True)

# Import de plusieurs sites
# run_import_pipeline(sites_to_import=["fr", "es", "uk"], incremental=False)

# Import de TOUS les sites configures
# run_import_pipeline(sites_to_import=list(WP_SITES.keys()), incremental=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification des donnees

# COMMAND ----------

# Affiche un apercu des donnees importees par site et type
full_table = f"{CONTENT_TABLE_CONFIG['catalog']}.{CONTENT_TABLE_CONFIG['schema']}.{CONTENT_TABLE_CONFIG['table_name']}"

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
    GROUP BY site_id, content_type, language
    ORDER BY site_id, content_type
"""))

# COMMAND ----------

# Apercu des derniers articles par site
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
    WHERE content_type = 'post'
    ORDER BY site_id, date_published DESC
    LIMIT 20
"""))

