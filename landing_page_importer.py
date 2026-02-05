# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import des Landing Pages WordPress
# MAGIC
# MAGIC Ce notebook permet de :
# MAGIC - Recuperer les landing pages via l'API WordPress REST `/wp-json/wp/v2/landing-page`
# MAGIC - Stocker les contenus dans la table `cegid_website` avec le content_type `landing_page`
# MAGIC - Supporter l'incremental via le tracking des IDs deja importes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import des utilitaires communs

# COMMAND ----------

# MAGIC %run ./wordpress_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration specifique aux landing pages

# COMMAND ----------

# Configuration Databricks pour les landing pages (meme table que posts/pages)
LANDING_PAGE_TABLE_CONFIG = {
    "catalog": DATABRICKS_CATALOG,
    "schema": DATABRICKS_SCHEMA,
    "table_name": "cegid_website"
}

# Endpoint API pour les landing pages
LANDING_PAGE_ENDPOINT = "/landing-page"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema de la table cegid_website
# MAGIC
# MAGIC Le schema est identique a celui des posts/pages (partage via `cegid_website`).

# COMMAND ----------

# Schema PySpark pour les contenus (identique a blog_importer)
LANDING_PAGE_SCHEMA = StructType([
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

    # --- AI FORMATTING ---
    StructField("date_formatted", TimestampType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fonction de transformation des landing pages

# COMMAND ----------

def extract_acf_content(item: Dict) -> str:
    """
    Reconstruit le contenu HTML a partir des blocs ACF vah_flexible_main.

    Les landing pages n'ont pas de contenu dans content.rendered,
    le contenu reel est dans les blocs ACF flexibles :
    - wysiwyg-section : contenu HTML dans le champ 'wysiwyg'
    - testimonial-cards : temoignages clients (auteur, job, citation)
    - product-section : section produits avec contenu + IDs produits
    """
    acf_main = get_nested_value(item, 'acf.vah_flexible_main', [])
    if not acf_main:
        return ''

    html_parts = []

    for block in acf_main:
        layout = block.get('acf_fc_layout', '')

        if layout == 'wysiwyg-section':
            wysiwyg = block.get('wysiwyg', '')
            if wysiwyg:
                html_parts.append(wysiwyg)

        elif layout == 'testimonial-cards':
            cards = block.get('testimonial_cards', [])
            for card in cards:
                author = card.get('author', '').strip()
                job = card.get('job', '')
                content = card.get('content', '')
                if content:
                    html_parts.append(
                        f'<blockquote><p>{content}</p>'
                        f'<cite>{author}, {job}</cite></blockquote>'
                    )

        elif layout == 'product-section':
            content = block.get('content', '')
            if content:
                html_parts.append(content)

    return '\n'.join(html_parts)


def transform_landing_page_item(item: Dict, site_id: str, site_config: Dict) -> Dict:
    """
    Transforme une landing page WordPress en format standardise pour Databricks.

    Schema cible: gdp_cdt_dev_04_gld.sandbox_mkt.cegid_website
    Content type: landing_page (offset 20_000_000)

    Note: content.rendered est vide pour ce type de contenu.
    Le contenu reel est reconstruit depuis acf.vah_flexible_main.
    """
    wp_id = item.get('id')
    content_type = "landing_page"

    # === CONTENU ===
    # content.rendered est vide, on reconstruit depuis les blocs ACF
    content_raw = extract_acf_content(item)
    content_text = clean_html_content(content_raw)
    excerpt_raw = get_nested_value(item, 'excerpt.rendered', '')

    # === MEDIA ===
    featured_image_url = get_nested_value(item, '_embedded.wp:featuredmedia.0.source_url')

    # === LANGUE ===
    language = item.get('lang') or site_config.get("language", "fr")

    # === SEO: noindex ===
    robots_index = get_nested_value(item, 'yoast_head_json.robots.index')
    noindex = robots_index == 'noindex' if robots_index else None

    # === TAXONOMIES CUSTOM ===
    custom_taxonomies = {}
    for key in ['occupation', 'solution', 'secteur', 'product_type', 'company']:
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
        "title": get_nested_value(item, 'title.rendered', ''),

        # --- SEO ---
        "meta_description": get_nested_value(item, 'yoast_head_json.description'),
        "meta_title": get_nested_value(item, 'yoast_head_json.title'),
        "meta_keyword": get_nested_value(item, 'yoast_head_json.focuskw'),
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

        # --- AI FORMATTING ---
        "date_formatted": None,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fonction d'upsert des landing pages

# COMMAND ----------

def upsert_landing_pages(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Upsert (MERGE) des landing pages dans la table Delta cegid_website.
    Met a jour les contenus existants, insere les nouveaux.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    # Cree une vue temporaire
    df.createOrReplaceTempView("new_landing_pages")

    # MERGE pour upsert
    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING new_landing_pages AS source
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

    print(f"Upsert landing pages termine dans {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Pipeline principal d'import des landing pages

# COMMAND ----------

def run_landing_page_import_pipeline(sites_to_import: List[str] = WP_SITES_TO_IMPORT,
                                      incremental: bool = True):
    """
    Execute le pipeline d'import des landing pages pour un ou plusieurs sites.

    Route API: /wp-json/wp/v2/landing-page
    Table cible: gdp_cdt_dev_04_gld.sandbox_mkt.cegid_website
    Content type: landing_page

    Args:
        sites_to_import: Liste des site_id a importer (ex: ["fr", "es"])
        incremental: Si True, importe seulement les nouvelles landing pages
    """
    catalog = LANDING_PAGE_TABLE_CONFIG["catalog"]
    schema = LANDING_PAGE_TABLE_CONFIG["schema"]
    table_name = LANDING_PAGE_TABLE_CONFIG["table_name"]

    # Cree la table si necessaire (schema identique a cegid_website)
    create_delta_table(
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        spark_schema=LANDING_PAGE_SCHEMA,
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
        print(f"[{site_label}] Import: Landing Pages (landing_page)")
        print(f"{'='*50}")

        # Recupere le dernier ID pour import incremental
        since_id = None
        if incremental:
            since_id = get_last_imported_id(catalog, schema, table_name, site_id, "landing_page")
            if since_id:
                print(f"Mode incremental - depuis ID: {since_id}")

        # Recupere les landing pages via /wp-json/wp/v2/landing-page
        items = connector.fetch_all_content(
            content_type="landing_page",
            endpoint=LANDING_PAGE_ENDPOINT,
            since_id=since_id
        )

        if not items:
            print(f"Aucune nouvelle landing page a importer pour {site_label}")
            continue

        # Transforme les items
        transformed_items = [
            transform_landing_page_item(item, site_id, site_config)
            for item in items
        ]

        # Cree le DataFrame
        df = spark.createDataFrame(transformed_items, LANDING_PAGE_SCHEMA)

        # Upsert dans la table
        upsert_landing_pages(df, catalog, schema, table_name)

        total_imported += len(transformed_items)
        print(f"[{site_label}] {len(transformed_items)} landing page(s) importee(s)")

    print(f"\n{'#'*60}")
    print(f"Import landing pages termine! Total: {total_imported} landing pages importees")
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

# Import des landing pages d'un seul site (FR)
# run_landing_page_import_pipeline(sites_to_import=["fr"], incremental=False)

# Import incremental des landing pages d'un seul site
# run_landing_page_import_pipeline(sites_to_import=["fr"], incremental=True)

# Import des landing pages de plusieurs sites
# run_landing_page_import_pipeline(sites_to_import=["fr", "es", "uk"], incremental=False)

# Import des landing pages de TOUS les sites configures
# run_landing_page_import_pipeline(sites_to_import=list(WP_SITES.keys()), incremental=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verification des donnees

# COMMAND ----------

# Affiche un apercu des landing pages importees par site
full_table = f"{LANDING_PAGE_TABLE_CONFIG['catalog']}.{LANDING_PAGE_TABLE_CONFIG['schema']}.{LANDING_PAGE_TABLE_CONFIG['table_name']}"

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
    WHERE content_type = 'landing_page'
    GROUP BY site_id, content_type, language
    ORDER BY site_id
"""))

# COMMAND ----------

# Apercu des dernieres landing pages par site
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
    WHERE content_type = 'landing_page'
    ORDER BY site_id, date_published DESC
    LIMIT 20
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. AI functions (optionnel)
# MAGIC
# MAGIC Le formatage AI des contenus est realise dans un notebook dedie `ai_formatter`
# MAGIC afin d'eviter les timeouts du serverless compute sur de gros volumes.
# MAGIC
# MAGIC Le notebook traite les elements par batch de 5 via AI_QUERY.
# MAGIC Les landing pages dans `cegid_website` sont automatiquement prises en charge.
# MAGIC
# MAGIC **Usage:** Executer `ai_formatter` apres l'import des contenus.
