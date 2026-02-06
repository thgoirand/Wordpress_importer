# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import des Produits WordPress (ACF)
# MAGIC
# MAGIC Ce notebook permet de :
# MAGIC - Recuperer les pages produit via l'API WordPress REST `/wp-json/wp/v2/product`
# MAGIC - Extraire les champs ACF flexibles (header, arguments, features, resources)
# MAGIC - Stocker les produits dans la table `cegid_website` (content_type = "product")
# MAGIC - Supporter l'import incremental via la date de derniere modification

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import des utilitaires communs

# COMMAND ----------

# MAGIC %run ./wordpress_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration specifique aux produits

# COMMAND ----------

# Configuration Databricks pour les produits (meme table que blog_importer)
PRODUCT_TABLE_CONFIG = {
    "catalog": DATABRICKS_CATALOG,
    "schema": DATABRICKS_SCHEMA,
    "table_name": "cegid_website"
}

# Endpoint API pour les produits
PRODUCT_ENDPOINT = "/product"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema cegid_website (identique a blog_importer)

# COMMAND ----------

# Schema PySpark pour les contenus (identique a blog_importer)
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

    # --- AI FORMATTING ---
    StructField("date_formatted", TimestampType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mapping des champs produit vers cegid_website

# COMMAND ----------

PRODUCT_FIELD_MAPPING = {
    # --- IDENTIFIANTS ---
    "id": None,                          # Calcule via calculate_composite_id()
    "wp_id": "id",
    "content_type": None,                # "product"
    "site_id": None,                     # Config du site

    # --- METADONNEES ---
    "slug": "slug",
    "url": "link",
    "title": "title.rendered",

    # --- SEO (Yoast) ---
    "meta_description": "yoast_head_json.description",
    "meta_title": "yoast_head_json.title",
    "meta_keyword": "_yoast_wpseo_focuskw",             # Focus keyword Yoast (top-level field)
    "noindex": "yoast_head_json.robots.index",

    # --- CONTENU (assemble depuis les champs ACF) ---
    "content_raw": None,                 # Assemble depuis ACF header + main
    "content_text": None,                # Version texte nettoyee
    "excerpt": None,                     # header_content_display

    # --- TAXONOMIES ---
    "categories": None,                  # Pas de categories pour les produits
    "tags": None,                        # Pas de tags pour les produits
    "custom_taxonomies": None,           # occupation, etc.

    # --- DATES ---
    "date_published": "date",
    "date_modified": "modified",
    "date_imported": None,               # datetime.now()

    # --- AUTEUR & STATUT ---
    "status": "status",
    "author_id": "author",

    # --- MEDIA ---
    "featured_image_url": "yoast_head_json.og_image.0.url",

    # --- LANGUE ---
    "language": "lang",

    # --- DONNEES BRUTES ---
    "raw_json": None,                    # JSON complet (inclut tous les champs ACF)
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fonction de transformation des produits

# COMMAND ----------

def _build_product_content_raw(header_content, acf_main):
    """
    Assemble le contenu HTML brut d'un produit a partir des champs ACF.
    Combine le header et les blocs principaux (arguments, features).
    """
    parts = []

    if header_content:
        parts.append(header_content)

    # Ajoute les arguments produit
    for block in acf_main:
        if block.get('acf_fc_layout') == 'product-arguments':
            for arg in block.get('arguments', []):
                content = arg.get('content', '')
                if content:
                    parts.append(content)

    # Ajoute les features produit
    for block in acf_main:
        if block.get('acf_fc_layout') == 'product-features':
            for section in block.get('sections', []):
                content = section.get('content', '')
                if content:
                    parts.append(content)

    return "\n".join(parts) if parts else None


def transform_product_item(item: Dict, site_id: str, site_config: Dict) -> Dict:
    """
    Transforme un item produit WordPress avec ACF en format standardise pour cegid_website.

    Schema cible: gdp_cdt_dev_04_gld.sandbox_mkt.cegid_website
    Les champs ACF specifiques sont assembles dans content_raw/content_text
    et les donnees brutes completes sont conservees dans raw_json.
    """
    wp_id = item.get('id')
    content_type = "product"

    # === LANGUE ===
    language = item.get('lang') or site_config.get("language", "fr")

    # === ACF: Extraction du header flexible ===
    acf_header = get_nested_value(item, 'acf.vah_flexible_header', [])
    header_block = acf_header[0] if acf_header else {}
    header_data = header_block.get('header', {}) if header_block else {}

    header_content = header_data.get('content', '')
    header_content_display = header_data.get('content_display', '')

    # === ACF: Extraction du main flexible ===
    acf_main = get_nested_value(item, 'acf.vah_flexible_main', [])

    # === CONTENU: Assemble depuis les champs ACF ===
    content_raw = _build_product_content_raw(header_content, acf_main)
    content_text = clean_html_content(content_raw) if content_raw else None
    excerpt = clean_html_content(header_content_display) if header_content_display else None

    # === SEO: Yoast ===
    og_image = get_nested_value(item, 'yoast_head_json.og_image.0.url')

    # === SEO: noindex ===
    robots_index = get_nested_value(item, 'yoast_head_json.robots.index')
    noindex = robots_index == 'noindex' if robots_index else None

    # === TAXONOMIES CUSTOM ===
    custom_taxonomies = {}
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
        "excerpt": excerpt,

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
        "featured_image_url": og_image,

        # --- LANGUE ---
        "language": language,

        # --- DONNEES BRUTES ---
        "raw_json": json.dumps(item, ensure_ascii=False),

        # --- AI FORMATTING ---
        "date_formatted": None,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fonction d'upsert des produits

# COMMAND ----------

def upsert_products(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Upsert (MERGE) des produits dans la table cegid_website.
    Met a jour les produits existants, insere les nouveaux.
    Memes champs que upsert_content dans blog_importer.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    # Cree une vue temporaire
    df.createOrReplaceTempView("new_products")

    # MERGE pour upsert (meme logique que blog_importer)
    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING new_products AS source
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

    print(f"Upsert produits termine dans {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pipeline principal d'import des produits

# COMMAND ----------

def run_product_import_pipeline(sites_to_import: List[str] = WP_SITES_TO_IMPORT,
                                 incremental: bool = True):
    """
    Execute le pipeline d'import des produits pour un ou plusieurs sites.

    Route API: /wp-json/wp/v2/product
    Table cible: gdp_cdt_dev_04_gld.sandbox_mkt.cegid_website (content_type = "product")

    Args:
        sites_to_import: Liste des site_id a importer (ex: ["fr", "es"])
        incremental: Si True, importe seulement les produits nouveaux ou modifies
    """
    catalog = PRODUCT_TABLE_CONFIG["catalog"]
    schema = PRODUCT_TABLE_CONFIG["schema"]
    table_name = PRODUCT_TABLE_CONFIG["table_name"]

    # Cree la table cegid_website si necessaire (meme schema que blog_importer)
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
        print(f"[{site_label}] Import: Produits (product)")
        print(f"{'='*50}")

        # Recupere la derniere date de modification pour import incremental
        modified_after = None
        if incremental:
            modified_after = get_last_modified_date(catalog, schema, table_name, site_id, "product")
            if modified_after:
                print(f"Mode incremental - contenus modifies apres: {modified_after}")

        # Recupere les produits WordPress via /wp-json/wp/v2/product
        items = connector.fetch_all_content(
            content_type="product",
            endpoint=PRODUCT_ENDPOINT,
            modified_after=modified_after
        )

        if not items:
            print(f"Aucun nouveau produit a importer pour {site_label}")
            continue

        # Transforme les items avec la fonction specifique aux produits
        transformed_items = [
            transform_product_item(item, site_id, site_config)
            for item in items
        ]

        # Cree le DataFrame avec le schema cegid_website
        df = spark.createDataFrame(transformed_items, CONTENT_SCHEMA)

        # Upsert dans la table produits
        upsert_products(df, catalog, schema, table_name)

        total_imported += len(transformed_items)
        print(f"[{site_label}] {len(transformed_items)} produit(s) importe(s)")

    print(f"\n{'#'*60}")
    print(f"Import produits termine! Total: {total_imported} produits importes")
    print(f"   Sites traites: {', '.join(sites_to_import)}")
    print(f"{'#'*60}")

    return total_imported

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Execution

# COMMAND ----------

run_product_import_pipeline(sites_to_import=["fr"], incremental=True)

# =============================================================================
# EXEMPLES D'EXECUTION
# =============================================================================

# Import des produits d'un seul site (FR) - full
# run_product_import_pipeline(sites_to_import=["fr"], incremental=False)

# Import des produits de plusieurs sites
# run_product_import_pipeline(sites_to_import=["fr", "es", "uk"], incremental=False)

# Import des produits de TOUS les sites configures
# run_product_import_pipeline(sites_to_import=list(WP_SITES.keys()), incremental=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Verification des donnees

# COMMAND ----------

# Affiche un apercu des produits importes par site
product_table = f"{PRODUCT_TABLE_CONFIG['catalog']}.{PRODUCT_TABLE_CONFIG['schema']}.{PRODUCT_TABLE_CONFIG['table_name']}"

display(spark.sql(f"""
    SELECT
        site_id,
        language,
        COUNT(*) as nb_products,
        MIN(date_published) as oldest,
        MAX(date_published) as newest,
        MAX(date_imported) as last_import
    FROM {product_table}
    WHERE content_type = 'product'
    GROUP BY site_id, language
    ORDER BY site_id
"""))

# COMMAND ----------

# Apercu des produits avec leurs donnees
display(spark.sql(f"""
    SELECT
        site_id,
        wp_id,
        title,
        url,
        meta_title,
        LEFT(content_text, 200) as content_preview,
        date_published
    FROM {product_table}
    WHERE content_type = 'product'
    ORDER BY site_id, date_published DESC
    LIMIT 20
"""))

# COMMAND ----------

# Repartition des contenus par type dans cegid_website
display(spark.sql(f"""
    SELECT
        content_type,
        site_id,
        COUNT(*) as nb_items,
        MAX(date_imported) as last_import
    FROM {product_table}
    GROUP BY content_type, site_id
    ORDER BY content_type, site_id
"""))
