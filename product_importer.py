# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import des Produits WordPress (ACF)
# MAGIC
# MAGIC Ce notebook permet de :
# MAGIC - Recuperer les pages produit via l'API WordPress REST `/wp-json/wp/v2/product`
# MAGIC - Extraire les champs ACF flexibles (header, arguments, features, resources)
# MAGIC - Stocker les produits dans la table `cegid_products`
# MAGIC - Supporter l'import incremental via le tracking des IDs

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import des utilitaires communs

# COMMAND ----------

# MAGIC %run ./wordpress_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration specifique aux produits

# COMMAND ----------

# Configuration Databricks pour les produits
PRODUCT_TABLE_CONFIG = {
    "catalog": DATABRICKS_CATALOG,
    "schema": DATABRICKS_SCHEMA,
    "table_name": "cegid_products"
}

# Endpoint API pour les produits
PRODUCT_ENDPOINT = "/product"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schema des produits

# COMMAND ----------

# Schema pour les arguments produit (pictogrammes + stats)
PRODUCT_ARGUMENT_SCHEMA = StructType([
    StructField("pictogram_icon", StringType(), True),
    StructField("content", StringType(), True),
])

# Schema pour les sections de fonctionnalites produit
PRODUCT_FEATURE_SECTION_SCHEMA = StructType([
    StructField("alignement", StringType(), True),
    StructField("media_type", StringType(), True),
    StructField("image", IntegerType(), True),
    StructField("content", StringType(), True),
])

# Schema principal pour les produits
PRODUCT_SCHEMA = StructType([
    # --- IDENTIFIANTS ---
    StructField("id", LongType(), False),
    StructField("wp_id", IntegerType(), False),
    StructField("content_type", StringType(), False),
    StructField("site_id", StringType(), False),

    # --- METADONNEES ---
    StructField("slug", StringType(), True),
    StructField("url", StringType(), True),
    StructField("title", StringType(), True),
    StructField("template", StringType(), True),

    # --- SEO (Yoast) ---
    StructField("meta_description", StringType(), True),
    StructField("meta_title", StringType(), True),
    StructField("og_image_url", StringType(), True),
    StructField("schema_json", StringType(), True),  # JSON-LD schema

    # --- TAXONOMIES ---
    StructField("occupation", ArrayType(IntegerType()), True),

    # --- ACF: HEADER FLEXIBLE ---
    StructField("header_content", StringType(), True),
    StructField("header_content_display", StringType(), True),
    StructField("header_image_desktop", IntegerType(), True),
    StructField("header_form_config", StringType(), True),  # JSON stringifie

    # --- ACF: PRODUCT ARGUMENTS ---
    StructField("product_arguments", ArrayType(PRODUCT_ARGUMENT_SCHEMA), True),

    # --- ACF: PRODUCT FEATURES ---
    StructField("product_features", ArrayType(PRODUCT_FEATURE_SECTION_SCHEMA), True),

    # --- ACF: PRODUCT RESOURCES ---
    StructField("resources_list", ArrayType(IntegerType()), True),

    # --- ACF: RELATED PRODUCTS ---
    StructField("related_products", ArrayType(IntegerType()), True),

    # --- DATES ---
    StructField("date_published", TimestampType(), True),
    StructField("date_modified", TimestampType(), True),
    StructField("date_imported", TimestampType(), False),

    # --- LANGUE ---
    StructField("language", StringType(), True),

    # --- DONNEES BRUTES ---
    StructField("raw_json", StringType(), True),
    StructField("raw_acf_header", StringType(), True),
    StructField("raw_acf_main", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Mapping des champs produit

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
    "template": "template",

    # --- SEO (Yoast) ---
    "meta_description": "yoast_head_json.description",
    "meta_title": "yoast_head_json.title",
    "og_image_url": "yoast_head_json.og_image.0.url",
    "schema_json": "yoast_head_json.schema",

    # --- TAXONOMIES ---
    "occupation": "occupation",

    # --- ACF: HEADER FLEXIBLE ---
    "header_content": "acf.vah_flexible_header.0.header.content",
    "header_content_display": "acf.vah_flexible_header.0.header.content_display",
    "header_image_desktop": "acf.vah_flexible_header.0.header.image_destktop",
    "header_form_config": "acf.vah_flexible_header.0.header.form",

    # --- ACF: PRODUCT ARGUMENTS ---
    "product_arguments": "acf.vah_flexible_main",  # Extraction custom (layout: product-arguments)

    # --- ACF: PRODUCT FEATURES ---
    "product_features": "acf.vah_flexible_main",   # Extraction custom (layout: product-features)

    # --- ACF: PRODUCT RESOURCES ---
    "resources_list": "acf.vah_flexible_main",     # Extraction custom (layout: product-resources)

    # --- ACF: RELATED PRODUCTS ---
    "related_products": "acf.vah_flexible_main",   # Extraction custom (layout: product-others)

    # --- DATES ---
    "date_published": "date",
    "date_modified": "modified",
    "date_imported": None,               # datetime.now()

    # --- LANGUE ---
    "language": "lang",

    # --- DONNEES BRUTES ---
    "raw_json": None,
    "raw_acf_header": "acf.vah_flexible_header",
    "raw_acf_main": "acf.vah_flexible_main",
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fonction de transformation des produits

# COMMAND ----------

def transform_product_item(item: Dict, site_id: str, site_config: Dict) -> Dict:
    """
    Transforme un item produit WordPress avec ACF en format standardise pour Databricks.

    Schema cible: gdp_cdt_dev_04_gld.sandbox_mkt.cegid_products
    Gere les champs ACF specifiques (vah_flexible_header, vah_flexible_main)
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
    header_image_desktop = header_data.get('image_destktop')  # Typo dans l'API
    header_form_config = json.dumps(header_data.get('form', {}), ensure_ascii=False) if header_data.get('form') else None

    # === ACF: Extraction du main flexible ===
    acf_main = get_nested_value(item, 'acf.vah_flexible_main', [])

    # --- Product Arguments (layout: product-arguments) ---
    product_arguments = []
    for block in acf_main:
        if block.get('acf_fc_layout') == 'product-arguments':
            arguments = block.get('arguments', [])
            for arg in arguments:
                product_arguments.append({
                    "pictogram_icon": arg.get('pictogram_icon', ''),
                    "content": arg.get('content', '')
                })

    # --- Product Features (layout: product-features) ---
    product_features = []
    for block in acf_main:
        if block.get('acf_fc_layout') == 'product-features':
            sections = block.get('sections', [])
            for section in sections:
                product_features.append({
                    "alignement": section.get('alignement', ''),
                    "media_type": section.get('media_type', ''),
                    "image": section.get('image'),
                    "content": section.get('content', '')
                })

    # --- Product Resources (layout: product-resources) ---
    resources_list = []
    for block in acf_main:
        if block.get('acf_fc_layout') == 'product-resources':
            resources_list = block.get('resources_list', [])
            break

    # --- Related Products (layout: product-others) ---
    related_products = []
    for block in acf_main:
        if block.get('acf_fc_layout') == 'product-others':
            related_products = block.get('products_section', [])
            break

    # === SEO: Yoast ===
    og_image = get_nested_value(item, 'yoast_head_json.og_image.0.url')
    schema_data = get_nested_value(item, 'yoast_head_json.schema')
    schema_json = json.dumps(schema_data, ensure_ascii=False) if schema_data else None

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
        "template": item.get('template', ''),

        # --- SEO (Yoast) ---
        "meta_description": get_nested_value(item, 'yoast_head_json.description'),
        "meta_title": get_nested_value(item, 'yoast_head_json.title'),
        "og_image_url": og_image,
        "schema_json": schema_json,

        # --- TAXONOMIES ---
        "occupation": item.get('occupation', []),

        # --- ACF: HEADER FLEXIBLE ---
        "header_content": header_content,
        "header_content_display": header_content_display,
        "header_image_desktop": header_image_desktop,
        "header_form_config": header_form_config,

        # --- ACF: PRODUCT ARGUMENTS ---
        "product_arguments": product_arguments if product_arguments else None,

        # --- ACF: PRODUCT FEATURES ---
        "product_features": product_features if product_features else None,

        # --- ACF: PRODUCT RESOURCES ---
        "resources_list": resources_list if resources_list else None,

        # --- ACF: RELATED PRODUCTS ---
        "related_products": related_products if related_products else None,

        # --- DATES ---
        "date_published": parse_wp_date(item.get('date')),
        "date_modified": parse_wp_date(item.get('modified')),
        "date_imported": datetime.now(),

        # --- LANGUE ---
        "language": language,

        # --- DONNEES BRUTES ---
        "raw_json": json.dumps(item, ensure_ascii=False),
        "raw_acf_header": json.dumps(acf_header, ensure_ascii=False) if acf_header else None,
        "raw_acf_main": json.dumps(acf_main, ensure_ascii=False) if acf_main else None,
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fonction d'upsert des produits

# COMMAND ----------

def upsert_products(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Upsert (MERGE) des produits dans la table Delta.
    Met a jour les produits existants, insere les nouveaux.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    # Cree une vue temporaire
    df.createOrReplaceTempView("new_products")

    # MERGE pour upsert avec tous les champs produit
    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING new_products AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                title = source.title,
                slug = source.slug,
                url = source.url,
                template = source.template,
                meta_description = source.meta_description,
                meta_title = source.meta_title,
                og_image_url = source.og_image_url,
                schema_json = source.schema_json,
                occupation = source.occupation,
                header_content = source.header_content,
                header_content_display = source.header_content_display,
                header_image_desktop = source.header_image_desktop,
                header_form_config = source.header_form_config,
                product_arguments = source.product_arguments,
                product_features = source.product_features,
                resources_list = source.resources_list,
                related_products = source.related_products,
                date_modified = source.date_modified,
                date_imported = source.date_imported,
                raw_json = source.raw_json,
                raw_acf_header = source.raw_acf_header,
                raw_acf_main = source.raw_acf_main
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
    Table cible: gdp_cdt_dev_04_gld.sandbox_mkt.cegid_products

    Args:
        sites_to_import: Liste des site_id a importer (ex: ["fr", "es"])
        incremental: Si True, importe seulement les nouveaux produits
    """
    catalog = PRODUCT_TABLE_CONFIG["catalog"]
    schema = PRODUCT_TABLE_CONFIG["schema"]
    table_name = PRODUCT_TABLE_CONFIG["table_name"]

    # Cree la table produits si necessaire
    create_delta_table(
        catalog=catalog,
        schema=schema,
        table_name=table_name,
        spark_schema=PRODUCT_SCHEMA,
        partition_by=["site_id"]
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

        # Recupere le dernier ID pour import incremental
        since_id = None
        if incremental:
            since_id = get_last_imported_id(catalog, schema, table_name, site_id)
            if since_id:
                print(f"Mode incremental - depuis ID: {since_id}")

        # Recupere les produits WordPress via /wp-json/wp/v2/product
        items = connector.fetch_all_content(
            content_type="product",
            endpoint=PRODUCT_ENDPOINT,
            since_id=since_id
        )

        if not items:
            print(f"Aucun nouveau produit a importer pour {site_label}")
            continue

        # Transforme les items avec la fonction specifique aux produits
        transformed_items = [
            transform_product_item(item, site_id, site_config)
            for item in items
        ]

        # Cree le DataFrame avec le schema produit
        df = spark.createDataFrame(transformed_items, PRODUCT_SCHEMA)

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

# =============================================================================
# EXEMPLES D'EXECUTION
# =============================================================================

# Import des produits d'un seul site (FR)
# run_product_import_pipeline(sites_to_import=["fr"], incremental=False)

# Import incremental des produits d'un seul site
# run_product_import_pipeline(sites_to_import=["fr"], incremental=True)

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
    GROUP BY site_id, language
    ORDER BY site_id
"""))

# COMMAND ----------

# Apercu des produits avec leurs donnees ACF
display(spark.sql(f"""
    SELECT
        site_id,
        wp_id,
        title,
        url,
        template,
        meta_title,
        SIZE(occupation) as nb_occupations,
        SIZE(product_arguments) as nb_arguments,
        SIZE(product_features) as nb_features,
        SIZE(resources_list) as nb_resources,
        SIZE(related_products) as nb_related,
        date_published
    FROM {product_table}
    ORDER BY site_id, date_published DESC
    LIMIT 20
"""))

# COMMAND ----------

# Detail des arguments produit
display(spark.sql(f"""
    SELECT
        wp_id,
        title,
        EXPLODE(product_arguments) as argument
    FROM {product_table}
    WHERE product_arguments IS NOT NULL
    LIMIT 10
"""))

# COMMAND ----------

# Detail des features produit
display(spark.sql(f"""
    SELECT
        wp_id,
        title,
        EXPLODE(product_features) as feature
    FROM {product_table}
    WHERE product_features IS NOT NULL
    LIMIT 10
"""))
