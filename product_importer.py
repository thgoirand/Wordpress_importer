# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import des Produits WordPress (Bronze + Silver)
# MAGIC
# MAGIC Ce notebook permet de :
# MAGIC - Recuperer les pages produit via l'API WordPress REST `/wp-json/wp/v2/product`
# MAGIC - Stocker les donnees brutes dans la table **bronze** `bronze_product`
# MAGIC - Transformer et stocker les contenus standardises dans la table **silver** `cegid_website_pages`
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

# Configuration du type de contenu
CONTENT_TYPE = "product"
CONTENT_ENDPOINT = "/product"

# Noms de tables (architecture medallion)
BRONZE_TABLE_NAME = BRONZE_TABLES[CONTENT_TYPE]  # bronze_product
SILVER_TABLE_NAME = SILVER_TABLE                   # cegid_website_pages

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fonctions de transformation des produits (bronze -> silver)

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
    Transforme un item produit WordPress avec ACF en format standardise pour la table silver.
    """
    wp_id = item.get('id')
    content_type = CONTENT_TYPE

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
    }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Pipeline principal d'import (Bronze + Silver)

# COMMAND ----------

def run_product_import_pipeline(sites_to_import: List[str] = WP_SITES_TO_IMPORT,
                                 incremental: bool = True):
    """
    Execute le pipeline d'import des produits pour un ou plusieurs sites.
    Ecrit d'abord dans la table bronze, puis dans la table silver.
    """
    catalog = DATABRICKS_CATALOG
    schema = DATABRICKS_SCHEMA

    # Cree les tables bronze et silver si necessaire
    create_delta_table(
        catalog=catalog, schema=schema,
        table_name=BRONZE_TABLE_NAME,
        spark_schema=BRONZE_SCHEMA,
        partition_by=["site_id"]
    )
    create_delta_table(
        catalog=catalog, schema=schema,
        table_name=SILVER_TABLE_NAME,
        spark_schema=SILVER_SCHEMA,
        partition_by=["site_id", "content_type"]
    )

    total_imported = 0

    for site_id in sites_to_import:
        if site_id not in WP_SITES:
            print(f"Site '{site_id}' non configure, ignore")
            continue

        site_config = WP_SITES[site_id]
        site_label = site_config.get("label", site_id)

        print(f"\n{'#'*60}")
        print(f"SITE: {site_label} ({site_id})")
        print(f"{'#'*60}")

        connector = WordPressConnector(site_id, site_config)

        # Recupere la derniere date de modification pour import incremental (depuis bronze)
        modified_after = None
        if incremental:
            modified_after = get_last_modified_date(catalog, schema, BRONZE_TABLE_NAME, site_id, CONTENT_TYPE)
            if modified_after:
                print(f"Mode incremental - contenus modifies apres: {modified_after}")

        # Recupere les produits WordPress
        items = connector.fetch_all_content(
            content_type=CONTENT_TYPE,
            endpoint=CONTENT_ENDPOINT,
            modified_after=modified_after
        )

        if not items:
            print(f"Aucun nouveau produit a importer pour {site_label}")
            continue

        # --- BRONZE : donnees brutes ---
        print(f"\n[{site_label}] Ecriture bronze ({BRONZE_TABLE_NAME})...")
        bronze_items = [
            {
                "id": calculate_composite_id(item.get('id'), CONTENT_TYPE, site_id),
                "wp_id": item.get('id'),
                "content_type": CONTENT_TYPE,
                "site_id": site_id,
                "raw_json": json.dumps(item, ensure_ascii=False),
                "date_modified": parse_wp_date(item.get('modified')),
                "date_imported": datetime.now(),
            }
            for item in items
        ]
        df_bronze = spark.createDataFrame(bronze_items, BRONZE_SCHEMA)
        upsert_bronze(df_bronze, catalog, schema, BRONZE_TABLE_NAME)

        # --- SILVER : donnees standardisees ---
        print(f"[{site_label}] Ecriture silver ({SILVER_TABLE_NAME})...")
        transformed_items = [
            transform_product_item(item, site_id, site_config)
            for item in items
        ]
        df_silver = spark.createDataFrame(transformed_items, SILVER_SCHEMA)
        upsert_silver(df_silver, catalog, schema, SILVER_TABLE_NAME)

        total_imported += len(transformed_items)
        print(f"[{site_label}] {len(transformed_items)} produit(s) importe(s) (bronze + silver)")

    print(f"\n{'#'*60}")
    print(f"Import produits termine! Total: {total_imported} produits importes")
    print(f"   Sites traites: {', '.join(sites_to_import)}")
    print(f"{'#'*60}")

    return total_imported

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execution

# COMMAND ----------

# Import incremental des produits
run_product_import_pipeline(sites_to_import=["fr"], incremental=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Verification des donnees

# COMMAND ----------

# Verification bronze
bronze_table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{BRONZE_TABLE_NAME}"
display(spark.sql(f"""
    SELECT
        site_id,
        COUNT(*) as nb_items,
        MAX(date_imported) as last_import
    FROM {bronze_table}
    GROUP BY site_id
    ORDER BY site_id
"""))

# COMMAND ----------

# Verification silver
silver_table = f"{DATABRICKS_CATALOG}.{DATABRICKS_SCHEMA}.{SILVER_TABLE_NAME}"
display(spark.sql(f"""
    SELECT
        site_id,
        language,
        COUNT(*) as nb_products,
        MIN(date_published) as oldest,
        MAX(date_published) as newest,
        MAX(date_imported) as last_import
    FROM {silver_table}
    WHERE content_type = '{CONTENT_TYPE}'
    GROUP BY site_id, language
    ORDER BY site_id
"""))

# COMMAND ----------

# Repartition des contenus par type dans silver
display(spark.sql(f"""
    SELECT
        content_type,
        site_id,
        COUNT(*) as nb_items,
        MAX(date_imported) as last_import
    FROM {silver_table}
    GROUP BY content_type, site_id
    ORDER BY content_type, site_id
"""))
