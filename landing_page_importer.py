# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Import des Landing Pages WordPress (Bronze + Silver)
# MAGIC
# MAGIC Ce notebook permet de :
# MAGIC - Recuperer les landing pages via l'API WordPress REST `/wp-json/wp/v2/landing-page`
# MAGIC - Stocker les donnees brutes dans la table **SLV** `cegid_website_landing_page_slv`
# MAGIC - Transformer et stocker les contenus standardises dans la table **PLT** `cegid_website_plt`
# MAGIC - Supporter l'incremental via le tracking des dates de modification

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Import des utilitaires communs

# COMMAND ----------

# MAGIC %run ./wordpress_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration specifique aux landing pages

# COMMAND ----------

# Configuration du type de contenu
CONTENT_TYPE = "landing_page"
CONTENT_ENDPOINT = "/landing-page"

# Noms de tables (architecture medallion)
BRONZE_TABLE_NAME = BRONZE_TABLES[CONTENT_TYPE]  # cegid_website_landing_page_slv
SILVER_TABLE_NAME = SILVER_TABLE                   # cegid_website_plt

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Fonctions de transformation des landing pages (bronze -> silver)

# COMMAND ----------

def _build_landing_page_content_raw(item: Dict) -> str:
    """
    Assemble le contenu HTML brut d'une landing page a partir des champs ACF.
    Les landing pages stockent leur contenu dans acf.vah_flexible_main
    (et non dans content.rendered qui est vide).

    Blocs supportes:
    - builder-grid > flexible_grid > grid-content : contenu principal
    - arguments-content > arguments : blocs argumentaires
    """
    parts = []

    # Header flexible (si present)
    acf_header = get_nested_value(item, 'acf.vah_flexible_header', [])
    if acf_header:
        for block in acf_header:
            header_data = block.get('header', {}) or {}
            content = header_data.get('content', '')
            if content:
                parts.append(content)

    # Main flexible
    acf_main = get_nested_value(item, 'acf.vah_flexible_main', [])
    if not acf_main:
        return ""

    for block in acf_main:
        layout = block.get('acf_fc_layout', '')

        # builder-grid : parcourt les sous-blocs de la grille
        if layout == 'builder-grid':
            for grid_item in (block.get('flexible_grid') or []):
                if grid_item.get('acf_fc_layout') == 'grid-content':
                    content = grid_item.get('content', '')
                    if content:
                        parts.append(content)

        # arguments-content : parcourt les arguments
        elif layout == 'arguments-content':
            intro = block.get('intro', '')
            if intro:
                parts.append(intro)
            for arg in (block.get('arguments') or []):
                content = arg.get('content', '')
                if content:
                    parts.append(content)

    return "\n".join(parts) if parts else ""


def transform_landing_page_item(item: Dict, site_id: str, site_config: Dict) -> Dict:
    """
    Transforme une landing page WordPress en format standardise pour la table silver.
    """
    wp_id = item.get('id')
    content_type = CONTENT_TYPE

    # === CONTENU ===
    content_raw = _build_landing_page_content_raw(item)
    if not content_raw:
        content_raw = get_nested_value(item, 'content.rendered', '')
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
        "meta_keyword": None,
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
# MAGIC ## 4. Pipeline principal d'import (Bronze + Silver)

# COMMAND ----------

def run_landing_page_import_pipeline(sites_to_import: List[str] = WP_SITES_TO_IMPORT,
                                      incremental: bool = True):
    """
    Execute le pipeline d'import des landing pages pour un ou plusieurs sites.
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

        # Recupere la derniere date de modification pour import incremental
        modified_after = None
        if incremental:
            modified_after = get_last_modified_date(catalog, schema, BRONZE_TABLE_NAME, site_id, CONTENT_TYPE)
            if modified_after:
                print(f"Mode incremental - contenus modifies apres: {modified_after}")

        # Recupere les landing pages WordPress
        items = connector.fetch_all_content(
            content_type=CONTENT_TYPE,
            endpoint=CONTENT_ENDPOINT,
            modified_after=modified_after
        )

        if not items:
            print(f"Aucune nouvelle landing page a importer pour {site_label}")
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
            transform_landing_page_item(item, site_id, site_config)
            for item in items
        ]
        df_silver = spark.createDataFrame(transformed_items, SILVER_SCHEMA)
        upsert_silver(df_silver, catalog, schema, SILVER_TABLE_NAME)

        total_imported += len(transformed_items)
        print(f"[{site_label}] {len(transformed_items)} landing page(s) importee(s) (bronze + silver)")

    print(f"\n{'#'*60}")
    print(f"Import landing pages termine! Total: {total_imported} landing pages importees")
    print(f"   Sites traites: {', '.join(sites_to_import)}")
    print(f"{'#'*60}")

    return total_imported

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execution

# COMMAND ----------

# Import incremental des landing pages
run_landing_page_import_pipeline(sites_to_import=["fr"], incremental=True)

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
        content_type,
        language,
        COUNT(*) as nb_items,
        MIN(date_published) as oldest,
        MAX(date_published) as newest,
        MAX(date_imported) as last_import
    FROM {silver_table}
    WHERE content_type = '{CONTENT_TYPE}'
    GROUP BY site_id, content_type, language
    ORDER BY site_id
"""))

# COMMAND ----------

# Apercu des dernieres landing pages (silver)
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
    FROM {silver_table}
    WHERE content_type = '{CONTENT_TYPE}'
    ORDER BY site_id, date_published DESC
    LIMIT 20
"""))
