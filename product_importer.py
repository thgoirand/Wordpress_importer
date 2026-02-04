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
# MAGIC ## 1. Configuration

# COMMAND ----------

# Configuration WordPress
# Credentials (utiliser Databricks Secrets en production)
WP_BASE_URL = "https://www.cegid.com"  # Domaine racine
WP_LOGIN = "semji"
WP_PASSWORD = " 2VUV BySM SSrp wzJW ZFul nLaf"  # Set your password here

# Configuration des sites par langue
WP_SITES = {
    "fr": {
        "prefix": "fr",
        "language": "fr",
        "label": "France"
    },
    "es": {
        "prefix": "es",
        "language": "es",
        "label": "Espana"
    },
    "uk": {
        "prefix": "uk",
        "language": "en-GB",
        "label": "United Kingdom"
    },
    "us": {
        "prefix": "us",
        "language": "en-US",
        "label": "United States"
    },
    "de": {
        "prefix": "de",
        "language": "de",
        "label": "Deutschland"
    },
    "it": {
        "prefix": "it",
        "language": "it",
        "label": "Italia"
    },
    "pt": {
        "prefix": "pt",
        "language": "pt",
        "label": "Portugal"
    },
}

# Site(s) a importer (liste ou "all" pour tous)
WP_SITES_TO_IMPORT = ["fr"]  # Ex: ["fr", "es"] ou list(WP_SITES.keys()) pour tous

# Configuration technique WordPress
WORDPRESS_CONFIG = {
    "base_url": WP_BASE_URL,
    "api_endpoint": "/wp-json/wp/v2",
    "per_page": 100,  # Maximum autorise par l'API WordPress
    "timeout": 30,
    "auth": (WP_LOGIN, WP_PASSWORD)  # Basic Auth ou Application Password
}

# Configuration Databricks pour les produits
PRODUCT_TABLE_CONFIG = {
    "catalog": "gdp_cdt_dev_04_gld",
    "schema": "sandbox_mkt",
    "table_name": "cegid_products"
}

# Endpoint API pour les produits
PRODUCT_ENDPOINT = "/product"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports

# COMMAND ----------

import requests
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, ArrayType, MapType, LongType, BooleanType
)
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, max as spark_max,
    from_json, explode, regexp_replace, trim
)
from bs4 import BeautifulSoup
import html

# Initialisation Spark (deja disponible dans Databricks)
spark = SparkSession.builder.getOrCreate()

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
# MAGIC ## 4. Fonctions utilitaires

# COMMAND ----------

def clean_html_content(html_content: str) -> str:
    """
    Nettoie le contenu HTML pour extraire le texte brut.
    Utile pour le RAG et l'indexation.
    """
    if not html_content:
        return ""

    # Decode les entites HTML
    decoded = html.unescape(html_content)

    # Parse avec BeautifulSoup
    soup = BeautifulSoup(decoded, 'html.parser')

    # Supprime les scripts et styles
    for element in soup(['script', 'style', 'nav', 'footer', 'header']):
        element.decompose()

    # Extrait le texte
    text = soup.get_text(separator=' ', strip=True)

    # Nettoie les espaces multiples
    text = ' '.join(text.split())

    return text


def get_nested_value(data: dict, path: str, default=None):
    """
    Extrait une valeur imbriquee depuis un dictionnaire via une notation pointee.

    Exemples:
        get_nested_value(item, "title.rendered")
        -> item["title"]["rendered"]

        get_nested_value(item, "_embedded.author.0.name")
        -> item["_embedded"]["author"][0]["name"]

    Args:
        data: Dictionnaire source
        path: Chemin en notation pointee (ex: "a.b.0.c")
        default: Valeur par defaut si le chemin n'existe pas

    Returns:
        La valeur trouvee ou default
    """
    if not path or not data:
        return default

    keys = path.split('.')
    value = data

    try:
        for key in keys:
            if isinstance(value, dict):
                value = value.get(key)
            elif isinstance(value, list):
                # Support des index numeriques (ex: "author.0.name")
                index = int(key)
                value = value[index] if len(value) > index else None
            else:
                return default

            if value is None:
                return default
        return value
    except (KeyError, IndexError, TypeError, ValueError):
        return default


def parse_wp_date(date_str: str) -> Optional[datetime]:
    """
    Parse une date ISO WordPress en objet datetime.
    Formats supportes:
    - 2024-01-15T10:30:00
    - 2024-01-15T10:30:00Z
    - 2024-01-15
    """
    if not date_str:
        return None

    try:
        # Format avec T et possible Z
        if 'T' in date_str:
            # Supprime le Z final si present
            clean_date = date_str.replace('Z', '')
            return datetime.fromisoformat(clean_date)
        else:
            # Format date seule
            return datetime.strptime(date_str, '%Y-%m-%d')
    except (ValueError, TypeError):
        return None


def calculate_composite_id(wp_id: int, content_type: str, site_id: str) -> int:
    """
    Calcule un ID composite unique base sur le site, le type de contenu et l'ID WordPress.
    Permet d'eviter les collisions entre sites et types de contenus.

    Structure: SITE_OFFSET + TYPE_OFFSET + wp_id
    """
    SITE_OFFSETS = {
        "fr": 1_000_000_000,
        "es": 2_000_000_000,
        "uk": 3_000_000_000,
        "us": 4_000_000_000,
        "de": 5_000_000_000,
        "it": 6_000_000_000,
        "pt": 7_000_000_000,
        "root": 0,
    }

    TYPE_OFFSETS = {
        "post": 0,
        "page": 10_000_000,
        "landing_page": 20_000_000,
        "product": 30_000_000,
        "custom_post_1": 40_000_000,
        "custom_post_2": 50_000_000,
    }

    site_offset = SITE_OFFSETS.get(site_id, 9_000_000_000)
    type_offset = TYPE_OFFSETS.get(content_type, 100_000_000)

    return site_offset + type_offset + wp_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Classe WordPressConnector

# COMMAND ----------

class WordPressConnector:
    """
    Connecteur WordPress multi-sites vers Databricks.
    Gere la recuperation paginee avec authentification.
    """

    def __init__(self, site_id: str, site_config: Dict, config: Dict = WORDPRESS_CONFIG):
        self.site_id = site_id
        self.site_config = site_config
        self.base_url = config["base_url"].rstrip('/')
        self.api_endpoint = config["api_endpoint"]
        self.per_page = config["per_page"]
        self.timeout = config["timeout"]
        self.auth = config.get("auth")
        self.session = requests.Session()

        # Configure l'authentification
        if self.auth:
            self.session.auth = self.auth

    def _get_site_url(self) -> str:
        """Construit l'URL du site avec le prefixe de langue."""
        prefix = self.site_config.get("prefix", "")
        if prefix:
            return f"{self.base_url}/{prefix}"
        return self.base_url

    def _get_api_url(self, endpoint: str) -> str:
        """Construit l'URL complete de l'API pour ce site."""
        return f"{self._get_site_url()}{self.api_endpoint}{endpoint}"

    def _fetch_page(self, endpoint: str, page: int, params: Dict = None) -> Tuple[List[Dict], int]:
        """
        Recupere une page de resultats de l'API WordPress.
        Retourne les items et le nombre total de pages.
        """
        url = self._get_api_url(endpoint)

        request_params = {
            "page": page,
            "per_page": self.per_page,
            "_embed": 1,
            "status": "publish"
        }

        if params:
            request_params.update(params)

        try:
            response = self.session.get(
                url,
                params=request_params,
                timeout=self.timeout
            )
            response.raise_for_status()

            total_pages = int(response.headers.get('X-WP-TotalPages', 1))
            total_items = int(response.headers.get('X-WP-Total', 0))
            items = response.json()

            return items, total_pages

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                return [], 0
            print(f"Erreur HTTP {e.response.status_code}: {e}")
            return [], 0
        except requests.exceptions.RequestException as e:
            print(f"Erreur API WordPress: {e}")
            return [], 0

    def fetch_all_content(self, content_type: str, endpoint: str,
                          since_id: Optional[int] = None) -> List[Dict]:
        """
        Recupere tous les contenus d'un type donne pour ce site.
        Support optionnel de l'import incremental via since_id.
        """
        all_items = []
        page = 1
        total_pages = 1

        params = {}
        if since_id:
            params["after"] = since_id

        site_label = self.site_config.get("label", self.site_id)
        print(f"[{site_label}] Recuperation des {content_type}s...")
        print(f"   URL: {self._get_api_url(endpoint)}")

        while page <= total_pages:
            items, total_pages = self._fetch_page(endpoint, page, params)

            if not items:
                break

            all_items.extend(items)
            print(f"   Page {page}/{total_pages} - {len(items)} items recuperes")
            page += 1

        print(f"[{site_label}] Total {content_type}s recuperes: {len(all_items)}")
        return all_items

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fonction de transformation des produits

# COMMAND ----------

def transform_product_item(item: Dict, site_id: str, site_config: Dict) -> Dict:
    """
    Transforme un item produit WordPress avec ACF en format standardise pour Databricks.

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
# MAGIC ## 7. Fonctions de gestion de table

# COMMAND ----------

def create_product_table_if_not_exists(catalog: str, schema: str, table_name: str):
    """Cree la table des produits si elle n'existe pas."""

    full_table_name = f"{catalog}.{schema}.{table_name}"

    # Cree le schema si necessaire
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # Verifie si la table existe
    if not spark.catalog.tableExists(full_table_name):
        print(f"Creation de la table produits {full_table_name}...")

        # Cree une DataFrame vide avec le schema produit
        empty_df = spark.createDataFrame([], PRODUCT_SCHEMA)

        # Ecrit en Delta avec partitionnement par site
        empty_df.write \
            .format("delta") \
            .partitionBy("site_id") \
            .option("delta.enableChangeDataFeed", "true") \
            .saveAsTable(full_table_name)

        print(f"Table produits {full_table_name} creee avec succes")
    else:
        print(f"Table produits {full_table_name} existe deja")


def get_last_imported_product_id(catalog: str, schema: str, table_name: str,
                                  site_id: str) -> Optional[int]:
    """Recupere le dernier ID WordPress importe pour les produits d'un site."""

    full_table_name = f"{catalog}.{schema}.{table_name}"

    try:
        result = spark.sql(f"""
            SELECT MAX(wp_id) as max_id
            FROM {full_table_name}
            WHERE site_id = '{site_id}'
        """).collect()

        if result and result[0]['max_id']:
            return result[0]['max_id']
    except Exception as e:
        print(f"Note: {e}")

    return None


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
# MAGIC ## 8. Pipeline principal d'import des produits

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
    create_product_table_if_not_exists(catalog, schema, table_name)

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
            since_id = get_last_imported_product_id(catalog, schema, table_name, site_id)
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
# MAGIC ## 9. Execution

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
# MAGIC ## 10. Verification des donnees

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
