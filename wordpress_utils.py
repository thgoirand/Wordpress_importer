# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC # Utilitaires WordPress - Configuration et fonctions partagées
# MAGIC
# MAGIC Ce notebook contient les configurations et fonctions communes utilisées par tous les pipelines d'import WordPress.
# MAGIC
# MAGIC **Usage:** Exécuter ce notebook via `%run` dans les autres notebooks d'import.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration globale

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

# Configuration Databricks par defaut
DATABRICKS_CATALOG = "gdp_cdt_dev_04_gld"
DATABRICKS_SCHEMA = "sandbox_mkt"

# --- Convention de nommage des tables ---
# SLV (Silver) : une table par type de contenu (donnees brutes WordPress)
BRONZE_TABLES = {
    "post": "cegid_website_blog_slv",
    "product": "cegid_website_product_slv",
    "landing_page": "cegid_website_landing_page_slv",
    "study_case": "cegid_website_study_case_slv",
}

# PLT (Plateau) : table unifiee avec extraction markdown initiale
SILVER_TABLE = "cegid_website_plt"

# GLD (Gold) : table finale enrichie par les fonctions IA
GOLD_TABLE = "cegid_website_gld"

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
# MAGIC ## 3. Fonctions utilitaires

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

        get_nested_value(item, "yoast_head_json.description")
        -> item["yoast_head_json"]["description"]

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

def get_rendered_field(item: dict, field: str) -> str:
    """Extrait un champ rendu de l'API WordPress."""
    if field in item and isinstance(item[field], dict):
        return item[field].get('rendered', '')
    return item.get(field, '')

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

    Offset par site (milliards):
    - fr: 1_000_000_000
    - es: 2_000_000_000
    - uk: 3_000_000_000
    - etc.

    Offset par type (millions):
    - post: 0
    - page: 10_000_000
    - landing_page: 20_000_000
    - product: 30_000_000
    """
    SITE_OFFSETS = {
        "fr": 1_000_000_000,
        "es": 2_000_000_000,
        "uk": 3_000_000_000,
        "us": 4_000_000_000,
        "de": 5_000_000_000,
        "it": 6_000_000_000,
        "pt": 7_000_000_000,
        "root": 0,  # Site racine sans prefixe
    }

    TYPE_OFFSETS = {
        "post": 0,
        "page": 10_000_000,
        "landing_page": 20_000_000,
        "product": 30_000_000,
        "custom_post_1": 40_000_000,
        "custom_post_2": 50_000_000,
        "study_case": 60_000_000,
    }

    site_offset = SITE_OFFSETS.get(site_id, 9_000_000_000)
    type_offset = TYPE_OFFSETS.get(content_type, 100_000_000)

    return site_offset + type_offset + wp_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Classe WordPressConnector

# COMMAND ----------

class WordPressConnector:
    """
    Connecteur WordPress multi-sites vers Databricks.
    Gere la recuperation paginee avec authentification et l'ecriture incrementale.
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
            "_embed": 1,  # Inclut les donnees liees (auteur, featured image, etc.)
            "status": "publish"  # Seulement les contenus publies
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
                # Page au-dela du maximum, fin de la pagination
                return [], 0
            print(f"Erreur HTTP {e.response.status_code}: {e}")
            return [], 0
        except requests.exceptions.RequestException as e:
            print(f"Erreur API WordPress: {e}")
            return [], 0

    def fetch_all_content(self, content_type: str, endpoint: str,
                          since_id: Optional[int] = None,
                          modified_after: Optional[str] = None) -> List[Dict]:
        """
        Recupere tous les contenus d'un type donne pour ce site.
        Support optionnel de l'import incremental via modified_after (date ISO 8601)
        ou since_id (ancien mode par ID, deprecie).
        """
        all_items = []
        page = 1
        total_pages = 1

        params = {}
        if modified_after:
            # Recupere les contenus modifies apres cette date
            params["modified_after"] = modified_after
            params["orderby"] = "modified"
            params["order"] = "asc"
        elif since_id:
            # Ancien mode: recupere seulement les IDs superieurs (nouveaux contenus)
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
# MAGIC ## 5. Fonctions de gestion de table generiques

# COMMAND ----------

def create_delta_table(catalog: str, schema: str, table_name: str,
                       spark_schema: StructType, partition_by: List[str] = None):
    """
    Cree une table Delta si elle n'existe pas.

    Args:
        catalog: Nom du catalogue Unity Catalog
        schema: Nom du schema
        table_name: Nom de la table
        spark_schema: Schema PySpark de la table
        partition_by: Liste des colonnes de partitionnement
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    # Cree le schema si necessaire
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # Verifie si la table existe
    if not spark.catalog.tableExists(full_table_name):
        print(f"Creation de la table {full_table_name}...")

        # Cree une DataFrame vide avec le schema
        empty_df = spark.createDataFrame([], spark_schema)

        # Ecrit en Delta avec partitionnement
        writer = empty_df.write.format("delta").option("delta.enableChangeDataFeed", "true")

        if partition_by:
            writer = writer.partitionBy(*partition_by)

        writer.saveAsTable(full_table_name)

        print(f"Table {full_table_name} creee avec succes")
    else:
        # Ajoute les colonnes manquantes si le schema a evolue
        existing_columns = {f.name for f in spark.table(full_table_name).schema.fields}
        for field in spark_schema.fields:
            if field.name not in existing_columns:
                col_type = field.dataType.simpleString()
                spark.sql(f"ALTER TABLE {full_table_name} ADD COLUMNS ({field.name} {col_type})")
                print(f"Colonne '{field.name}' ajoutee a {full_table_name}")
        print(f"Table {full_table_name} existe deja")


def get_last_imported_id(catalog: str, schema: str, table_name: str,
                         site_id: str, content_type: str = None) -> Optional[int]:
    """
    Recupere le dernier ID WordPress importe pour un site (et optionnellement un type).
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    try:
        where_clause = f"WHERE site_id = '{site_id}'"
        if content_type:
            where_clause += f" AND content_type = '{content_type}'"

        result = spark.sql(f"""
            SELECT MAX(wp_id) as max_id
            FROM {full_table_name}
            {where_clause}
        """).collect()

        if result and result[0]['max_id']:
            return result[0]['max_id']
    except Exception as e:
        print(f"Note: {e}")

    return None


def get_last_modified_date(catalog: str, schema: str, table_name: str,
                           site_id: str, content_type: str = None) -> Optional[str]:
    """
    Recupere la date de derniere modification pour un site et type de contenu.
    Retourne la date au format ISO 8601 (ex: 2024-01-15T10:30:00) pour l'API WordPress.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    try:
        where_clause = f"WHERE site_id = '{site_id}'"
        if content_type:
            where_clause += f" AND content_type = '{content_type}'"

        result = spark.sql(f"""
            SELECT MAX(date_modified) as last_modified
            FROM {full_table_name}
            {where_clause}
        """).collect()

        if result and result[0]['last_modified']:
            return result[0]['last_modified'].strftime('%Y-%m-%dT%H:%M:%S')
    except Exception as e:
        print(f"Note: {e}")

    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Schemas Medallion (Bronze / Silver / Gold)

# COMMAND ----------

# --- BRONZE SCHEMA ---
# Schema minimal pour les tables bronze (donnees brutes WordPress)
BRONZE_SCHEMA = StructType([
    StructField("id", LongType(), False),
    StructField("wp_id", IntegerType(), False),
    StructField("content_type", StringType(), False),
    StructField("site_id", StringType(), False),
    StructField("raw_json", StringType(), True),
    StructField("date_modified", TimestampType(), True),
    StructField("date_imported", TimestampType(), False),
])

# --- SILVER SCHEMA ---
# Schema unifie pour la table silver (contenus nettoyes et standardises)
SILVER_SCHEMA = StructType([
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

# --- GOLD SCHEMA ---
# Schema enrichi pour la table gold (contenus + enrichissements AI)
GOLD_SCHEMA = StructType([
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
    StructField("date_formatted", TimestampType(), True),

    # --- AUTEUR & STATUT ---
    StructField("status", StringType(), True),
    StructField("author_id", IntegerType(), True),

    # --- MEDIA ---
    StructField("featured_image_url", StringType(), True),

    # --- LANGUE ---
    StructField("language", StringType(), True),

    # --- ENRICHISSEMENTS AI ---
    StructField("key_figures", ArrayType(StringType()), True),
    StructField("has_regulatory_content", BooleanType(), True),
    StructField("has_country_specific_context", BooleanType(), True),
    StructField("funnel_stage", StringType(), True),

    # --- DONNEES BRUTES ---
    StructField("raw_json", StringType(), True),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Fonctions generiques d'upsert (Bronze / Silver / Gold)

# COMMAND ----------

def upsert_bronze(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Upsert (MERGE) de donnees brutes dans une table bronze.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"
    df.createOrReplaceTempView("new_bronze")

    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING new_bronze AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                raw_json = source.raw_json,
                date_modified = source.date_modified,
                date_imported = source.date_imported
        WHEN NOT MATCHED THEN
            INSERT (id, wp_id, content_type, site_id, raw_json, date_modified, date_imported)
            VALUES (source.id, source.wp_id, source.content_type, source.site_id,
                    source.raw_json, source.date_modified, source.date_imported)
    """)
    print(f"Upsert bronze termine dans {full_table_name}")


def upsert_silver(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Upsert (MERGE) de contenus standardises dans la table PLT cegid_website_plt.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"
    df.createOrReplaceTempView("new_silver")

    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING new_silver AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                title = source.title,
                content_raw = source.content_raw,
                content_text = source.content_text,
                excerpt = source.excerpt,
                meta_description = source.meta_description,
                meta_title = source.meta_title,
                meta_keyword = source.meta_keyword,
                noindex = source.noindex,
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
    print(f"Upsert silver termine dans {full_table_name}")


def upsert_gold_from_silver(catalog: str, schema: str, gold_table: str,
                             silver_table: str, content_type: str = None):
    """
    Copie les donnees silver vers gold pour les contenus non encore presents dans gold.
    Permet aux AI formatters de travailler ensuite sur la table gold.
    """
    full_gold = f"{catalog}.{schema}.{gold_table}"
    full_silver = f"{catalog}.{schema}.{silver_table}"

    type_filter = f"AND content_type = '{content_type}'" if content_type else ""

    spark.sql(f"""
        MERGE INTO {full_gold} AS target
        USING (
            SELECT * FROM {full_silver}
            WHERE 1=1 {type_filter}
        ) AS source
        ON target.id = source.id
        WHEN MATCHED THEN
            UPDATE SET
                title = source.title,
                content_raw = source.content_raw,
                excerpt = source.excerpt,
                meta_description = source.meta_description,
                meta_title = source.meta_title,
                meta_keyword = source.meta_keyword,
                noindex = source.noindex,
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
    print(f"Sync silver -> gold termine dans {full_gold}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Export des variables pour les autres notebooks
# MAGIC
# MAGIC Ce notebook expose les variables et fonctions suivantes:
# MAGIC - `WP_SITES`, `WP_SITES_TO_IMPORT`, `WORDPRESS_CONFIG`
# MAGIC - `DATABRICKS_CATALOG`, `DATABRICKS_SCHEMA`
# MAGIC - `BRONZE_TABLES`, `SILVER_TABLE`, `GOLD_TABLE`
# MAGIC - `BRONZE_SCHEMA`, `SILVER_SCHEMA`, `GOLD_SCHEMA`
# MAGIC - `clean_html_content()`, `get_nested_value()`, `parse_wp_date()`, `calculate_composite_id()`
# MAGIC - `WordPressConnector`
# MAGIC - `create_delta_table()`, `get_last_imported_id()`, `get_last_modified_date()`
# MAGIC - `upsert_bronze()`, `upsert_silver()`, `upsert_gold_from_silver()`
