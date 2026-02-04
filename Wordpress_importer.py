# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Collecte des contenus du site Cegid pour RAG et SEO
# MAGIC
# MAGIC Ce notebook permet de :
# MAGIC - Récupérer les articles, pages, landing pages et custom posts via l'API WordPress REST
# MAGIC - Stocker les contenus dans la table `Cegid_website` avec gestion des offsets
# MAGIC - Supporter l'incrémental via le tracking des IDs déjà importés

# COMMAND ----------

# MAGIC %md
# MAGIC
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
        "label": "España"
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

# Site(s) à importer (liste ou "all" pour tous)
WP_SITES_TO_IMPORT = ["fr"]  # Ex: ["fr", "es"] ou list(WP_SITES.keys()) pour tous

# Configuration Databricks
DATABRICKS_CONFIG = {
    "catalog": "gdp_cdt_dev_04_gld",  # Adapter selon votre catalogue Unity Catalog
    "schema": "sandbox_mkt",
    "table_name": "cegid_website"
}

# Configuration technique WordPress
WORDPRESS_CONFIG = {
    "base_url": WP_BASE_URL,
    "api_endpoint": "/wp-json/wp/v2",
    "per_page": 100,  # Maximum autorisé par l'API WordPress
    "timeout": 30,
    "auth": (WP_LOGIN, WP_PASSWORD)  # Basic Auth ou Application Password
}

# Configuration des sites par langue
CONTENT_TYPES = {
    "post": {
        "endpoint": "/posts",
        "label": "blog"
    },
    "page": {
        "endpoint": "/pages",
        "label": "pages"
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Imports et initialisation
# MAGIC

# COMMAND ----------

import requests
import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    TimestampType, ArrayType, MapType, LongType
)
from pyspark.sql.functions import (
    col, lit, current_timestamp, when, max as spark_max,
    from_json, explode, regexp_replace, trim
)
from bs4 import BeautifulSoup
import html
from pyspark.sql.types import BooleanType

# Initialisation Spark (déjà disponible dans Databricks)
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schéma de la table Cegid_website
# MAGIC

# COMMAND ----------

FIELD_MAPPING = {

    # --- IDENTIFIANTS ---
    "id": None,                          # Calculé via calculate_composite_id()
    "wp_id": "id",
    "content_type": None,                # Passé en paramètre
    "site_id": None,                     # Config du site
    
    # --- MÉTADONNÉES ---
    "slug": "slug",
    "url": "link",
    "title": "title.rendered",
    
    # --- SEO ---
    "meta_description": "yoast_head_json.description",
    "meta_title": "yoast_head_json.title",
    "meta_keyword": "yoast_head_json.focuskw",        # Focus keyword Yoast
    "noindex": "yoast_head_json.robots.index",       # Robot noindex
    
    # --- CONTENU ---
    "content_raw": "content.rendered",
    "content_text": None,                # Calculé (HTML nettoyé)
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
    
    # --- DONNÉES BRUTES ---
    "raw_json": None,
}

CONTENT_SCHEMA = StructType([
    # --- IDENTIFIANTS ---
    StructField("id", LongType(), False),
    StructField("wp_id", IntegerType(), False),
    StructField("content_type", StringType(), False),
    StructField("site_id", StringType(), False),
    
    # --- MÉTADONNÉES ---
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
    
    # --- DONNÉES BRUTES ---
    StructField("raw_json", StringType(), True),
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
    
    # Décode les entités HTML
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
    Extrait une valeur imbriquée depuis un dictionnaire via une notation pointée.
    
    Exemples:
        get_nested_value(item, "title.rendered")  
        → item["title"]["rendered"]
        
        get_nested_value(item, "_embedded.author.0.name")  
        → item["_embedded"]["author"][0]["name"]
        
        get_nested_value(item, "yoast_head_json.description")
        → item["yoast_head_json"]["description"]
    
    Args:
        data: Dictionnaire source
        path: Chemin en notation pointée (ex: "a.b.0.c")
        default: Valeur par défaut si le chemin n'existe pas
    
    Returns:
        La valeur trouvée ou default
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
                # Support des index numériques (ex: "author.0.name")
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
    Formats supportés: 
    - 2024-01-15T10:30:00
    - 2024-01-15T10:30:00Z
    - 2024-01-15
    """
    if not date_str:
        return None
    
    try:
        # Format avec T et possible Z
        if 'T' in date_str:
            # Supprime le Z final si présent
            clean_date = date_str.replace('Z', '')
            return datetime.fromisoformat(clean_date)
        else:
            # Format date seule
            return datetime.strptime(date_str, '%Y-%m-%d')
    except (ValueError, TypeError):
        return None

def calculate_composite_id(wp_id: int, content_type: str, site_id: str) -> int:
    """
    Calcule un ID composite unique basé sur le site, le type de contenu et l'ID WordPress.
    Permet d'éviter les collisions entre sites et types de contenus.
    
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
        "root": 0,  # Site racine sans préfixe
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
# MAGIC # 5. Classe principale du connecteur

# COMMAND ----------

class WordPressConnector:
    """
    Connecteur WordPress multi-sites vers Databricks.
    Gère la récupération paginée avec authentification et l'écriture incrémentale.
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
        """Construit l'URL du site avec le préfixe de langue."""
        prefix = self.site_config.get("prefix", "")
        if prefix:
            return f"{self.base_url}/{prefix}"
        return self.base_url
        
    def _get_api_url(self, endpoint: str) -> str:
        """Construit l'URL complète de l'API pour ce site."""
        return f"{self._get_site_url()}{self.api_endpoint}{endpoint}"
    
    def _fetch_page(self, endpoint: str, page: int, params: Dict = None) -> Tuple[List[Dict], int]:
        """
        Récupère une page de résultats de l'API WordPress.
        Retourne les items et le nombre total de pages.
        """
        url = self._get_api_url(endpoint)
        
        request_params = {
            "page": page,
            "per_page": self.per_page,
            "_embed": 1,  # Inclut les données liées (auteur, featured image, etc.)
            "status": "publish"  # Seulement les contenus publiés
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
                # Page au-delà du maximum, fin de la pagination
                return [], 0
            print(f"❌ Erreur HTTP {e.response.status_code}: {e}")
            return [], 0
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur API WordPress: {e}")
            return [], 0
    
    def fetch_all_content(self, content_type: str, endpoint: str, 
                          since_id: Optional[int] = None) -> List[Dict]:
        """
        Récupère tous les contenus d'un type donné pour ce site.
        Support optionnel de l'import incrémental via since_id.
        """
        all_items = []
        page = 1
        total_pages = 1
        
        params = {}
        if since_id:
            # Récupère seulement les IDs supérieurs (nouveaux contenus)
            params["after"] = since_id
        
        site_label = self.site_config.get("label", self.site_id)
        print(f"📥 [{site_label}] Récupération des {content_type}s...")
        print(f"   URL: {self._get_api_url(endpoint)}")
        
        while page <= total_pages:
            items, total_pages = self._fetch_page(endpoint, page, params)
            
            if not items:
                break
                
            all_items.extend(items)
            print(f"   Page {page}/{total_pages} - {len(items)} items récupérés")
            page += 1
        
        print(f"✅ [{site_label}] Total {content_type}s récupérés: {len(all_items)}")
        return all_items
    
    def transform_item(self, item: Dict, content_type: str) -> Dict:
        """
        Transforme un item WordPress en format standardisé pour Databricks.
        
        Schéma cible: gdp_cdt_dev_04_gld.sandbox_mkt.cegid_website
        22 colonnes (hors partitions)
        """
        wp_id = item.get('id')
        
        # === CONTENU ===
        content_raw = get_nested_value(item, 'content.rendered', '')
        content_text = clean_html_content(content_raw)
        excerpt_raw = get_nested_value(item, 'excerpt.rendered', '')
        
        # === MEDIA ===
        featured_image_url = get_nested_value(item, '_embedded.wp:featuredmedia.0.source_url')
        
        # === LANGUE ===
        language = item.get('lang') or self.site_config.get("language", "fr")
        
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
            "id": calculate_composite_id(wp_id, content_type, self.site_id),
            "wp_id": wp_id,
            "content_type": content_type,
            "site_id": self.site_id,
            
            # --- MÉTADONNÉES ---
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
            
            # --- DONNÉES BRUTES ---
            "raw_json": json.dumps(item, ensure_ascii=False),
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fonctions de gestion de la table Databricks

# COMMAND ----------

def create_table_if_not_exists(catalog: str, schema: str, table_name: str):
    """Crée la table si elle n'existe pas."""
    
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    # Crée le schéma si nécessaire
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
    
    # Vérifie si la table existe
    if not spark.catalog.tableExists(full_table_name):
        print(f"📝 Création de la table {full_table_name}...")
        
        # Crée une DataFrame vide avec le schéma
        empty_df = spark.createDataFrame([], CONTENT_SCHEMA)
        
        # Écrit en Delta avec partitionnement par site et type
        empty_df.write \
            .format("delta") \
            .partitionBy("site_id", "content_type") \
            .option("delta.enableChangeDataFeed", "true") \
            .saveAsTable(full_table_name)
        
        print(f"✅ Table {full_table_name} créée avec succès")
    else:
        print(f"ℹ️ Table {full_table_name} existe déjà")


def get_last_imported_id(catalog: str, schema: str, table_name: str, 
                         content_type: str, site_id: str) -> Optional[int]:
    """Récupère le dernier ID WordPress importé pour un type de contenu et un site."""
    
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    try:
        result = spark.sql(f"""
            SELECT MAX(wp_id) as max_id 
            FROM {full_table_name} 
            WHERE content_type = '{content_type}'
              AND site_id = '{site_id}'
        """).collect()
        
        if result and result[0]['max_id']:
            return result[0]['max_id']
    except Exception as e:
        print(f"Note: {e}")
    
    return None


def upsert_content(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Upsert (MERGE) des contenus dans la table Delta.
    Met à jour les contenus existants, insère les nouveaux.
    """
    
    full_table_name = f"{catalog}.{schema}.{table_name}"
    
    # Crée une vue temporaire
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
    
    print(f"✅ Upsert terminé dans {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pipeline principal

# COMMAND ----------

def run_import_pipeline(content_types: Dict = CONTENT_TYPES, 
                        sites_to_import: List[str] = WP_SITES_TO_IMPORT,
                        incremental: bool = True,
                        specific_type: Optional[str] = None):
    """
    Exécute le pipeline d'import complet pour un ou plusieurs sites.
    
    Args:
        content_types: Dictionnaire des types de contenu à importer
        sites_to_import: Liste des site_id à importer (ex: ["fr", "es"])
        incremental: Si True, importe seulement les nouveaux contenus
        specific_type: Si spécifié, importe seulement ce type de contenu
    """
    
    catalog = DATABRICKS_CONFIG["catalog"]
    schema = DATABRICKS_CONFIG["schema"]
    table_name = DATABRICKS_CONFIG["table_name"]
    
    # Crée la table si nécessaire
    create_table_if_not_exists(catalog, schema, table_name)
    
    # Filtre les types si spécifié
    types_to_import = {specific_type: content_types[specific_type]} if specific_type else content_types
    
    total_imported = 0
    
    # Boucle sur les sites
    for site_id in sites_to_import:
        if site_id not in WP_SITES:
            print(f"⚠️ Site '{site_id}' non configuré, ignoré")
            continue
            
        site_config = WP_SITES[site_id]
        site_label = site_config.get("label", site_id)
        
        print(f"\n{'#'*60}")
        print(f"🌐 SITE: {site_label} ({site_id})")
        print(f"   URL: {WORDPRESS_CONFIG['base_url']}/{site_config.get('prefix', '')}")
        print(f"{'#'*60}")
        
        # Initialise le connecteur pour ce site
        connector = WordPressConnector(site_id, site_config)
        
        for content_type, config in types_to_import.items():
            print(f"\n{'='*50}")
            print(f"📦 [{site_label}] Import: {config['label']} ({content_type})")
            print(f"{'='*50}")
            
            # Récupère le dernier ID pour import incrémental
            since_id = None
            if incremental:
                since_id = get_last_imported_id(catalog, schema, table_name, content_type, site_id)
                if since_id:
                    print(f"ℹ️ Mode incrémental - depuis ID: {since_id}")
            
            # Récupère les contenus WordPress
            items = connector.fetch_all_content(
                content_type=content_type,
                endpoint=config["endpoint"],
                since_id=since_id
            )
            
            if not items:
                print(f"ℹ️ Aucun nouveau contenu à importer pour {content_type}")
                continue
            
            # Transforme les items
            transformed_items = [
                connector.transform_item(item, content_type) 
                for item in items
            ]
            
            # Crée le DataFrame
            df = spark.createDataFrame(transformed_items, CONTENT_SCHEMA)
            
            # Upsert dans la table
            upsert_content(df, catalog, schema, table_name)
            
            total_imported += len(transformed_items)
            print(f"📊 [{site_label}] {len(transformed_items)} {content_type}(s) importé(s)")
    
    print(f"\n{'#'*60}")
    print(f"🎉 Import terminé! Total: {total_imported} contenus importés")
    print(f"   Sites traités: {', '.join(sites_to_import)}")
    print(f"{'#'*60}")
    
    return total_imported

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Exécution

# COMMAND ----------


# =============================================================================
# EXEMPLES D'EXÉCUTION
# =============================================================================

# Import d'un seul site (FR), tous les types de contenu
# run_import_pipeline(sites_to_import=["fr"], incremental=False)

# Import incrémental d'un seul site
# run_import_pipeline(sites_to_import=["fr"], incremental=True)

# Import de plusieurs sites
# run_import_pipeline(sites_to_import=["fr", "es", "uk"], incremental=False)

# Import de TOUS les sites configurés
# run_import_pipeline(sites_to_import=list(WP_SITES.keys()), incremental=False)

# Import d'un type spécifique sur un site
run_import_pipeline(sites_to_import=["fr"], specific_type="post", incremental=False)


# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Vérification des données

# COMMAND ----------

# Affiche un aperçu des données importées par site et type
full_table = f"{DATABRICKS_CONFIG['catalog']}.{DATABRICKS_CONFIG['schema']}.{DATABRICKS_CONFIG['table_name']}"

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

# Exemple: Aperçu des derniers articles par site
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## AI functions

# COMMAND ----------

# 1. Identifier les éléments à traiter
pre_update_query = f"""
SELECT 
    id,
    title,
    slug,
    date_modified,
    CASE 
        WHEN content_text IS NULL OR content_text = '' THEN '🆕 Nouveau'
        ELSE '✏️ Update'
    END AS statut
FROM {SOURCE_TABLE}
WHERE 
    raw_json IS NOT NULL AND raw_json != ''
    AND (
        content_text IS NULL 
        OR content_text = ''
        OR (date_modified >= CURRENT_DATE() - INTERVAL 7 DAYS 
            AND date_modified > COALESCE(date_formatted, '1900-01-01'))
    )
ORDER BY date_modified DESC
"""

df_to_process = spark.sql(pre_update_query)
count = df_to_process.count()

print(f"📋 {count} éléments à traiter :")
display(df_to_process)

# 2. MERGE
if count > 0:
    merge_query = f"""
    MERGE INTO {SOURCE_TABLE} AS target
    USING (
        SELECT 
            id,
            AI_QUERY(
                'databricks-claude-haiku-4-5',
                CONCAT(
                    'Tu es un expert en formatage de contenu web. ',
                    'Convertis ce JSON WordPress en markdown propre et structuré. ',
                    'Utilise des titres (##, ###), des listes, et formate correctement les liens. ',
                    'Retourne uniquement le markdown, sans explications. ',
                    'JSON: ',
                    raw_json
                )
            ) AS new_content_text,
            CURRENT_TIMESTAMP() AS new_date_formatted
        FROM {SOURCE_TABLE}
        WHERE 
            raw_json IS NOT NULL AND raw_json != ''
            AND (
                content_text IS NULL 
                OR content_text = ''
                OR (date_modified >= CURRENT_DATE() - INTERVAL 7 DAYS 
                    AND date_modified > COALESCE(date_formatted, '1900-01-01'))
            )
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET 
        content_text = source.new_content_text,
        date_formatted = source.new_date_formatted
    """
    
    spark.sql(merge_query)
    print("✅ MERGE terminé")
else:
    print("ℹ️ Aucun élément à traiter")

# COMMAND ----------

# MAGIC %md
# MAGIC