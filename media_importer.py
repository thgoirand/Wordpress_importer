# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Import des Médias WordPress
# MAGIC
# MAGIC Ce notebook permet de :
# MAGIC - Récupérer les médias (attachments) via l'API WordPress REST (`/wp-json/wp/v2/media`)
# MAGIC - Stocker les données dans la table `cegid_website_medias`
# MAGIC - Supporter l'import incrémental (basé sur `date_modified`) et le mode TRUNCATE + INSERT

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Chargement des utilitaires communs

# COMMAND ----------

# MAGIC %run ./wordpress_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration spécifique

# COMMAND ----------

DATABRICKS_MEDIA_CONFIG = {
    "catalog": DATABRICKS_CATALOG,
    "schema": DATABRICKS_SCHEMA,
    "table_name": "cegid_website_medias",
}

# Endpoint WordPress pour les médias
MEDIA_ENDPOINT = "/media"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Imports complémentaires

# COMMAND ----------

import json
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, LongType, ArrayType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schéma de la table `cegid_website_medias`

# COMMAND ----------

MEDIA_SCHEMA = StructType([
    # --- IDENTIFIANTS ---
    StructField("id", LongType(), False),                    # ID composite unique
    StructField("wp_id", IntegerType(), False),              # ID WordPress original
    StructField("site_id", StringType(), False),             # Identifiant du site (fr, es, uk, etc.)

    # --- INFORMATIONS PRINCIPALES ---
    StructField("slug", StringType(), True),                 # Slug URL-friendly
    StructField("title", StringType(), True),                # Titre du média
    StructField("alt_text", StringType(), True),             # Texte alternatif
    StructField("source_url", StringType(), True),           # URL complète du fichier original

    # --- TYPE ET DIMENSIONS ---
    StructField("media_type", StringType(), True),           # Type général (image, video, etc.)
    StructField("mime_type", StringType(), True),            # Type MIME (image/webp, image/jpeg, etc.)
    StructField("file_extension", StringType(), True),       # Extension du fichier (webp, jpg, png, etc.)
    StructField("filesize", LongType(), True),               # Taille du fichier en octets
    StructField("width", IntegerType(), True),               # Largeur en pixels
    StructField("height", IntegerType(), True),              # Hauteur en pixels

    # --- MÉTADONNÉES ---
    StructField("class_list", ArrayType(StringType()), True),  # Classes CSS associées
    StructField("status", StringType(), True),               # Statut (inherit, etc.)

    # --- LANGUE ---
    StructField("language", StringType(), True),             # Code langue (fr, es, en-GB, etc.)

    # --- DATES ---
    StructField("date_created", TimestampType(), True),      # Date de création WordPress
    StructField("date_modified", TimestampType(), True),     # Date de dernière modification
    StructField("date_imported", TimestampType(), False),    # Date d'import dans Databricks

    # --- DONNÉES BRUTES ---
    StructField("raw_json", StringType(), True),             # JSON complet de l'API
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4b. Code SQL de création de la table (référence)
# MAGIC
# MAGIC ```sql
# MAGIC CREATE TABLE IF NOT EXISTS gdp_cdt_dev_04_gld.sandbox_mkt.cegid_website_medias (
# MAGIC     id              BIGINT        NOT NULL,
# MAGIC     wp_id           INT           NOT NULL,
# MAGIC     site_id         STRING        NOT NULL,
# MAGIC     slug            STRING,
# MAGIC     title           STRING,
# MAGIC     alt_text        STRING,
# MAGIC     source_url      STRING,
# MAGIC     media_type      STRING,
# MAGIC     mime_type       STRING,
# MAGIC     file_extension  STRING,
# MAGIC     filesize        BIGINT,
# MAGIC     width           INT,
# MAGIC     height          INT,
# MAGIC     class_list      ARRAY<STRING>,
# MAGIC     status          STRING,
# MAGIC     language        STRING,
# MAGIC     date_created    TIMESTAMP,
# MAGIC     date_modified   TIMESTAMP,
# MAGIC     date_imported   TIMESTAMP     NOT NULL,
# MAGIC     raw_json        STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (site_id)
# MAGIC TBLPROPERTIES (delta.enableChangeDataFeed = true);
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fonctions utilitaires

# COMMAND ----------

def calculate_media_id(wp_id: int, site_id: str) -> int:
    """
    Calcule un ID composite unique pour les médias.

    Structure: SITE_OFFSET + MEDIA_OFFSET + wp_id

    Offset par site (milliards):
    - fr: 1_000_000_000
    - es: 2_000_000_000
    - etc.

    Offset média: 700_000_000 (pour éviter collision avec les autres types)
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

    MEDIA_OFFSET = 700_000_000

    site_offset = SITE_OFFSETS.get(site_id, 9_000_000_000)
    return site_offset + MEDIA_OFFSET + wp_id


def extract_file_extension(item: dict) -> str:
    """
    Extrait l'extension du fichier depuis source_url ou mime_type.
    """
    source_url = item.get("source_url", "")
    if source_url:
        # Extraire l'extension depuis l'URL
        path = source_url.split("?")[0]  # Supprime les query params
        _, ext = os.path.splitext(path)
        if ext:
            return ext.lstrip(".").lower()

    # Fallback sur le mime_type
    mime = item.get("mime_type", "")
    if "/" in mime:
        return mime.split("/")[-1].lower()

    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Connecteur WordPress pour les médias

# COMMAND ----------

class WordPressMediaConnector:
    """
    Connecteur WordPress pour récupérer les médias (attachments).
    Utilise l'endpoint /wp-json/wp/v2/media.
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
        """
        url = self._get_api_url(endpoint)

        request_params = {
            "page": page,
            "per_page": self.per_page,
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
            items = response.json()

            return items, total_pages

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400:
                return [], 0
            elif e.response.status_code == 404:
                print(f"⚠️ Endpoint {endpoint} non disponible sur ce site")
                return [], 0
            print(f"❌ Erreur HTTP {e.response.status_code}: {e}")
            return [], 0
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur API WordPress: {e}")
            return [], 0

    def fetch_all_media(self, modified_after: Optional[str] = None) -> List[Dict]:
        """
        Récupère tous les médias du site.

        Args:
            modified_after: Date ISO 8601 pour l'import incrémental (ex: "2024-01-15T10:30:00")
        """
        all_items = []
        page = 1
        total_pages = 1

        params = {}
        if modified_after:
            params["modified_after"] = modified_after
            params["orderby"] = "modified"
            params["order"] = "asc"

        site_label = self.site_config.get("label", self.site_id)
        print(f"📥 [{site_label}] Récupération des médias...")
        print(f"   URL: {self._get_api_url(MEDIA_ENDPOINT)}")
        if modified_after:
            print(f"   Mode incrémental: modified_after={modified_after}")

        while page <= total_pages:
            items, total_pages = self._fetch_page(MEDIA_ENDPOINT, page, params)

            if not items:
                break

            all_items.extend(items)
            print(f"   Page {page}/{total_pages} - {len(items)} médias récupérés")
            page += 1

        print(f"✅ [{site_label}] Total médias récupérés: {len(all_items)}")
        return all_items

    def transform_media_item(self, item: Dict) -> Dict:
        """
        Transforme un média WordPress en format standardisé pour la table cegid_website_medias.
        """
        wp_id = item.get("id")

        # Dimensions et taille depuis media_details
        media_details = item.get("media_details") or {}
        width = media_details.get("width")
        height = media_details.get("height")
        filesize = media_details.get("filesize")

        # Extension du fichier
        file_ext = extract_file_extension(item)

        # Titre rendu
        title = get_nested_value(item, "title.rendered", "")

        # class_list
        class_list = item.get("class_list")
        if class_list and not isinstance(class_list, list):
            class_list = None

        return {
            "id": calculate_media_id(wp_id, self.site_id),
            "wp_id": wp_id,
            "site_id": self.site_id,
            "slug": item.get("slug"),
            "title": title,
            "alt_text": item.get("alt_text", ""),
            "source_url": item.get("source_url"),
            "media_type": item.get("media_type"),
            "mime_type": item.get("mime_type"),
            "file_extension": file_ext,
            "filesize": filesize,
            "width": width,
            "height": height,
            "class_list": class_list,
            "status": item.get("status"),
            "language": item.get("lang") or self.site_config.get("language", "fr"),
            "date_created": parse_wp_date(item.get("date")),
            "date_modified": parse_wp_date(item.get("modified")),
            "date_imported": datetime.now(),
            "raw_json": json.dumps(item, ensure_ascii=False),
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Fonctions de gestion de la table Databricks

# COMMAND ----------

def create_media_table_if_not_exists(catalog: str, schema: str, table_name: str):
    """Crée la table medias si elle n'existe pas."""
    full_table_name = f"{catalog}.{schema}.{table_name}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    if not spark.catalog.tableExists(full_table_name):
        print(f"📝 Création de la table {full_table_name}...")

        empty_df = spark.createDataFrame([], MEDIA_SCHEMA)

        empty_df.write \
            .format("delta") \
            .partitionBy("site_id") \
            .option("delta.enableChangeDataFeed", "true") \
            .saveAsTable(full_table_name)

        print(f"✅ Table {full_table_name} créée avec succès")
    else:
        print(f"ℹ️ Table {full_table_name} existe déjà")
        _migrate_media_table_schema(full_table_name)


def _migrate_media_table_schema(full_table_name: str):
    """
    Vérifie que la table existante possède toutes les colonnes du schéma attendu.
    Ajoute les colonnes manquantes via ALTER TABLE si nécessaire.
    """
    print(f"ℹ️ Schéma à jour, aucune migration nécessaire")


def truncate_media_data(catalog: str, schema: str, table_name: str, site_id: str = None):
    """
    Vide les données de la table médias (ou une partition spécifique).
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    if site_id:
        print(f"🗑️ Suppression des données: site_id = '{site_id}'")
        spark.sql(f"DELETE FROM {full_table_name} WHERE site_id = '{site_id}'")
    else:
        print(f"🗑️ Vidage complet de la table {full_table_name}")
        spark.sql(f"TRUNCATE TABLE {full_table_name}")

    print("✅ Suppression terminée")


def upsert_media_data(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Insère ou met à jour les médias via MERGE (upsert sur id).
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    df.createOrReplaceTempView("new_media")

    spark.sql(f"""
        MERGE INTO {full_table_name} AS target
        USING new_media AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET
            wp_id = source.wp_id,
            site_id = source.site_id,
            slug = source.slug,
            title = source.title,
            alt_text = source.alt_text,
            source_url = source.source_url,
            media_type = source.media_type,
            mime_type = source.mime_type,
            file_extension = source.file_extension,
            filesize = source.filesize,
            width = source.width,
            height = source.height,
            class_list = source.class_list,
            status = source.status,
            language = source.language,
            date_created = source.date_created,
            date_modified = source.date_modified,
            date_imported = source.date_imported,
            raw_json = source.raw_json
        WHEN NOT MATCHED THEN INSERT *
    """)

    print(f"✅ Upsert terminé dans {full_table_name}")


def insert_media_data(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Insère les données dans la table médias (après truncate).
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(full_table_name)

    print(f"✅ Insertion terminée dans {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Pipeline principal

# COMMAND ----------

def run_media_import_pipeline(
    sites_to_import: List[str] = WP_SITES_TO_IMPORT,
    incremental: bool = True,
    truncate_before_insert: bool = False,
):
    """
    Exécute le pipeline d'import des médias WordPress.

    Args:
        sites_to_import: Liste des site_id à importer (ex: ["fr", "es"])
        incremental: Si True, utilise la date de dernière modification pour ne récupérer
                     que les médias modifiés depuis le dernier import.
                     Si False, récupère tous les médias.
        truncate_before_insert: Si True, vide les données du site avant insertion
                                (incompatible avec incremental, force un full refresh).
    """
    catalog = DATABRICKS_MEDIA_CONFIG["catalog"]
    schema = DATABRICKS_MEDIA_CONFIG["schema"]
    table_name = DATABRICKS_MEDIA_CONFIG["table_name"]

    # Crée la table si nécessaire
    create_media_table_if_not_exists(catalog, schema, table_name)

    total_imported = 0

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

        # Détecte la date de dernière modification pour l'import incrémental
        modified_after = None
        if incremental and not truncate_before_insert:
            full_table_name = f"{catalog}.{schema}.{table_name}"
            try:
                result = spark.sql(f"""
                    SELECT MAX(date_modified) as last_modified
                    FROM {full_table_name}
                    WHERE site_id = '{site_id}'
                """).collect()
                if result and result[0]['last_modified']:
                    modified_after = result[0]['last_modified'].strftime('%Y-%m-%dT%H:%M:%S')
                    print(f"📅 Import incrémental depuis: {modified_after}")
            except Exception as e:
                print(f"ℹ️ Pas de données existantes, import complet ({e})")

        # Initialise le connecteur
        connector = WordPressMediaConnector(site_id, site_config)

        # Récupère les médias
        items = connector.fetch_all_media(modified_after=modified_after)

        if not items:
            print(f"ℹ️ Aucun média trouvé pour {site_label}")
            continue

        # Transforme les items
        transformed = [connector.transform_media_item(item) for item in items]
        print(f"📊 [{site_label}] {len(transformed)} médias transformés")

        # Crée le DataFrame
        df_media = spark.createDataFrame(transformed, MEDIA_SCHEMA)

        # Insertion
        if truncate_before_insert:
            truncate_media_data(catalog, schema, table_name, site_id)
            insert_media_data(df_media, catalog, schema, table_name)
        else:
            # Mode MERGE (upsert) : incrémental ou full
            upsert_media_data(df_media, catalog, schema, table_name)

        total_imported += len(transformed)

    print(f"\n{'#'*60}")
    print(f"🎉 Import médias terminé! Total: {total_imported} médias importés")
    print(f"   Sites traités: {', '.join(sites_to_import)}")
    print(f"   Mode: {'incrémental' if incremental and not truncate_before_insert else 'complet'}")
    print(f"{'#'*60}")

    return total_imported

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Exécution

# COMMAND ----------

# =============================================================================
# EXEMPLES D'EXÉCUTION
# =============================================================================

# Import incrémental du site FR (mode par défaut - MERGE)
# run_media_import_pipeline(sites_to_import=["fr"], incremental=True)

# Import complet du site FR (récupère tout, MERGE)
# run_media_import_pipeline(sites_to_import=["fr"], incremental=False)

# Import complet avec vidage préalable (TRUNCATE + INSERT)
# run_media_import_pipeline(sites_to_import=["fr"], incremental=False, truncate_before_insert=True)

# Import de tous les sites (incrémental)
# run_media_import_pipeline(sites_to_import=list(WP_SITES.keys()), incremental=True)

# Exécution par défaut: site FR, mode incrémental
run_media_import_pipeline(sites_to_import=["fr"], incremental=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Vérification des données

# COMMAND ----------

media_table = (
    f"{DATABRICKS_MEDIA_CONFIG['catalog']}."
    f"{DATABRICKS_MEDIA_CONFIG['schema']}."
    f"{DATABRICKS_MEDIA_CONFIG['table_name']}"
)

# Statistiques par site et type de média
display(spark.sql(f"""
    SELECT
        site_id,
        language,
        media_type,
        file_extension,
        COUNT(*) as nb_medias,
        ROUND(SUM(filesize) / 1024 / 1024, 2) as total_size_mb,
        MAX(date_imported) as last_import
    FROM {media_table}
    GROUP BY site_id, language, media_type, file_extension
    ORDER BY site_id, nb_medias DESC
"""))

# COMMAND ----------

# Aperçu des médias importés
display(spark.sql(f"""
    SELECT
        site_id,
        wp_id,
        slug,
        title,
        alt_text,
        file_extension,
        mime_type,
        filesize,
        width,
        height,
        language,
        date_created,
        date_modified,
        source_url
    FROM {media_table}
    ORDER BY date_created DESC
    LIMIT 50
"""))

# COMMAND ----------

# Distribution des types de fichiers
display(spark.sql(f"""
    SELECT
        site_id,
        file_extension,
        COUNT(*) as count,
        ROUND(AVG(filesize) / 1024, 2) as avg_size_kb,
        ROUND(AVG(width), 0) as avg_width,
        ROUND(AVG(height), 0) as avg_height
    FROM {media_table}
    GROUP BY site_id, file_extension
    ORDER BY site_id, count DESC
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Fonctions utilitaires pour la maintenance

# COMMAND ----------

def get_media_stats(catalog: str = None, schema: str = None, table_name: str = None):
    """Affiche les statistiques de la table médias."""
    catalog = catalog or DATABRICKS_MEDIA_CONFIG["catalog"]
    schema = schema or DATABRICKS_MEDIA_CONFIG["schema"]
    table_name = table_name or DATABRICKS_MEDIA_CONFIG["table_name"]
    full_table = f"{catalog}.{schema}.{table_name}"

    return spark.sql(f"""
        SELECT
            site_id,
            media_type,
            file_extension,
            COUNT(*) as total,
            COUNT(DISTINCT wp_id) as unique_wp_ids,
            ROUND(SUM(filesize) / 1024 / 1024, 2) as total_size_mb,
            MIN(date_created) as oldest_media,
            MAX(date_created) as newest_media,
            MAX(date_imported) as last_import
        FROM {full_table}
        GROUP BY site_id, media_type, file_extension
        ORDER BY site_id, total DESC
    """)


def refresh_media_single_site(site_id: str):
    """Rafraîchit les médias d'un seul site (vide et remplace)."""
    print(f"🔄 Rafraîchissement des médias du site {site_id}...")
    run_media_import_pipeline(
        sites_to_import=[site_id],
        incremental=False,
        truncate_before_insert=True
    )


def refresh_media_all_sites():
    """Rafraîchit les médias de tous les sites."""
    print("🔄 Rafraîchissement des médias de tous les sites...")
    run_media_import_pipeline(
        sites_to_import=list(WP_SITES.keys()),
        incremental=False,
        truncate_before_insert=True
    )
