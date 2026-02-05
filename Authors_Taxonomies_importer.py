# Databricks notebook source
# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import des Auteurs et Taxonomies WordPress
# MAGIC
# MAGIC Ce notebook permet de :
# MAGIC - Récupérer les auteurs (users) via l'API WordPress REST
# MAGIC - Récupérer les taxonomies custom (occupation, solution, secteur, etc.) via l'API WordPress REST
# MAGIC - Stocker les données dans les tables `cegid_website_taxonomy` et `cegid_website_authors`
# MAGIC - Vider et remplacer les données chaque semaine (mode TRUNCATE + INSERT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Chargement des utilitaires communs

# COMMAND ----------

# MAGIC %run ./wordpress_utils

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration spécifique

# COMMAND ----------

DATABRICKS_CONFIG = {
    "catalog": DATABRICKS_CATALOG,
    "schema": DATABRICKS_SCHEMA,
    "taxonomy_table_name": "cegid_website_taxonomy",
    "authors_table_name": "cegid_website_authors",
}

# Types de taxonomies à récupérer
# Note: "author" est traité via /wp-json/wp/v1/authors
TAXONOMY_TYPES = {
    "author": {
        "endpoint": "/authors",
        "api_endpoint": "/wp-json/wp/v1",
        "label": "Auteurs",
        "is_user": True  # Marqueur spécial pour les users
    },
    "occupation": {
        "endpoint": "/occupation",
        "api_endpoint": "/wp-json/wp/v2",
        "label": "Occupations/Métiers"
    },
    "category": {
        "endpoint": "/categories",
        "label": "Catégories"
    },
    "tag": {
        "endpoint": "/tags",
        "label": "Tags"
    },
    "solution": {
        "endpoint": "/solution",
        "label": "Solutions"
    },
    "secteur": {
        "endpoint": "/secteur",
        "label": "Secteurs"
    },
    "product_type": {
        "endpoint": "/product_type",
        "label": "Types de produits"
    }
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Imports complémentaires

# COMMAND ----------

import json
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    TimestampType, LongType, BooleanType
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Schéma de la table cegid_website_taxonomy

# COMMAND ----------

# Schéma unifié pour taxonomies
TAXONOMY_SCHEMA = StructType([
    # --- IDENTIFIANTS ---
    StructField("id", LongType(), False),              # ID composite unique
    StructField("wp_id", IntegerType(), False),        # ID WordPress original
    StructField("site_id", StringType(), False),       # Identifiant du site (fr, es, uk, etc.)
    StructField("taxonomy", StringType(), False),      # Type: author, occupation, category, tag, etc.

    # --- INFORMATIONS PRINCIPALES ---
    StructField("title", StringType(), True),          # Nom (name pour taxonomy, display_name pour user)
    StructField("slug", StringType(), True),           # Slug URL-friendly
    StructField("description", StringType(), True),    # Description (si disponible)

    # --- MÉTADONNÉES TAXONOMIE ---
    StructField("url", StringType(), True),            # URL du terme

    # --- HIÉRARCHIE (pour taxonomies hiérarchiques) ---
    StructField("parent_id", IntegerType(), True),     # ID du parent (categories)
    StructField("count", IntegerType(), True),         # Nombre d'éléments associés

    # --- LANGUE ---
    StructField("language", StringType(), True),       # Code langue du site

    # --- DATES ---
    StructField("date_imported", TimestampType(), False),  # Date d'import

    # --- DONNÉES BRUTES ---
    StructField("raw_json", StringType(), True),       # JSON complet de l'API
])

# COMMAND ----------

# Schéma dédié pour les auteurs
AUTHORS_SCHEMA = StructType([
    StructField("id", LongType(), False),              # ID composite unique
    StructField("wp_id", IntegerType(), False),        # ID WordPress original
    StructField("site_id", StringType(), False),       # Identifiant du site (fr, es, uk, etc.)
    StructField("name", StringType(), True),           # Nom affiché
    StructField("slug", StringType(), True),           # Slug URL-friendly
    StructField("email", StringType(), True),          # Email
    StructField("job", StringType(), True),            # Poste/fonction
    StructField("bio", StringType(), True),            # Biographie
    StructField("photo", StringType(), True),          # URL de la photo
    StructField("linkedin_url", StringType(), True),   # URL LinkedIn
    StructField("language", StringType(), True),       # Code langue du site
    StructField("date_imported", TimestampType(), False),  # Date d'import
    StructField("raw_json", StringType(), True),       # JSON complet de l'API
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fonctions utilitaires

# COMMAND ----------

def calculate_taxonomy_id(wp_id: int, taxonomy: str, site_id: str) -> int:
    """
    Calcule un ID composite unique pour les taxonomies/auteurs.

    Structure: SITE_OFFSET + TAXONOMY_OFFSET + wp_id

    Offset par site (milliards):
    - fr: 1_000_000_000
    - es: 2_000_000_000
    - etc.

    Offset par type de taxonomy (centaines de millions):
    - author: 0
    - occupation: 100_000_000
    - category: 200_000_000
    - tag: 300_000_000
    - solution: 400_000_000
    - secteur: 500_000_000
    - product_type: 600_000_000
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

    TAXONOMY_OFFSETS = {
        "author": 0,
        "occupation": 100_000_000,
        "category": 200_000_000,
        "tag": 300_000_000,
        "solution": 400_000_000,
        "secteur": 500_000_000,
        "product_type": 600_000_000,
    }

    site_offset = SITE_OFFSETS.get(site_id, 9_000_000_000)
    taxonomy_offset = TAXONOMY_OFFSETS.get(taxonomy, 900_000_000)

    return site_offset + taxonomy_offset + wp_id

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Classe du connecteur WordPress pour Taxonomies

# COMMAND ----------

class WordPressTaxonomyConnector:
    """
    Connecteur WordPress pour récupérer les auteurs et taxonomies.
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

    def _get_api_url(self, endpoint: str, api_endpoint: Optional[str] = None) -> str:
        """Construit l'URL complète de l'API pour ce site."""
        api_root = api_endpoint or self.api_endpoint
        return f"{self._get_site_url()}{api_root}{endpoint}"

    def _fetch_page(
        self,
        endpoint: str,
        page: int,
        params: Dict = None,
        api_endpoint: Optional[str] = None,
    ) -> Tuple[List[Dict], int]:
        """
        Récupère une page de résultats de l'API WordPress.
        """
        url = self._get_api_url(endpoint, api_endpoint=api_endpoint)

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
                # Taxonomy n'existe pas sur ce site
                print(f"⚠️ Endpoint {endpoint} non disponible sur ce site")
                return [], 0
            print(f"❌ Erreur HTTP {e.response.status_code}: {e}")
            return [], 0
        except requests.exceptions.RequestException as e:
            print(f"❌ Erreur API WordPress: {e}")
            return [], 0

    def fetch_all_items(
        self,
        taxonomy: str,
        endpoint: str,
        is_user: bool = False,
        api_endpoint: Optional[str] = None,
    ) -> List[Dict]:
        """
        Récupère tous les éléments d'une taxonomie ou tous les users.
        """
        all_items = []
        page = 1
        total_pages = 1

        params = {}
        if is_user:
            # Pour les users, on peut filtrer par contexte
            params["context"] = "edit"  # Donne plus d'infos si on a les droits
        else:
            # Pour les taxonomies, on peut demander le count
            params["hide_empty"] = "false"  # Inclut les termes sans posts

        site_label = self.site_config.get("label", self.site_id)
        print(f"📥 [{site_label}] Récupération des {taxonomy}...")
        print(f"   URL: {self._get_api_url(endpoint, api_endpoint=api_endpoint)}")

        while page <= total_pages:
            items, total_pages = self._fetch_page(endpoint, page, params, api_endpoint=api_endpoint)

            if not items:
                break

            all_items.extend(items)
            print(f"   Page {page}/{total_pages} - {len(items)} items récupérés")
            page += 1

        print(f"✅ [{site_label}] Total {taxonomy}: {len(all_items)}")
        return all_items

    def transform_user(self, item: Dict) -> Dict:
        """
        Transforme un user WordPress en format standardisé.

        Les champs job, bio, linkedin_url et photo sont extraits depuis
        l'objet ACF de la réponse API (item.acf.*), avec fallback sur
        les champs top-level pour rétrocompatibilité.
        """
        wp_id = item.get('id')

        # Photo: priorité ACF photo (ID média), puis avatar_urls
        acf_photo = get_nested_value(item, "acf.photo")
        if acf_photo and isinstance(acf_photo, str):
            photo_url = acf_photo
        elif acf_photo and isinstance(acf_photo, int):
            # ACF photo est un ID média WordPress, on le stocke comme string
            photo_url = str(acf_photo)
        else:
            avatar_urls = item.get('avatar_urls', {})
            photo_url = avatar_urls.get('96') or avatar_urls.get('48') or avatar_urls.get('24')

        # ACF fields avec fallback top-level
        linkedin_url = (get_nested_value(item, "acf.linkedin_url")
                        or get_nested_value(item, "linkedin_url")
                        or get_nested_value(item, "linkedin"))
        job = (get_nested_value(item, "acf.job")
               or get_nested_value(item, "job")
               or get_nested_value(item, "position"))
        bio = (get_nested_value(item, "acf.bio")
               or item.get('description', ''))

        return {
            "id": calculate_taxonomy_id(wp_id, "author", self.site_id),
            "wp_id": wp_id,
            "site_id": self.site_id,
            "name": item.get('name') or item.get('display_name', ''),
            "slug": item.get('slug'),
            "email": item.get('email'),  # Peut être null selon les permissions
            "job": job,
            "bio": bio,
            "photo": photo_url,
            "linkedin_url": linkedin_url,
            "language": self.site_config.get("language", "fr"),
            "date_imported": datetime.now(),
            "raw_json": json.dumps(item, ensure_ascii=False),
        }

    def transform_taxonomy(self, item: Dict, taxonomy: str) -> Dict:
        """
        Transforme un terme de taxonomie WordPress en format standardisé.
        """
        wp_id = item.get('id')

        return {
            "id": calculate_taxonomy_id(wp_id, taxonomy, self.site_id),
            "wp_id": wp_id,
            "site_id": self.site_id,
            "taxonomy": taxonomy,
            "title": item.get('name', ''),
            "slug": item.get('slug'),
            "description": item.get('description', ''),
            "url": item.get('link'),
            "parent_id": item.get('parent'),  # 0 si pas de parent
            "count": item.get('count'),  # Nombre de posts avec ce terme
            "language": self.site_config.get("language", "fr"),
            "date_imported": datetime.now(),
            "raw_json": json.dumps(item, ensure_ascii=False),
        }

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Fonctions de gestion de la table Databricks

# COMMAND ----------

def create_taxonomy_table_if_not_exists(catalog: str, schema: str, table_name: str):
    """Crée la table taxonomy si elle n'existe pas."""

    full_table_name = f"{catalog}.{schema}.{table_name}"

    # Crée le schéma si nécessaire
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    # Vérifie si la table existe
    if not spark.catalog.tableExists(full_table_name):
        print(f"📝 Création de la table {full_table_name}...")

        # Crée une DataFrame vide avec le schéma
        empty_df = spark.createDataFrame([], TAXONOMY_SCHEMA)

        # Écrit en Delta avec partitionnement par site et taxonomy
        empty_df.write \
            .format("delta") \
            .partitionBy("site_id", "taxonomy") \
            .option("delta.enableChangeDataFeed", "true") \
            .saveAsTable(full_table_name)

        print(f"✅ Table {full_table_name} créée avec succès")
    else:
        print(f"ℹ️ Table {full_table_name} existe déjà")


def create_authors_table_if_not_exists(catalog: str, schema: str, table_name: str):
    """Crée la table auteurs si elle n'existe pas."""

    full_table_name = f"{catalog}.{schema}.{table_name}"

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")

    if not spark.catalog.tableExists(full_table_name):
        print(f"📝 Création de la table {full_table_name}...")

        empty_df = spark.createDataFrame([], AUTHORS_SCHEMA)

        empty_df.write \
            .format("delta") \
            .partitionBy("site_id") \
            .option("delta.enableChangeDataFeed", "true") \
            .saveAsTable(full_table_name)

        print(f"✅ Table {full_table_name} créée avec succès")
    else:
        print(f"ℹ️ Table {full_table_name} existe déjà")


def truncate_taxonomy_data(catalog: str, schema: str, table_name: str,
                           site_id: str = None, taxonomy: str = None):
    """
    Vide les données de la table (ou une partition spécifique).

    Args:
        site_id: Si spécifié, ne supprime que ce site
        taxonomy: Si spécifié, ne supprime que cette taxonomy
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    conditions = []
    if site_id:
        conditions.append(f"site_id = '{site_id}'")
    if taxonomy:
        conditions.append(f"taxonomy = '{taxonomy}'")

    if conditions:
        where_clause = " AND ".join(conditions)
        print(f"🗑️ Suppression des données: {where_clause}")
        spark.sql(f"DELETE FROM {full_table_name} WHERE {where_clause}")
    else:
        print(f"🗑️ Vidage complet de la table {full_table_name}")
        spark.sql(f"TRUNCATE TABLE {full_table_name}")

    print("✅ Suppression terminée")


def truncate_authors_data(catalog: str, schema: str, table_name: str, site_id: str = None):
    """
    Vide les données de la table auteurs (ou une partition spécifique).
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    if site_id:
        print(f"🗑️ Suppression des données: site_id = '{site_id}'")
        spark.sql(f"DELETE FROM {full_table_name} WHERE site_id = '{site_id}'")
    else:
        print(f"🗑️ Vidage complet de la table {full_table_name}")
        spark.sql(f"TRUNCATE TABLE {full_table_name}")

    print("✅ Suppression terminée")


def insert_taxonomy_data(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Insère les données dans la table (après truncate).
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(full_table_name)

    print(f"✅ Insertion terminée dans {full_table_name}")


def insert_authors_data(df: DataFrame, catalog: str, schema: str, table_name: str):
    """
    Insère les données dans la table auteurs.
    """
    full_table_name = f"{catalog}.{schema}.{table_name}"

    df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(full_table_name)

    print(f"✅ Insertion terminée dans {full_table_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pipeline principal

# COMMAND ----------

def run_taxonomy_import_pipeline(
    taxonomy_types: Dict = TAXONOMY_TYPES,
    sites_to_import: List[str] = WP_SITES_TO_IMPORT,
    specific_taxonomy: Optional[str] = None,
    truncate_before_insert: bool = True
):
    """
    Exécute le pipeline d'import des auteurs et taxonomies.

    Ce pipeline est conçu pour être exécuté chaque semaine et remplacer
    intégralement les données existantes (mode TRUNCATE + INSERT).

    Args:
        taxonomy_types: Dictionnaire des types de taxonomies à importer
        sites_to_import: Liste des site_id à importer (ex: ["fr", "es"])
        specific_taxonomy: Si spécifié, importe seulement cette taxonomy
        truncate_before_insert: Si True, vide les données avant insertion (défaut: True)
    """

    catalog = DATABRICKS_CONFIG["catalog"]
    schema = DATABRICKS_CONFIG["schema"]
    taxonomy_table_name = DATABRICKS_CONFIG["taxonomy_table_name"]
    authors_table_name = DATABRICKS_CONFIG["authors_table_name"]

    # Crée la table si nécessaire
    create_taxonomy_table_if_not_exists(catalog, schema, taxonomy_table_name)
    create_authors_table_if_not_exists(catalog, schema, authors_table_name)

    # Filtre les taxonomies si spécifié
    types_to_import = {specific_taxonomy: taxonomy_types[specific_taxonomy]} if specific_taxonomy else taxonomy_types

    total_imported = 0
    all_taxonomy_items = []
    all_author_items = []

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
        connector = WordPressTaxonomyConnector(site_id, site_config)

        for taxonomy, config in types_to_import.items():
            print(f"\n{'='*50}")
            print(f"📦 [{site_label}] Import: {config['label']} ({taxonomy})")
            print(f"{'='*50}")

            is_user = config.get("is_user", False)
            api_endpoint = config.get("api_endpoint")

            # Récupère les éléments
            items = connector.fetch_all_items(
                taxonomy=taxonomy,
                endpoint=config["endpoint"],
                is_user=is_user,
                api_endpoint=api_endpoint
            )

            if not items:
                print(f"ℹ️ Aucun {taxonomy} trouvé")
                continue

            # Transforme les items
            if is_user:
                transformed_items = [connector.transform_user(item) for item in items]
                all_author_items.extend(transformed_items)
            else:
                transformed_items = [connector.transform_taxonomy(item, taxonomy) for item in items]
                all_taxonomy_items.extend(transformed_items)

            total_imported += len(transformed_items)
            print(f"📊 [{site_label}] {len(transformed_items)} {taxonomy}(s) préparé(s)")

    # Insertion des données
    if all_taxonomy_items:
        print(f"\n{'='*60}")
        print(f"💾 INSERTION EN BASE DE DONNÉES (TAXONOMIES)")
        print(f"{'='*60}")

        df_taxonomy = spark.createDataFrame(all_taxonomy_items, TAXONOMY_SCHEMA)

        if truncate_before_insert:
            if specific_taxonomy and specific_taxonomy != "author":
                for site_id in sites_to_import:
                    truncate_taxonomy_data(catalog, schema, taxonomy_table_name, site_id, specific_taxonomy)
            else:
                for site_id in sites_to_import:
                    truncate_taxonomy_data(catalog, schema, taxonomy_table_name, site_id)

        insert_taxonomy_data(df_taxonomy, catalog, schema, taxonomy_table_name)

    if all_author_items:
        print(f"\n{'='*60}")
        print(f"💾 INSERTION EN BASE DE DONNÉES (AUTEURS)")
        print(f"{'='*60}")

        df_authors = spark.createDataFrame(all_author_items, AUTHORS_SCHEMA)

        if truncate_before_insert and (specific_taxonomy in (None, "author")):
            for site_id in sites_to_import:
                truncate_authors_data(catalog, schema, authors_table_name, site_id)

        insert_authors_data(df_authors, catalog, schema, authors_table_name)

    print(f"\n{'#'*60}")
    print(f"🎉 Import terminé! Total: {total_imported} éléments importés")
    print(f"   Sites traités: {', '.join(sites_to_import)}")
    print(f"   Taxonomies: {', '.join(types_to_import.keys())}")
    print(f"{'#'*60}")

    return total_imported

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Exécution

# COMMAND ----------

# =============================================================================
# EXEMPLES D'EXÉCUTION
# =============================================================================

# Import de tous les auteurs et taxonomies pour le site FR (vide et remplace)
# run_taxonomy_import_pipeline(sites_to_import=["fr"], truncate_before_insert=True)

# Import uniquement des auteurs pour tous les sites
# run_taxonomy_import_pipeline(specific_taxonomy="author", sites_to_import=list(WP_SITES.keys()))

# Import uniquement des occupations pour le site FR
# run_taxonomy_import_pipeline(specific_taxonomy="occupation", sites_to_import=["fr"])

# Import sans vidage préalable (ajoute aux données existantes)
# run_taxonomy_import_pipeline(sites_to_import=["fr"], truncate_before_insert=False)

# Import de tous les sites et toutes les taxonomies
# run_taxonomy_import_pipeline(sites_to_import=list(WP_SITES.keys()))

# Exécution par défaut: site FR, toutes taxonomies, mode TRUNCATE + INSERT
run_taxonomy_import_pipeline(sites_to_import=["fr"], truncate_before_insert=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Vérification des données

# COMMAND ----------

# Affiche un aperçu des données importées par site et taxonomy
taxonomy_table = (
    f"{DATABRICKS_CONFIG['catalog']}."
    f"{DATABRICKS_CONFIG['schema']}."
    f"{DATABRICKS_CONFIG['taxonomy_table_name']}"
)
authors_table = (
    f"{DATABRICKS_CONFIG['catalog']}."
    f"{DATABRICKS_CONFIG['schema']}."
    f"{DATABRICKS_CONFIG['authors_table_name']}"
)

display(spark.sql(f"""
    SELECT
        site_id,
        taxonomy,
        language,
        COUNT(*) as nb_items,
        MAX(date_imported) as last_import
    FROM {taxonomy_table}
    GROUP BY site_id, taxonomy, language
    ORDER BY site_id, taxonomy
"""))

# COMMAND ----------

# Aperçu des auteurs
display(spark.sql(f"""
    SELECT
        site_id,
        wp_id,
        name,
        slug,
        email,
        job,
        bio,
        photo,
        linkedin_url
    FROM {authors_table}
    ORDER BY site_id, name
    LIMIT 50
"""))

# COMMAND ----------

# Aperçu des occupations
display(spark.sql(f"""
    SELECT
        site_id,
        wp_id,
        title as name,
        slug,
        description,
        count as nb_posts,
        parent_id
    FROM {taxonomy_table}
    WHERE taxonomy = 'occupation'
    ORDER BY site_id, count DESC
    LIMIT 50
"""))

# COMMAND ----------

# Aperçu de toutes les taxonomies (hors auteurs)
display(spark.sql(f"""
    SELECT
        site_id,
        taxonomy,
        wp_id,
        title as name,
        slug,
        count as nb_posts
    FROM {taxonomy_table}
    WHERE taxonomy != 'author'
    ORDER BY site_id, taxonomy, count DESC
    LIMIT 100
"""))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Fonctions utilitaires pour la maintenance

# COMMAND ----------

def get_taxonomy_stats(catalog: str = None, schema: str = None, table_name: str = None):
    """Affiche les statistiques de la table taxonomy."""
    catalog = catalog or DATABRICKS_CONFIG["catalog"]
    schema = schema or DATABRICKS_CONFIG["schema"]
    table_name = table_name or DATABRICKS_CONFIG["taxonomy_table_name"]
    full_table = f"{catalog}.{schema}.{table_name}"

    return spark.sql(f"""
        SELECT
            site_id,
            taxonomy,
            COUNT(*) as total,
            COUNT(DISTINCT wp_id) as unique_wp_ids,
            MIN(date_imported) as first_import,
            MAX(date_imported) as last_import
        FROM {full_table}
        GROUP BY site_id, taxonomy
        ORDER BY site_id, taxonomy
    """)


def refresh_single_site(site_id: str):
    """Rafraîchit les données d'un seul site (vide et remplace)."""
    print(f"🔄 Rafraîchissement du site {site_id}...")
    run_taxonomy_import_pipeline(
        sites_to_import=[site_id],
        truncate_before_insert=True
    )


def refresh_all_sites():
    """Rafraîchit les données de tous les sites (exécution hebdomadaire)."""
    print("🔄 Rafraîchissement de tous les sites...")
    run_taxonomy_import_pipeline(
        sites_to_import=list(WP_SITES.keys()),
        truncate_before_insert=True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Scheduling (pour exécution hebdomadaire)
# MAGIC
# MAGIC Pour planifier l'exécution hebdomadaire de ce notebook dans Databricks:
# MAGIC
# MAGIC 1. **Via Workflows UI:**
# MAGIC    - Aller dans Workflows > Create Job
# MAGIC    - Ajouter ce notebook comme tâche
# MAGIC    - Configurer le schedule: `0 0 * * 0` (chaque dimanche à minuit)
# MAGIC
# MAGIC 2. **Via Databricks CLI:**
# MAGIC ```bash
# MAGIC databricks jobs create --json '{
# MAGIC   "name": "Weekly Authors & Taxonomies Import",
# MAGIC   "tasks": [{
# MAGIC     "task_key": "import_taxonomies",
# MAGIC     "notebook_task": {
# MAGIC       "notebook_path": "/path/to/Authors_Taxonomies_importer"
# MAGIC     }
# MAGIC   }],
# MAGIC   "schedule": {
# MAGIC     "quartz_cron_expression": "0 0 0 ? * SUN",
# MAGIC     "timezone_id": "Europe/Paris"
# MAGIC   }
# MAGIC }'
# MAGIC ```
# MAGIC
# MAGIC 3. **Appel manuel:**
# MAGIC ```python
# MAGIC # Pour rafraîchir tous les sites manuellement
# MAGIC refresh_all_sites()
# MAGIC ```
