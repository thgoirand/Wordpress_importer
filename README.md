# WordPress Importer — Cegid

Pipeline d'import de contenus WordPress vers Databricks (Delta Lake). Le projet extrait les données de 7 sites WordPress multilingues de cegid.com et les charge dans des tables Delta pour analyse et traitement.

## Sites supportés

| Clé  | URL                         | Langue     |
|------|-----------------------------|------------|
| `fr` | https://www.cegid.com/fr    | Français   |
| `es` | https://www.cegid.com/es    | Espagnol   |
| `uk` | https://www.cegid.com/uk    | Anglais UK |
| `us` | https://www.cegid.com/us    | Anglais US |
| `de` | https://www.cegid.com/de    | Allemand   |
| `it` | https://www.cegid.com/it    | Italien    |
| `pt` | https://www.cegid.com/pt    | Portugais  |

## Architecture

```
WordPress REST API (/wp-json/wp/v2)
    │  (requêtes paginées, Basic Auth)
    ▼
WordPressConnector (wordpress_utils.py)
    │  (transformation en dictionnaires Python)
    ▼
PySpark DataFrames
    │  (MERGE / INSERT)
    ▼
Databricks Delta Tables (gdp_cdt_dev_04_gld.sandbox_mkt)
    │  (optionnel)
    ▼
AI Formatting (Claude Haiku via Databricks AI Functions)
```

## Tables cibles

| Table                        | Suffixe | Contenu                                                    |
|------------------------------|---------|-------------------------------------------------------------|
| `cegid_website_blog_slv`     | SLV     | Données brutes blog (ex-bronze)                             |
| `cegid_website_plt`          | PLT     | Articles, landing pages, produits, études de cas (extraction markdown) |
| `cegid_website_gld`          | GLD     | Table finale enrichie par les fonctions IA                  |
| `cegid_website_authors`      |         | Auteurs WordPress avec métadonnées ACF                      |
| `cegid_website_taxonomy`     |         | Catégories, tags, taxonomies personnalisées                 |
| `cegid_website_medias`       |         | Médias (images, documents, etc.)                            |

## Structure du projet

```
├── wordpress_utils.py                 # Utilitaires partagés et configuration
├── blog_importer.py                   # Import des articles de blog
├── Authors_Taxonomies_importer.py     # Import des auteurs et taxonomies
├── landing_page_importer.py           # Import des landing pages
├── product_importer.py                # Import des produits
├── study_case_importer.py             # Import des études de cas
├── media_importer.py                  # Import des médias
├── ai_formatter_blog.py               # Formatage IA des articles de blog
├── ai_formatter_landing_page.py       # Formatage IA des landing pages
├── ai_formatter_product.py            # Formatage IA des produits
└── ai_formatter_use_case.py           # Formatage IA des use cases + key figures
```

## Description des notebooks

### wordpress_utils.py — Utilitaires partagés

Module central importé par tous les autres notebooks. Contient :

- **Configuration globale** : identifiants WordPress, définition des 7 sites, catalogue/schéma Databricks par défaut.
- **`clean_html_content(html)`** : extrait le texte brut depuis du HTML via BeautifulSoup.
- **`get_nested_value(data, key)`** : accède à des valeurs imbriquées par notation pointée (ex. `"title.rendered"`).
- **`parse_wp_date(date_str)`** : parse les dates ISO 8601 de WordPress.
- **`calculate_composite_id(site_id, content_type, wp_id)`** : génère un ID unique combinant site, type de contenu et ID WordPress pour éviter les collisions entre sites.
- **`WordPressConnector`** : classe de connexion à l'API REST WordPress avec gestion des sessions HTTP, authentification Basic Auth, et récupération paginée des contenus. Supporte le filtrage par date de modification pour l'import incrémental.
- **`create_delta_table()`** / **`get_last_modified_date()`** : fonctions de gestion des tables Delta (création, lecture de la dernière date importée).

**Stratégie de composition d'ID :**

Les IDs sont composés hiérarchiquement pour garantir l'unicité :
- Offset site (milliards) : FR=1B, ES=2B, UK=3B, US=4B, DE=5B, IT=6B, PT=7B
- Offset type (millions) : post=0M, landing_page=20M, product=30M, study_case=60M, taxonomy=100M–600M, media=700M

---

### blog_importer.py — Import des articles de blog

**Endpoint API** : `/wp-json/wp/v2/posts`
**Type de contenu** : `post`
**Table cible** : `cegid_website_plt`

Importe les articles de blog avec :
- Métadonnées SEO (meta_description, meta_title, noindex) via Yoast
- Contenu HTML brut et texte nettoyé
- Taxonomies : catégories, tags, occupation, solution, secteur, product_type
- Image à la une depuis les données embarquées (`_embedded`)
- Support de l'import incrémental (MERGE/upsert sur les articles modifiés)

**Utilisation :**

```python
# Import incrémental d'un site
run_import_pipeline(sites_to_import=["fr"], incremental=True)

# Import complet de tous les sites
run_import_pipeline(sites_to_import=list(WP_SITES.keys()), incremental=False)
```

---

### Authors_Taxonomies_importer.py — Import des auteurs et taxonomies

**Endpoints API** : `/wp-json/wp/v1/authors`, `/wp-json/wp/v2/{taxonomy}`
**Tables cibles** : `cegid_website_authors`, `cegid_website_taxonomy`

Importe deux types de données :

**Auteurs** — avec les champs ACF : poste, bio, URL LinkedIn, photo.

**Taxonomies** — 7 types supportés : author, occupation, category, tag, solution, secteur, product_type. Gère la hiérarchie (parent_id).

**Stratégie de rafraîchissement** : TRUNCATE + INSERT hebdomadaire (remplacement complet des données).

```python
# Import de tous les auteurs et taxonomies pour un site
run_import_pipeline(sites_to_import=["fr"])

# Import de toutes les taxonomies pour tous les sites
run_import_pipeline(sites_to_import=list(WP_SITES.keys()))
```

---

### landing_page_importer.py — Import des landing pages

**Endpoint API** : `/wp-json/wp/v2/landing-page`
**Type de contenu** : `landing_page`
**Table cible** : `cegid_website_plt`

Particularité : le contenu est assemblé à partir des champs flexibles ACF (`vah_flexible_header` et `vah_flexible_main`) plutôt que du `content.rendered` standard. Supporte les blocs de type `builder-grid` (grilles de contenu) et `arguments-content`.

```python
run_import_pipeline(sites_to_import=["fr"], incremental=True)
```

---

### product_importer.py — Import des produits

**Endpoint API** : `/wp-json/wp/v2/product`
**Type de contenu** : `product`
**Table cible** : `cegid_website_plt`

Extrait le contenu produit depuis les blocs flexibles ACF : header, arguments et features. L'image à la une provient de la balise OG image (plus fiable que les données embarquées). Supporte les taxonomies personnalisées (occupation, solution, secteur, product_type).

```python
run_import_pipeline(sites_to_import=["fr"], incremental=True)
```

---

### study_case_importer.py — Import des études de cas

**Endpoint API** : `/wp-json/wp/v2/study-case`
**Type de contenu** : `study_case`
**Table cible** : `cegid_website_plt`

Extrait le contenu depuis les blocs `wysiwyg-section` de `vah_flexible_main`. Les blocs structurés (testimonial-cards, product-section) sont ignorés pour l'extraction texte mais conservés dans le JSON brut. Taxonomies personnalisées : occupation, company, solution, secteur, product_type.

```python
run_import_pipeline(sites_to_import=["fr"], incremental=True)
```

---

### media_importer.py — Import des médias

**Endpoint API** : `/wp-json/wp/v2/media`
**Table cible** : `cegid_website_medias`

Importe les pièces jointes avec métadonnées complètes : dimensions, taille, type MIME, extension, alt text, client associé, taxonomy_company_id. Supporte la migration de schéma (ajout automatique de colonnes manquantes).

```python
# Import incrémental
run_import_pipeline(sites_to_import=["fr"], incremental=True)

# Import complet avec troncature
run_import_pipeline(sites_to_import=["fr"], incremental=False, truncate_first=True)
```

---

### ai_formatter_blog.py — Formatage IA des articles de blog

**Modèle** : Claude Haiku via Databricks AI Functions (`AI_QUERY`)
**Scope** : Articles de blog (`post`)

Convertit le JSON brut des articles en markdown structuré :

1. Identifie les articles non formatés ou récemment modifiés (colonne `date_formatted`).
2. Découpe en lots (par défaut 5 éléments).
3. Appelle Claude Haiku pour chaque lot via `AI_QUERY`.
4. Met à jour `content_text` et `date_formatted` via MERGE.

```python
total_processed = run_ai_formatting()
```

---

### ai_formatter_landing_page.py — Formatage IA des landing pages

**Modèle** : Claude Haiku via Databricks AI Functions (`AI_QUERY`)
**Scope** : Landing pages (`landing_page`)

Convertit le JSON brut des landing pages (blocs ACF : header, grilles, arguments) en markdown structuré. Même processus par batch que le formatter blog.

```python
total_processed = run_ai_formatting()
```

---

### ai_formatter_product.py — Formatage IA des produits

**Modèle** : Claude Haiku via Databricks AI Functions (`AI_QUERY`)
**Scope** : Produits (`product`)

Convertit le JSON brut des pages produit (blocs ACF : header, arguments produit, features) en markdown structuré. Même processus par batch que le formatter blog.

```python
total_processed = run_ai_formatting()
```

---

### ai_formatter_use_case.py — Formatage IA des études de cas + key figures

**Modèle** : Claude Haiku via Databricks AI Functions (`AI_QUERY`)
**Scope** : Études de cas (`study_case`)

Post-traitement spécifique aux use cases qui :

1. Identifie les study cases non formatés ou récemment modifiés.
2. Ajoute automatiquement la colonne `key_figures` (ARRAY<STRING>) si absente.
3. Découpe en lots (par défaut 5 éléments).
4. Appelle Claude Haiku deux fois par lot via `AI_QUERY` :
   - Génération du contenu markdown (`content_text`)
   - Extraction des chiffres clés (`key_figures`) : pourcentages, gains, ROI, etc.
5. Met à jour `content_text`, `key_figures` et `date_formatted` via MERGE.

```python
total_processed = run_ai_formatting()
```

## Modes d'import

### Incrémental (par défaut)

Filtre les contenus modifiés depuis le dernier import via le paramètre `modified_after` de l'API WordPress. Utilise MERGE (upsert) : mise à jour si existant, insertion sinon.

### Complet

Récupère tous les contenus sans filtre de date. Peut optionnellement tronquer la table avant insertion.

## Dépendances

- `requests` — client HTTP
- `beautifulsoup4` — parsing HTML
- `pyspark` — DataFrames et SQL (Databricks)
- `json`, `datetime`, `html`, `typing`, `os` — bibliothèques standard

## Planification

Les notebooks sont conçus pour être exécutés en tant que **Databricks Workflows** :
- **Import incrémental** : quotidien
- **Auteurs & taxonomies** : hebdomadaire (TRUNCATE + INSERT)
- **Formatage IA blog** : après chaque import d'articles
- **Formatage IA landing pages** : après chaque import de landing pages
- **Formatage IA produits** : après chaque import de produits
- **Formatage IA use cases** : après chaque import de study cases (contenu + key figures)
