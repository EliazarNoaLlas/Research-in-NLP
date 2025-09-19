import asyncio
import os

import aiohttp
import pandas as pd
import spacy
import json
import re
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from transformers import AutoTokenizer, AutoModel
import torch
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, FOAF, DC, DCTERMS
import hashlib
from fuzzywuzzy import fuzz
import nltk
from nltk.corpus import stopwords
import requests_cache
from tenacity import retry, stop_after_attempt, wait_exponential

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Descargar recursos NLTK necesarios
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    nltk.download('stopwords')


@dataclass
class Article:
    """Estructura de datos para artículos"""
    title: str
    url: str
    pmcid: Optional[str] = None
    pmid: Optional[str] = None
    doi: Optional[str] = None
    authors: List[Dict] = None
    affiliations: List[str] = None
    journal: Optional[str] = None
    publication_date: Optional[datetime] = None
    abstract: Optional[str] = None
    full_text: Optional[str] = None
    keywords: List[str] = None
    mesh_terms: List[str] = None
    organisms: List[str] = None
    genes: List[str] = None
    proteins: List[str] = None
    conditions: List[str] = None
    experiments: List[str] = None
    measurements: List[str] = None
    quality_score: float = 0.0
    extraction_timestamp: datetime = None

    def __post_init__(self):
        if self.authors is None:
            self.authors = []
        if self.affiliations is None:
            self.affiliations = []
        if self.keywords is None:
            self.keywords = []
        if self.mesh_terms is None:
            self.mesh_terms = []
        if self.organisms is None:
            self.organisms = []
        if self.genes is None:
            self.genes = []
        if self.proteins is None:
            self.proteins = []
        if self.conditions is None:
            self.conditions = []
        if self.experiments is None:
            self.experiments = []
        if self.measurements is None:
            self.measurements = []
        if self.extraction_timestamp is None:
            self.extraction_timestamp = datetime.now()


class DataIngestionPipeline:
    """Pipeline completo de ingesta de datos científicos"""

    def __init__(self):
        self.session = None
        self.processed_articles = []
        self.failed_extractions = []
        self.duplicate_detector = DuplicateDetector()

        # Cache para requests
        self.cached_session = requests_cache.CachedSession(
            'space_biology_cache',
            expire_after=3600  # 1 hora
        )

        # Headers para web scraping profesional
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    async def __aenter__(self):
        """Context manager para gestión de sesión async"""
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=60)
        self.session = aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=timeout
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup de sesión async"""
        if self.session:
            await self.session.close()

    async def load_sb_publications_csv(self, csv_path: str = None) -> List[Dict]:
        """Carga el CSV de SB Publications"""
        if csv_path is None:
            # Descargar desde GitHub si no se proporciona path local
            csv_url = "https://raw.githubusercontent.com/jgalazka/SB_publications/main/SB_publications_PMC.csv"
            try:
                async with self.session.get(csv_url) as response:
                    csv_content = await response.text()
                    # Guardar temporalmente para pandas
                    with open('temp_sb_publications.csv', 'w', encoding='utf-8') as f:
                        f.write(csv_content)
                    csv_path = 'temp_sb_publications.csv'
            except Exception as e:
                logger.error(f"Error descargando CSV: {e}")
                return []

        try:
            df = pd.read_csv(csv_path)
            articles_data = []

            for _, row in df.iterrows():
                article_data = {
                    'title': row.get('Title', ''),
                    'url': row.get('Link', ''),
                    'pmcid': self.extract_pmcid_from_url(row.get('Link', ''))
                }
                articles_data.append(article_data)

            logger.info(f"Cargados {len(articles_data)} artículos desde CSV")
            return articles_data

        except Exception as e:
            logger.error(f"Error cargando CSV: {e}")
            return []

    def extract_pmcid_from_url(self, url: str) -> Optional[str]:
        """Extrae PMCID de URL de PMC"""
        if 'pmc/articles/PMC' in url:
            match = re.search(r'PMC(\d+)', url)
            if match:
                return f"PMC{match.group(1)}"
        return None

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
    async def extract_papers(self, articles_data: List[Dict]) -> List[Article]:
        """Extracción profesional de artículos con reintentos"""
        extracted_articles = []
        semaphore = asyncio.Semaphore(5)  # Limitar concurrencia

        async def extract_single_article(article_data):
            async with semaphore:
                try:
                    article = await self.extract_single_paper(article_data)
                    if article and article.quality_score > 0.5:  # Solo artículos de calidad
                        return article
                except Exception as e:
                    logger.error(f"Error extrayendo artículo {article_data.get('title', 'Sin título')}: {e}")
                    self.failed_extractions.append(article_data)
                return None

        # Procesamiento concurrente
        tasks = [extract_single_article(data) for data in articles_data]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, Article):
                extracted_articles.append(result)

        logger.info(f"Extraídos {len(extracted_articles)} artículos exitosamente")
        logger.info(f"Fallaron {len(self.failed_extractions)} extracciones")

        return extracted_articles

    async def extract_single_paper(self, article_data: Dict) -> Optional[Article]:
        """Extrae un artículo individual con técnicas profesionales de scraping"""
        url = article_data.get('url', '')
        title = article_data.get('title', '')

        if not url:
            return None

        try:
            # Rate limiting
            await asyncio.sleep(0.5)

            async with self.session.get(url) as response:
                if response.status != 200:
                    logger.warning(f"HTTP {response.status} for {url}")
                    return None

                html_content = await response.text()
                soup = BeautifulSoup(html_content, 'html.parser')

                # Crear artículo base
                article = Article(title=title, url=url)

                # Extraer PMCID y PMID
                article.pmcid = self.extract_pmcid_from_soup(soup)
                article.pmid = self.extract_pmid_from_soup(soup)
                article.doi = self.extract_doi_from_soup(soup)

                # Extraer metadatos básicos
                article.journal = self.extract_journal(soup)
                article.publication_date = self.extract_publication_date(soup)
                article.abstract = self.extract_abstract(soup)
                article.full_text = self.extract_full_text(soup)

                # Extraer autores y afiliaciones
                article.authors, article.affiliations = self.extract_authors_and_affiliations(soup)

                # Extraer términos MeSH si están disponibles
                article.mesh_terms = self.extract_mesh_terms(soup)
                article.keywords = self.extract_keywords(soup)

                # Enriquecer con PubMed API si tenemos PMID
                if article.pmid:
                    await self.enrich_with_pubmed_api(article)

                # Calcular score de calidad
                article.quality_score = self.calculate_quality_score(article)

                return article

        except Exception as e:
            logger.error(f"Error extrayendo {url}: {e}")
            return None

    def extract_pmcid_from_soup(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae PMCID del HTML"""
        # Buscar en metadatos
        meta_pmcid = soup.find('meta', {'name': 'citation_pmid'})
        if meta_pmcid:
            return f"PMC{meta_pmcid.get('content', '')}"

        # Buscar en URL actual
        for link in soup.find_all('link', rel='canonical'):
            href = link.get('href', '')
            pmcid = self.extract_pmcid_from_url(href)
            if pmcid:
                return pmcid

        return None

    def extract_pmid_from_soup(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae PMID del HTML"""
        meta_pmid = soup.find('meta', {'name': 'citation_pmid'})
        if meta_pmid:
            return meta_pmid.get('content')

        # Buscar en enlaces a PubMed
        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'pubmed' in href.lower():
                match = re.search(r'pubmed/(\d+)', href)
                if match:
                    return match.group(1)

        return None

    def extract_doi_from_soup(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae DOI del HTML"""
        # Buscar en metadatos
        meta_doi = soup.find('meta', {'name': 'citation_doi'})
        if meta_doi:
            return meta_doi.get('content')

        # Buscar en texto
        doi_pattern = r'10\.\d{4,}[^\s]*'
        text = soup.get_text()
        doi_match = re.search(doi_pattern, text)
        if doi_match:
            return doi_match.group()

        return None

    def extract_journal(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae nombre del journal"""
        # Buscar en metadatos
        meta_journal = soup.find('meta', {'name': 'citation_journal_title'})
        if meta_journal:
            return meta_journal.get('content')

        # Buscar en header o title
        journal_selectors = [
            '.journal-title',
            '.journal-name',
            '[data-testid="journal-title"]',
            'h1.journal',
            '.site-header .journal'
        ]

        for selector in journal_selectors:
            element = soup.select_one(selector)
            if element:
                return element.get_text().strip()

        return None

    def extract_publication_date(self, soup: BeautifulSoup) -> Optional[datetime]:
        """Extrae fecha de publicación"""
        # Buscar en metadatos
        date_fields = ['citation_date', 'citation_publication_date', 'DC.Date']
        for field in date_fields:
            meta_date = soup.find('meta', {'name': field})
            if meta_date:
                date_str = meta_date.get('content')
                return self.parse_date_string(date_str)

        # Buscar en texto visible
        date_patterns = [
            r'Published[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4})',
            r'(\d{4}-\d{2}-\d{2})',
            r'([A-Za-z]+\s+\d{1,2},?\s+\d{4})'
        ]

        text = soup.get_text()
        for pattern in date_patterns:
            match = re.search(pattern, text)
            if match:
                return self.parse_date_string(match.group(1))

        return None

    def parse_date_string(self, date_str: str) -> Optional[datetime]:
        """Parse diferentes formatos de fecha"""
        date_formats = [
            '%Y-%m-%d',
            '%Y/%m/%d',
            '%B %d, %Y',
            '%b %d, %Y',
            '%d %B %Y',
            '%d %b %Y',
            '%Y'
        ]

        for fmt in date_formats:
            try:
                return datetime.strptime(date_str.strip(), fmt)
            except ValueError:
                continue

        return None

    def extract_abstract(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae abstract del artículo"""
        abstract_selectors = [
            '.abstract',
            '#abstract',
            '[data-testid="abstract"]',
            '.article-abstract',
            '.summary',
            'div.abstract p',
            'section[data-type="abstract"]'
        ]

        for selector in abstract_selectors:
            element = soup.select_one(selector)
            if element:
                # Limpiar texto
                abstract = element.get_text().strip()
                # Remover etiquetas comunes
                abstract = re.sub(r'^(Abstract|Summary|ABSTRACT)[:.]?\s*', '', abstract)
                if len(abstract) > 50:  # Mínimo razonable para un abstract
                    return abstract

        return None

    def extract_full_text(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae texto completo del artículo"""
        # Remover elementos no deseados
        for element in soup(['script', 'style', 'nav', 'header', 'footer', 'aside']):
            element.decompose()

        content_selectors = [
            '.article-body',
            '.content-body',
            '.main-content',
            '[data-testid="article-content"]',
            'article',
            '.paper-content',
            '.full-text'
        ]

        for selector in content_selectors:
            content_element = soup.select_one(selector)
            if content_element:
                text = content_element.get_text().strip()
                if len(text) > 500:  # Mínimo para texto completo
                    return self.clean_text(text)

        # Fallback: todo el texto de la página
        full_text = soup.get_text()
        if len(full_text) > 1000:
            return self.clean_text(full_text)

        return None

    def clean_text(self, text: str) -> str:
        """Limpia y normaliza texto extraído"""
        # Remover espacios múltiples
        text = re.sub(r'\s+', ' ', text)
        # Remover caracteres especiales problemáticos
        text = re.sub(r'[^\w\s\-.,;:()[\]{}"\']', ' ', text)
        # Normalizar espacios
        text = text.strip()
        return text

    def extract_authors_and_affiliations(self, soup: BeautifulSoup) -> Tuple[List[Dict], List[str]]:
        """Extrae autores y afiliaciones"""
        authors = []
        affiliations = set()

        # Buscar en metadatos structured
        author_metas = soup.find_all('meta', {'name': 'citation_author'})
        for meta in author_metas:
            author_name = meta.get('content', '')
            if author_name:
                authors.append({
                    'name': author_name,
                    'affiliation': None
                })

        # Buscar afiliaciones en metadatos
        affil_metas = soup.find_all('meta', {'name': 'citation_author_institution'})
        for i, meta in enumerate(affil_metas):
            affiliation = meta.get('content', '')
            if affiliation:
                affiliations.add(affiliation)
                # Asociar con autor si hay correspondencia
                if i < len(authors):
                    authors[i]['affiliation'] = affiliation

        # Si no hay metadatos, buscar en el HTML
        if not authors:
            authors = self.extract_authors_from_html(soup)
            html_affiliations = self.extract_affiliations_from_html(soup)
            affiliations.update(html_affiliations)

        return authors, list(affiliations)

    def extract_authors_from_html(self, soup: BeautifulSoup) -> List[Dict]:
        """Extrae autores del HTML cuando no hay metadatos"""
        authors = []

        author_selectors = [
            '.authors .author',
            '.author-list .author',
            '[data-testid="author"]',
            '.contrib-group .contrib'
        ]

        for selector in author_selectors:
            author_elements = soup.select(selector)
            for element in author_elements:
                name = element.get_text().strip()
                if name and len(name) > 2:
                    authors.append({
                        'name': name,
                        'affiliation': None
                    })
            if authors:  # Si encontramos autores, no seguir buscando
                break

        return authors

    def extract_affiliations_from_html(self, soup: BeautifulSoup) -> List[str]:
        """Extrae afiliaciones del HTML"""
        affiliations = []

        affiliation_selectors = [
            '.affiliation',
            '.author-affiliation',
            '[data-testid="affiliation"]',
            '.aff'
        ]

        for selector in affiliation_selectors:
            elements = soup.select(selector)
            for element in elements:
                affiliation = element.get_text().strip()
                if affiliation and len(affiliation) > 10:
                    affiliations.append(affiliation)

        return affiliations

    def extract_mesh_terms(self, soup: BeautifulSoup) -> List[str]:
        """Extrae términos MeSH si están disponibles"""
        mesh_terms = []

        # Buscar en metadatos
        mesh_metas = soup.find_all('meta', {'name': 'citation_mesh_term'})
        for meta in mesh_metas:
            term = meta.get('content', '')
            if term:
                mesh_terms.append(term)

        # Buscar en secciones específicas de MeSH
        mesh_selectors = [
            '.mesh-terms',
            '.keywords-mesh',
            '[data-testid="mesh-terms"]'
        ]

        for selector in mesh_selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text()
                # Parsear términos separados por comas/puntos y coma
                terms = [term.strip() for term in re.split(r'[,;]', text)]
                mesh_terms.extend([term for term in terms if term])

        return list(set(mesh_terms))  # Remover duplicados

    def extract_keywords(self, soup: BeautifulSoup) -> List[str]:
        """Extrae keywords del artículo"""
        keywords = []

        # Buscar en metadatos
        keyword_metas = soup.find_all('meta', {'name': 'citation_keywords'})
        for meta in keyword_metas:
            keyword = meta.get('content', '')
            if keyword:
                keywords.append(keyword)

        # Buscar en secciones de keywords
        keyword_selectors = [
            '.keywords',
            '.article-keywords',
            '[data-testid="keywords"]',
            '.key-words'
        ]

        for selector in keyword_selectors:
            element = soup.select_one(selector)
            if element:
                text = element.get_text()
                # Parsear keywords
                terms = [kw.strip() for kw in re.split(r'[,;]', text)]
                keywords.extend([kw for kw in terms if kw and len(kw) > 2])

        return list(set(keywords))  # Remover duplicados

    async def enrich_with_pubmed_api(self, article: Article):
        """Enriquece artículo con datos de PubMed API"""
        if not article.pmid:
            return

        try:
            # NCBI Entrez API
            base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"

            # Obtener información detallada
            fetch_url = f"{base_url}efetch.fcgi?db=pubmed&id={article.pmid}&retmode=xml"

            async with self.session.get(fetch_url) as response:
                if response.status == 200:
                    xml_content = await response.text()
                    # Aquí procesarías el XML de PubMed
                    # Por simplicidad, solo logeamos
                    logger.info(f"Enriquecido artículo {article.pmid} con PubMed API")

        except Exception as e:
            logger.error(f"Error enriqueciendo con PubMed API: {e}")

    def calculate_quality_score(self, article: Article) -> float:
        """Calcula score de calidad del artículo"""
        score = 0.0
        max_score = 10.0

        # Título presente y razonable
        if article.title and len(article.title) > 10:
            score += 1.0

        # Abstract presente
        if article.abstract and len(article.abstract) > 100:
            score += 2.0

        # Texto completo disponible
        if article.full_text and len(article.full_text) > 1000:
            score += 2.0

        # Metadatos básicos
        if article.pmid or article.pmcid:
            score += 1.0

        if article.doi:
            score += 1.0

        if article.journal:
            score += 1.0

        if article.publication_date:
            score += 0.5

        # Autores
        if article.authors and len(article.authors) > 0:
            score += 1.0

        # Keywords o MeSH terms
        if (article.keywords and len(article.keywords) > 0) or (article.mesh_terms and len(article.mesh_terms) > 0):
            score += 0.5

        return min(score / max_score, 1.0)

    def validate_data_quality(self, articles: List[Article]) -> List[Article]:
        """Validación de calidad de datos"""
        validated_articles = []
        quality_threshold = 0.5

        for article in articles:
            # Revalidar score
            article.quality_score = self.calculate_quality_score(article)

            # Validaciones específicas
            if self.is_valid_article(article):
                if article.quality_score >= quality_threshold:
                    validated_articles.append(article)
                else:
                    logger.warning(f"Artículo con baja calidad descartado: {article.title}")
            else:
                logger.warning(f"Artículo inválido descartado: {article.title}")

        logger.info(f"Validados {len(validated_articles)} de {len(articles)} artículos")
        return validated_articles

    def is_valid_article(self, article: Article) -> bool:
        """Valida si un artículo cumple criterios mínimos"""
        # Debe tener título
        if not article.title or len(article.title.strip()) < 10:
            return False

        # Debe tener URL válida
        if not article.url or not self.is_valid_url(article.url):
            return False

        # Debe tener al menos abstract o texto completo
        if not article.abstract and not article.full_text:
            return False

        # Si tiene fecha, debe ser razonable (no futuro, no muy antigua)
        if article.publication_date:
            current_year = datetime.now().year
            pub_year = article.publication_date.year
            if pub_year > current_year or pub_year < 1950:
                return False

        return True

    def is_valid_url(self, url: str) -> bool:
        """Valida si una URL es válida"""
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False

    def standardize_format(self, articles: List[Article]) -> List[Dict]:
        """Convierte artículos a formato estándar JSON-LD"""
        standardized_articles = []

        for article in articles:
            # Remover duplicados
            if not self.duplicate_detector.is_duplicate(article):
                json_ld = self.convert_to_json_ld(article)
                rdf_triples = self.convert_to_rdf(article)

                standardized_article = {
                    'article_data': asdict(article),
                    'json_ld': json_ld,
                    'rdf_triples': rdf_triples,
                    'graph_entities': self.extract_graph_entities(article)
                }

                standardized_articles.append(standardized_article)
                self.duplicate_detector.add_article(article)

        logger.info(f"Estandarizados {len(standardized_articles)} artículos únicos")
        return standardized_articles

    def convert_to_json_ld(self, article: Article) -> Dict:
        """Convierte artículo a formato JSON-LD"""
        # Definir contexto semántico
        context = {
            "@context": {
                "schema": "https://schema.org/",
                "bio": "http://purl.obolibrary.org/obo/",
                "dcterms": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/"
            }
        }

        # Estructura básica del artículo
        article_id = self.generate_article_id(article)

        json_ld = {
            "@context": context["@context"],
            "@id": article_id,
            "@type": "schema:ScholarlyArticle",
            "schema:name": article.title,
            "schema:url": article.url,
            "dcterms:created": article.publication_date.isoformat() if article.publication_date else None,
            "schema:abstract": article.abstract,
            "schema:text": article.full_text
        }

        # Agregar journal
        if article.journal:
            json_ld["schema:isPartOf"] = {
                "@type": "schema:Periodical",
                "schema:name": article.journal
            }

        # Agregar autores
        if article.authors:
            authors_list = []
            for author in article.authors:
                author_obj = {
                    "@type": "schema:Person",
                    "schema:name": author.get('name', ''),
                }
                if author.get('affiliation'):
                    author_obj["schema:affiliation"] = {
                        "@type": "schema:Organization",
                        "schema:name": author['affiliation']
                    }
                authors_list.append(author_obj)

            json_ld["schema:author"] = authors_list

        # Agregar identificadores
        identifiers = []
        if article.doi:
            identifiers.append({
                "@type": "schema:PropertyValue",
                "schema:propertyID": "DOI",
                "schema:value": article.doi
            })

        if article.pmid:
            identifiers.append({
                "@type": "schema:PropertyValue",
                "schema:propertyID": "PMID",
                "schema:value": article.pmid
            })

        if article.pmcid:
            identifiers.append({
                "@type": "schema:PropertyValue",
                "schema:propertyID": "PMCID",
                "schema:value": article.pmcid
            })

        if identifiers:
            json_ld["schema:identifier"] = identifiers

        # Agregar keywords
        if article.keywords:
            json_ld["schema:keywords"] = article.keywords

        # Agregar términos MeSH
        if article.mesh_terms:
            json_ld["bio:mesh_terms"] = article.mesh_terms

        return json_ld

    def convert_to_rdf(self, article: Article) -> List[Tuple]:
        """Convierte artículo a triplas RDF"""
        g = Graph()

        # Definir namespaces
        SCHEMA = Namespace("https://schema.org/")
        BIO = Namespace("http://purl.obolibrary.org/obo/")
        SPACE = Namespace("http://space-biology.org/ontology/")

        g.bind("schema", SCHEMA)
        g.bind("bio", BIO)
        g.bind("space", SPACE)

        # URI del artículo
        article_uri = URIRef(self.generate_article_id(article))

        # Triplas básicas del artículo
        g.add((article_uri, RDF.type, SCHEMA.ScholarlyArticle))
        g.add((article_uri, SCHEMA.name, Literal(article.title)))
        g.add((article_uri, SCHEMA.url, URIRef(article.url)))

        if article.abstract:
            g.add((article_uri, SCHEMA.abstract, Literal(article.abstract)))

        if article.full_text:
            g.add((article_uri, SCHEMA.text, Literal(article.full_text)))

        if article.publication_date:
            g.add((article_uri, DCTERMS.created, Literal(article.publication_date)))

        # Journal
        if article.journal:
            journal_uri = URIRef(
                f"http://space-biology.org/journal/{hashlib.md5(article.journal.encode()).hexdigest()}")
            g.add((journal_uri, RDF.type, SCHEMA.Periodical))
            g.add((journal_uri, SCHEMA.name, Literal(article.journal)))
            g.add((article_uri, SCHEMA.isPartOf, journal_uri))

        # Autores
        for i, author in enumerate(article.authors):
            author_uri = URIRef(
                f"http://space-biology.org/author/{hashlib.md5(author.get('name', '').encode()).hexdigest()}")
            g.add((author_uri, RDF.type, SCHEMA.Person))
            g.add((author_uri, SCHEMA.name, Literal(author.get('name', ''))))
            g.add((article_uri, SCHEMA.author, author_uri))

            if author.get('affiliation'):
                affil_uri = URIRef(
                    f"http://space-biology.org/org/{hashlib.md5(author['affiliation'].encode()).hexdigest()}")
                g.add((affil_uri, RDF.type, SCHEMA.Organization))
                g.add((affil_uri, SCHEMA.name, Literal(author['affiliation'])))
                g.add((author_uri, SCHEMA.affiliation, affil_uri))

        # Identificadores
        if article.doi:
            g.add((article_uri, SCHEMA.doi, Literal(article.doi)))
        if article.pmid:
            g.add((article_uri, BIO.pmid, Literal(article.pmid)))
        if article.pmcid:
            g.add((article_uri, BIO.pmcid, Literal(article.pmcid)))

        # Keywords y MeSH terms
        for keyword in article.keywords:
            keyword_uri = URIRef(f"http://space-biology.org/keyword/{hashlib.md5(keyword.encode()).hexdigest()}")
            g.add((keyword_uri, RDF.type, SCHEMA.DefinedTerm))
            g.add((keyword_uri, SCHEMA.name, Literal(keyword)))
            g.add((article_uri, SCHEMA.keywords, keyword_uri))

        for mesh_term in article.mesh_terms:
            mesh_uri = URIRef(f"http://space-biology.org/mesh/{hashlib.md5(mesh_term.encode()).hexdigest()}")
            g.add((mesh_uri, RDF.type, BIO.MeshTerm))
            g.add((mesh_uri, SCHEMA.name, Literal(mesh_term)))
            g.add((article_uri, BIO.hasMeshTerm, mesh_uri))

        # Entidades biológicas extraídas
        for organism in article.organisms:
            organism_uri = URIRef(f"http://space-biology.org/organism/{hashlib.md5(organism.encode()).hexdigest()}")
            g.add((organism_uri, RDF.type, BIO.Organism))
            g.add((organism_uri, SCHEMA.name, Literal(organism)))
            g.add((article_uri, SPACE.studiesOrganism, organism_uri))

        # Convertir a lista de triplas
        triples = [(str(s), str(p), str(o)) for s, p, o in g]
        return triples

    def generate_article_id(self, article: Article) -> str:
        """Genera ID único para el artículo"""
        if article.doi:
            return f"http://space-biology.org/article/doi:{article.doi.replace('/', '_')}"
        elif article.pmcid:
            return f"http://space-biology.org/article/{article.pmcid}"
        elif article.pmid:
            return f"http://space-biology.org/article/pmid:{article.pmid}"
        else:
            # Fallback usando hash del título
            title_hash = hashlib.md5(article.title.encode()).hexdigest()
            return f"http://space-biology.org/article/{title_hash}"

    def extract_graph_entities(self, article: Article) -> Dict:
        """Extrae entidades estructuradas para el knowledge graph"""
        entities = {
            'Article': {
                'id': self.generate_article_id(article),
                'title': article.title,
                'date': article.publication_date.isoformat() if article.publication_date else None,
                'journal': article.journal,
                'quality_score': article.quality_score
            },
            'Authors': [
                {
                    'name': author.get('name', ''),
                    'affiliation': author.get('affiliation')
                }
                for author in article.authors
            ],
            'Keywords': article.keywords,
            'MeshTerms': article.mesh_terms,
            'Organisms': article.organisms,
            'Genes': article.genes,
            'Proteins': article.proteins,
            'Conditions': article.conditions,
            'Experiments': article.experiments,
            'Measurements': article.measurements
        }

        return entities


class DuplicateDetector:
    """Detector de artículos duplicados"""

    def __init__(self):
        self.seen_articles = set()
        self.seen_titles = {}
        self.similarity_threshold = 85

    def is_duplicate(self, article: Article) -> bool:
        """Determina si un artículo es duplicado"""
        # Check por identificadores únicos
        if article.doi and article.doi in self.seen_articles:
            return True
        if article.pmid and article.pmid in self.seen_articles:
            return True
        if article.pmcid and article.pmcid in self.seen_articles:
            return True

        # Check por similitud de título
        for existing_title, existing_article in self.seen_titles.items():
            similarity = fuzz.ratio(article.title.lower(), existing_title.lower())
            if similarity >= self.similarity_threshold:
                logger.info(f"Artículo duplicado detectado por título: {article.title}")
                return True

        return False

    def add_article(self, article: Article):
        """Agrega artículo al registro de vistos"""
        if article.doi:
            self.seen_articles.add(article.doi)
        if article.pmid:
            self.seen_articles.add(article.pmid)
        if article.pmcid:
            self.seen_articles.add(article.pmcid)

        self.seen_titles[article.title.lower()] = article


class BiologyNLPProcessor:
    """Procesador NLP especializado en biología espacial"""

    def __init__(self):
        try:
            # Cargar modelo ScispaCy
            self.scispacy_model = spacy.load("en_core_sci_lg")
        except OSError:
            logger.warning(
                "ScispaCy modelo no encontrado. Instalar con: pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.1/en_core_sci_lg-0.5.1.tar.gz")
            self.scispacy_model = None

        # Cargar BioBERT
        try:
            self.tokenizer = AutoTokenizer.from_pretrained('dmis-lab/biobert-v1.1')
            self.biobert_model = AutoModel.from_pretrained('dmis-lab/biobert-v1.1')
        except Exception as e:
            logger.warning(f"Error cargando BioBERT: {e}")
            self.tokenizer = None
            self.biobert_model = None

        # Diccionarios de entidades biológicas específicas de biología espacial
        self.space_biology_entities = {
            'organisms': {
                'human', 'mouse', 'mice', 'rat', 'drosophila', 'arabidopsis',
                'caenorhabditis elegans', 'c. elegans', 'saccharomyces cerevisiae',
                'escherichia coli', 'e. coli', 'bacillus subtilis'
            },
            'space_conditions': {
                'microgravity', 'hypergravity', 'weightlessness', 'zero gravity',
                'cosmic radiation', 'space radiation', 'solar radiation',
                'magnetic field', 'space environment', 'orbital flight',
                'parabolic flight', 'centrifuge', 'clinostat', 'rpms'
            },
            'biological_systems': {
                'cardiovascular', 'musculoskeletal', 'nervous system', 'immune system',
                'bone', 'muscle', 'heart', 'brain', 'kidney', 'liver',
                'bone density', 'muscle atrophy', 'osteoporosis'
            },
            'experiments': {
                'gene expression', 'protein analysis', 'cell culture',
                'tissue engineering', 'metabolomics', 'proteomics', 'genomics',
                'transcriptomics', 'histology', 'microscopy'
            },
            'measurements': {
                'body mass', 'bone density', 'muscle strength', 'heart rate',
                'blood pressure', 'hormone levels', 'gene expression levels',
                'protein concentration', 'cell viability', 'proliferation rate'
            }
        }

        # Patrones de expresiones regulares para extracción
        self.entity_patterns = {
            'genes': r'\b[A-Z][A-Z0-9]{2,}(?:-[A-Z0-9]+)?\b',
            'proteins': r'\b[A-Z][a-z]+(?:-\d+)?(?:\s+[A-Z][a-z]*)*\b',
            'measurements': r'\d+(?:\.\d+)?\s*(?:mg|g|kg|ml|l|μm|mm|cm|m|°C|%|ppm|μg)',
            'time_periods': r'\d+\s*(?:days?|weeks?|months?|years?|hrs?|hours?|mins?|minutes?)'
        }

    async def process_articles(self, articles: List[Article]) -> List[Article]:
        """Procesa lista de artículos con NLP"""
        processed_articles = []

        for article in articles:
            try:
                processed_article = await self.process_single_article(article)
                processed_articles.append(processed_article)
                logger.info(f"Procesado NLP: {article.title[:50]}...")
            except Exception as e:
                logger.error(f"Error procesando NLP en artículo {article.title}: {e}")
                processed_articles.append(article)  # Incluir sin procesar

        return processed_articles

    async def process_single_article(self, article: Article) -> Article:
        """Procesa un artículo individual con NLP"""
        # Combinar texto disponible
        text_to_process = ""
        if article.abstract:
            text_to_process += article.abstract + " "
        if article.full_text:
            # Limitar texto completo para eficiencia
            text_to_process += article.full_text[:5000]

        if not text_to_process.strip():
            return article

        # Extraer entidades
        entities = await self.extract_entities(text_to_process)

        # Asignar entidades extraídas al artículo
        article.organisms.extend(entities.get('organisms', []))
        article.genes.extend(entities.get('genes', []))
        article.proteins.extend(entities.get('proteins', []))
        article.conditions.extend(entities.get('conditions', []))
        article.experiments.extend(entities.get('experiments', []))
        article.measurements.extend(entities.get('measurements', []))

        # Remover duplicados
        article.organisms = list(set(article.organisms))
        article.genes = list(set(article.genes))
        article.proteins = list(set(article.proteins))
        article.conditions = list(set(article.conditions))
        article.experiments = list(set(article.experiments))
        article.measurements = list(set(article.measurements))

        return article

    async def extract_entities(self, text: str) -> Dict[str, List[str]]:
        """Extrae entidades biológicas del texto"""
        entities = {
            'organisms': [],
            'genes': [],
            'proteins': [],
            'conditions': [],
            'experiments': [],
            'measurements': []
        }

        text_lower = text.lower()

        # Extraer organismos
        for organism in self.space_biology_entities['organisms']:
            if organism in text_lower:
                entities['organisms'].append(organism)

        # Extraer condiciones espaciales
        for condition in self.space_biology_entities['space_conditions']:
            if condition in text_lower:
                entities['conditions'].append(condition)

        # Extraer sistemas biológicos
        for system in self.space_biology_entities['biological_systems']:
            if system in text_lower:
                entities['conditions'].append(system)

        # Extraer experimentos
        for experiment in self.space_biology_entities['experiments']:
            if experiment in text_lower:
                entities['experiments'].append(experiment)

        # Extraer mediciones
        for measurement in self.space_biology_entities['measurements']:
            if measurement in text_lower:
                entities['measurements'].append(measurement)

        # Usar regex para genes y proteínas
        gene_matches = re.findall(self.entity_patterns['genes'], text)
        entities['genes'].extend(gene_matches[:10])  # Limitar cantidad

        protein_matches = re.findall(self.entity_patterns['proteins'], text)
        entities['proteins'].extend(protein_matches[:10])

        # Extraer mediciones numéricas
        measurement_matches = re.findall(self.entity_patterns['measurements'], text)
        entities['measurements'].extend(measurement_matches[:15])

        # Usar ScispaCy si está disponible
        if self.scispacy_model:
            entities = await self.enhance_with_scispacy(text, entities)

        return entities

    async def enhance_with_scispacy(self, text: str, entities: Dict) -> Dict:
        """Mejora extracción con ScispaCy"""
        try:
            # Procesar con ScispaCy (limitar texto por rendimiento)
            doc = self.scispacy_model(text[:3000])

            for ent in doc.ents:
                entity_text = ent.text.lower().strip()

                # Clasificar entidades según label
                if ent.label_ in ['SPECIES', 'TAXON']:
                    entities['organisms'].append(entity_text)
                elif ent.label_ in ['GENE', 'PROTEIN']:
                    if len(entity_text) > 2:
                        entities['genes'].append(entity_text)
                elif ent.label_ in ['CHEMICAL', 'DRUG']:
                    entities['measurements'].append(entity_text)
                elif ent.label_ in ['DISEASE', 'CONDITION']:
                    entities['conditions'].append(entity_text)

        except Exception as e:
            logger.warning(f"Error con ScispaCy: {e}")

        return entities

    async def extract_relationships(self, text: str, entities: Dict) -> List[Dict]:
        """Extrae relaciones entre entidades"""
        relationships = []

        # Patrones simples de relaciones
        relation_patterns = [
            (r'(\w+)\s+(?:affects?|influences?|regulates?)\s+(\w+)', 'AFFECTS'),
            (r'(\w+)\s+(?:increases?|decreases?|reduces?)\s+(\w+)', 'MODULATES'),
            (r'(\w+)\s+(?:in|during|under)\s+(\w+)', 'CONTEXT'),
            (r'(\w+)\s+(?:expression|levels?)\s+(?:in|of)\s+(\w+)', 'EXPRESSED_IN')
        ]

        for pattern, relation_type in relation_patterns:
            matches = re.findall(pattern, text.lower())
            for match in matches[:5]:  # Limitar cantidad
                if len(match) == 2:
                    relationships.append({
                        'subject': match[0],
                        'predicate': relation_type,
                        'object': match[1],
                        'confidence': 0.7
                    })

        return relationships

    async def generate_embeddings(self, text: str) -> Optional[List[float]]:
        """Genera embeddings semánticos con BioBERT"""
        if not self.tokenizer or not self.biobert_model:
            return None

        try:
            # Tokenizar texto
            inputs = self.tokenizer(
                text[:512],
                return_tensors="pt",
                truncation=True,
                padding=True
            )

            # Generar embeddings
            with torch.no_grad():
                outputs = self.biobert_model(**inputs)
                embeddings = outputs.last_hidden_state.mean(dim=1).squeeze()

            return embeddings.tolist()

        except Exception as e:
            logger.error(f"Error generando embeddings: {e}")
            return None


# Función principal de ejecución
async def main():
    """Función principal para ejecutar el pipeline completo"""
    logger.info("Iniciando pipeline de ingesta de biología espacial...")
    # --------------------------------------------------
    # 1️⃣ Construir ruta al CSV de manera universal
    # --------------------------------------------------
    # Si estás en notebook o script, BASE_DIR apunta a la raíz del proyecto
    try:
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        # Si __file__ no existe (notebook), usa el directorio actual
        BASE_DIR = os.getcwd()

    csv_path = os.path.join(BASE_DIR, "data", "raw", "SB_publication_PMC.csv")
    csv_path = os.path.abspath(csv_path)

    if not os.path.exists(csv_path):
        logger.error(f"No se encontró el archivo CSV: {csv_path}")
        return
    # --------------------------------------------------
    # 2️⃣ Ejecutar pipeline
    # --------------------------------------------------
    async with DataIngestionPipeline() as pipeline:
        try:
            # 1. Cargar datos del CSV
            logger.info("Cargando datos de SB Publications...")
            articles_data = await pipeline.load_sb_publications_csv(csv_path=csv_path)

            if not articles_data:
                logger.error("No se pudieron cargar datos del CSV")
                return

            # Procesar solo los primeros 10 artículos para testing
            articles_data = articles_data[:10]

            # 2. Extraer artículos
            logger.info("Extrayendo artículos...")
            extracted_articles = await pipeline.extract_papers(articles_data)

            # 3. Validar calidad
            logger.info("Validando calidad de datos...")
            validated_articles = pipeline.validate_data_quality(extracted_articles)

            # 4. Procesamiento NLP
            logger.info("Iniciando procesamiento NLP...")
            nlp_processor = BiologyNLPProcessor()
            processed_articles = await nlp_processor.process_articles(validated_articles)

            # 5. Estandarizar formato
            logger.info("Estandarizando formato...")
            standardized_articles = pipeline.standardize_format(processed_articles)

            # 6. Guardar resultados
            logger.info("Guardando resultados...")
            output_file = f"space_biology_articles_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(standardized_articles, f, indent=2, ensure_ascii=False, default=str)

            # Estadísticas finales
            logger.info("=== ESTADÍSTICAS FINALES ===")
            logger.info(f"Artículos procesados: {len(standardized_articles)}")
            logger.info(f"Artículos fallidos: {len(pipeline.failed_extractions)}")

            if standardized_articles:
                avg_quality = sum(article['article_data']['quality_score'] for article in standardized_articles) / len(
                    standardized_articles)
                logger.info(f"Calidad promedio: {avg_quality:.2f}")

                total_entities = sum(
                    len(article['graph_entities']['Organisms']) +
                    len(article['graph_entities']['Genes']) +
                    len(article['graph_entities']['Proteins'])
                    for article in standardized_articles
                )
                logger.info(f"Total entidades extraídas: {total_entities}")

            logger.info(f"Resultados guardados en: {output_file}")

        except Exception as e:
            logger.error(f"Error en pipeline principal: {e}")
            raise


if __name__ == "__main__":
    # Ejecutar pipeline
    asyncio.run(main())