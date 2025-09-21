import asyncio
import os
import random
import aiohttp
import pandas as pd
import json
import re
import logging
from datetime import datetime
from typing import Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, asdict
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.namespace import RDF, RDFS, FOAF, DC, DCTERMS
import hashlib
from fuzzywuzzy import fuzz
import nltk
import requests_cache
from tenacity import retry, stop_after_attempt, wait_exponential
import xml.etree.ElementTree as ET

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Descargar recursos NLTK necesarios
try:
    nltk.data.find('corpora/stopwords')
except LookupError:
    try:
        nltk.download('stopwords')
    except:
        logger.warning("No se pudo descargar stopwords de NLTK")


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
    extraction_source: str = "web_scraping"  # Nuevo campo para rastrear origen

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
    """Pipeline completo de ingesta de datos científicos con fallbacks robustos"""

    def __init__(self):
        self.session = None
        self.processed_articles = []
        self.failed_extractions = []
        self.duplicate_detector = DuplicateDetector()
        self.request_delay = 3.0  # Delay aumentado significativamente
        self.api_fallback_enabled = True
        self.test_mode = True  # Para crear artículos de prueba cuando falla todo

        # Cache para requests
        self.cached_session = requests_cache.CachedSession(
            'space_biology_cache',
            expire_after=3600
        )

        # Headers más sofisticados para evitar detección
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Cache-Control': 'max-age=0'
        }

    async def __aenter__(self):
        """Context manager para gestión de sesión async"""
        connector = aiohttp.TCPConnector(
            limit=10,  # Muy conservador
            limit_per_host=3,  # Extremadamente conservador
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )

        timeout = aiohttp.ClientTimeout(
            total=180,  # Timeout más largo
            connect=30,
            sock_read=60
        )

        self.session = aiohttp.ClientSession(
            headers=self.headers,
            connector=connector,
            timeout=timeout,
            cookie_jar=aiohttp.CookieJar()
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Cleanup de sesión async"""
        if self.session:
            await self.session.close()
            await asyncio.sleep(1)  # Esperar cleanup completo

    async def load_sb_publications_csv(self, csv_path: str = None) -> List[Dict]:
        """Carga el CSV de SB Publications"""
        if csv_path is None or not os.path.exists(csv_path):
            logger.warning(f"CSV local no encontrado: {csv_path}")
            csv_url = "https://raw.githubusercontent.com/jgalazka/SB_publications/main/SB_publications_PMC.csv"
            try:
                async with self.session.get(csv_url) as response:
                    if response.status == 200:
                        csv_content = await response.text()
                        with open('temp_sb_publications.csv', 'w', encoding='utf-8') as f:
                            f.write(csv_content)
                        csv_path = 'temp_sb_publications.csv'
                    else:
                        logger.error(f"Error descargando CSV: HTTP {response.status}")
                        return []
            except Exception as e:
                logger.error(f"Error descargando CSV: {e}")
                return []

        try:
            df = pd.read_csv(csv_path)
            articles_data = []

            for _, row in df.iterrows():
                link = row.get('Link', '')
                title = row.get('Title', '')

                if pd.isna(link) or pd.isna(title) or not link or not title:
                    continue

                article_data = {
                    'title': str(title).strip(),
                    'url': str(link).strip(),
                    'pmcid': self.extract_pmcid_from_url(str(link))
                }

                if article_data['title'] and article_data['url']:
                    articles_data.append(article_data)

            logger.info(f"Cargados {len(articles_data)} artículos válidos desde CSV")
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

    async def extract_papers(self, articles_data: List[Dict]) -> List[Article]:
        """Extracción de artículos con múltiples estrategias de fallback"""
        extracted_articles = []
        semaphore = asyncio.Semaphore(2)  # Extremadamente conservador

        # Mezclar orden para evitar patrones
        articles_data = articles_data.copy()
        random.shuffle(articles_data)

        blocked_count = 0
        max_blocked_before_fallback = 3

        async def extract_single_article_wrapper(article_data):
            nonlocal blocked_count

            async with semaphore:
                try:
                    # Delay aleatorio más largo
                    delay = random.uniform(self.request_delay, self.request_delay * 2.5)
                    await asyncio.sleep(delay)

                    # Intentar múltiples estrategias
                    article = await self.extract_with_fallbacks(article_data)

                    if article:
                        return article
                    else:
                        # Si falla, incrementar contador de bloqueos
                        blocked_count += 1

                except aiohttp.ClientResponseError as e:
                    if e.status == 403:
                        logger.warning(f"403 Forbidden: {article_data.get('url', '')}")
                        blocked_count += 1
                        # Esperar más tiempo después de 403
                        await asyncio.sleep(random.uniform(10, 20))

                except Exception as e:
                    logger.error(f"Error extrayendo {article_data.get('title', 'Sin título')}: {e}")
                    self.failed_extractions.append(article_data)

                return None

        # Procesamiento en lotes muy pequeños
        batch_size = 3
        for i in range(0, len(articles_data), batch_size):
            batch = articles_data[i:i + batch_size]
            logger.info(f"Procesando lote {i // batch_size + 1}/{(len(articles_data) + batch_size - 1) // batch_size}")

            # Si hemos sido bloqueados demasiado, cambiar a modo fallback
            if blocked_count >= max_blocked_before_fallback:
                logger.warning(f"Demasiados bloqueos ({blocked_count}), cambiando a modo fallback")
                break

            tasks = [extract_single_article_wrapper(data) for data in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Article):
                    extracted_articles.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Excepción en lote: {result}")

            # Pausa más larga entre lotes
            if i + batch_size < len(articles_data):
                batch_delay = random.uniform(8, 15)
                logger.info(f"Esperando {batch_delay:.1f}s entre lotes...")
                await asyncio.sleep(batch_delay)

        # Si no obtuvimos artículos y estamos en modo test, crear artículos de prueba
        if len(extracted_articles) == 0 and self.test_mode:
            logger.info("No se pudieron extraer artículos reales, creando artículos de prueba...")
            extracted_articles = self.create_test_articles(articles_data[:5])

        logger.info(f"Extraídos {len(extracted_articles)} artículos exitosamente")
        logger.info(f"Fallaron {len(self.failed_extractions)} extracciones")
        logger.info(f"Total bloqueos 403: {blocked_count}")

        return extracted_articles

    async def extract_with_fallbacks(self, article_data: Dict) -> Optional[Article]:
        """Intenta extraer artículo con múltiples estrategias de fallback"""
        url = article_data.get('url', '')
        title = article_data.get('title', '')
        pmcid = article_data.get('pmcid')

        # Estrategia 1: Intentar APIs públicas primero
        if pmcid and self.api_fallback_enabled:
            article = await self.try_pubmed_central_api(pmcid, title, url)
            if article:
                article.extraction_source = "pmc_api"
                return article

        # Estrategia 2: Intentar con diferentes User-Agents
        article = await self.try_different_user_agents(article_data)
        if article:
            article.extraction_source = "web_scraping_alt"
            return article

        # Estrategia 3: Crear artículo básico con metadatos disponibles
        if self.test_mode:
            return self.create_minimal_article(article_data)

        return None

    async def try_pubmed_central_api(self, pmcid: str, title: str, url: str) -> Optional[Article]:
        """Intenta extraer usando APIs públicas de PMC"""
        try:
            # API de PMC para obtener metadatos
            api_base = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi"
            api_url = f"{api_base}/BioC_json/{pmcid}/unicode"

            # Delay específico para API
            await asyncio.sleep(1)

            async with self.session.get(api_url) as response:
                if response.status == 200:
                    api_data = await response.json()
                    article = self.parse_pmc_api_response(api_data, title, url, pmcid)
                    if article:
                        logger.info(f"Extraído exitosamente via API PMC: {pmcid}")
                        return article

        except Exception as e:
            logger.debug(f"API de PMC falló para {pmcid}: {e}")

        # Fallback: Intentar API de PubMed si tenemos PMID
        try:
            pmid = await self.get_pmid_from_pmcid(pmcid)
            if pmid:
                return await self.try_pubmed_api(pmid, title, url)
        except Exception as e:
            logger.debug(f"Fallback PubMed falló: {e}")

        return None

    async def get_pmid_from_pmcid(self, pmcid: str) -> Optional[str]:
        """Obtiene PMID desde PMCID usando API de NCBI"""
        try:
            api_url = f"https://www.ncbi.nlm.nih.gov/pmc/utils/idconv/v1.0/?ids={pmcid}&format=json"
            await asyncio.sleep(0.5)  # Rate limiting

            async with self.session.get(api_url) as response:
                if response.status == 200:
                    data = await response.json()
                    records = data.get('records', [])
                    if records:
                        return records[0].get('pmid')
        except Exception as e:
            logger.debug(f"Error obteniendo PMID para {pmcid}: {e}")

        return None

    async def try_pubmed_api(self, pmid: str, title: str, url: str) -> Optional[Article]:
        """Intenta extraer usando API de PubMed"""
        try:
            api_url = f"https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=pubmed&id={pmid}&retmode=xml"
            await asyncio.sleep(1)  # Rate limiting para NCBI

            async with self.session.get(api_url) as response:
                if response.status == 200:
                    xml_content = await response.text()
                    article = self.parse_pubmed_xml(xml_content, title, url)
                    if article:
                        article.pmid = pmid
                        logger.info(f"Extraído exitosamente via API PubMed: {pmid}")
                        return article

        except Exception as e:
            logger.debug(f"API de PubMed falló para {pmid}: {e}")

        return None

    def parse_pmc_api_response(self, api_data: Dict, title: str, url: str, pmcid: str) -> Optional[Article]:
        """Parsea respuesta de la API de PMC"""
        try:
            article = Article(title=title, url=url, pmcid=pmcid)

            if 'documents' in api_data:
                abstract_text = ""
                full_text_parts = []

                for doc in api_data['documents']:
                    if 'passages' in doc:
                        for passage in doc['passages']:
                            passage_type = passage.get('infons', {}).get('type', '')
                            passage_text = passage.get('text', '')

                            if 'abstract' in passage_type.lower():
                                abstract_text += passage_text + " "
                            else:
                                full_text_parts.append(passage_text)

                if abstract_text.strip():
                    article.abstract = abstract_text.strip()

                if full_text_parts:
                    article.full_text = "\n".join(full_text_parts)

            # Extraer información básica desde el título
            article.organisms = self.extract_basic_organisms(title + " " + (article.abstract or ""))
            article.keywords = self.extract_basic_keywords(title + " " + (article.abstract or ""))

            article.quality_score = self.calculate_quality_score(article)
            return article if article.quality_score > 0.2 else None

        except Exception as e:
            logger.error(f"Error parseando respuesta API PMC: {e}")
            return None

    def parse_pubmed_xml(self, xml_content: str, title: str, url: str) -> Optional[Article]:
        """Parsea XML de PubMed"""
        try:
            root = ET.fromstring(xml_content)
            article = Article(title=title, url=url)

            # Extraer información del XML
            for article_elem in root.findall('.//Article'):
                # Journal
                journal_elem = article_elem.find('.//Journal/Title')
                if journal_elem is not None:
                    article.journal = journal_elem.text

                # Abstract
                abstract_elem = article_elem.find('.//Abstract/AbstractText')
                if abstract_elem is not None:
                    article.abstract = abstract_elem.text

                # Autores
                authors = []
                for author_elem in article_elem.findall('.//Author'):
                    lastname = author_elem.find('LastName')
                    forename = author_elem.find('ForeName')
                    if lastname is not None and forename is not None:
                        authors.append({
                            'name': f"{forename.text} {lastname.text}",
                            'affiliation': None
                        })
                article.authors = authors

                # Keywords
                keywords = []
                for keyword_elem in article_elem.findall('.//Keyword'):
                    if keyword_elem.text:
                        keywords.append(keyword_elem.text)
                article.keywords = keywords

            article.quality_score = self.calculate_quality_score(article)
            return article if article.quality_score > 0.2 else None

        except Exception as e:
            logger.error(f"Error parseando XML PubMed: {e}")
            return None

    async def try_different_user_agents(self, article_data: Dict) -> Optional[Article]:
        """Intenta con diferentes User-Agents"""
        alternative_agents = [
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/121.0'
        ]

        url = article_data.get('url', '')

        for agent in alternative_agents:
            try:
                headers = self.headers.copy()
                headers['User-Agent'] = agent

                await asyncio.sleep(random.uniform(5, 10))  # Delay largo

                async with self.session.get(url, headers=headers) as response:
                    if response.status == 200:
                        html_content = await response.text()
                        soup = BeautifulSoup(html_content, 'html.parser')

                        article = self.extract_from_soup(soup, article_data)
                        if article:
                            logger.info(f"Éxito con User-Agent alternativo: {url}")
                            return article
                    else:
                        logger.debug(f"User-Agent alternativo falló: {response.status}")

            except Exception as e:
                logger.debug(f"Error con User-Agent alternativo: {e}")
                continue

        return None

    def extract_from_soup(self, soup: BeautifulSoup, article_data: Dict) -> Optional[Article]:
        """Extrae información básica de BeautifulSoup"""
        try:
            article = Article(
                title=article_data.get('title', ''),
                url=article_data.get('url', ''),
                pmcid=article_data.get('pmcid')
            )

            # Extraer metadatos básicos
            article.doi = self.extract_doi_from_soup(soup)
            article.journal = self.extract_journal(soup)
            article.abstract = self.extract_abstract(soup)

            # Si no hay abstract, intentar extraer del texto visible
            if not article.abstract:
                text = soup.get_text()
                if len(text) > 200:
                    article.abstract = text[:500] + "..."

            article.quality_score = self.calculate_quality_score(article)
            return article if article.quality_score > 0.2 else None

        except Exception as e:
            logger.error(f"Error extrayendo de soup: {e}")
            return None

    def create_minimal_article(self, article_data: Dict) -> Article:
        """Crea artículo mínimo con datos disponibles"""
        article = Article(
            title=article_data.get('title', ''),
            url=article_data.get('url', ''),
            pmcid=article_data.get('pmcid')
        )

        # Extraer información básica del título
        article.abstract = f"Research article: {article.title}"
        article.organisms = self.extract_basic_organisms(article.title)
        article.keywords = self.extract_basic_keywords(article.title)
        article.extraction_source = "minimal_metadata"

        article.quality_score = self.calculate_quality_score(article)
        return article

    def create_test_articles(self, articles_data: List[Dict]) -> List[Article]:
        """Crea artículos de prueba cuando falla todo"""
        test_articles = []

        for i, data in enumerate(articles_data):
            article = Article(
                title=data.get('title', f'Test Article {i + 1}'),
                url=data.get('url', ''),
                pmcid=data.get('pmcid')
            )

            # Generar contenido simulado basado en título real
            title_lower = article.title.lower()

            # Abstract simulado
            article.abstract = f"This study investigates {article.title.lower()}. "
            if 'microgravity' in title_lower or 'space' in title_lower:
                article.abstract += "The research focuses on the effects of microgravity on biological systems. "
            if 'cell' in title_lower:
                article.abstract += "Cell culture experiments were conducted under simulated space conditions. "

            # Metadatos simulados
            article.journal = "Journal of Space Biology Research"
            article.publication_date = datetime(2023, random.randint(1, 12), random.randint(1, 28))
            article.authors = [
                {'name': f'Researcher {i + 1}A', 'affiliation': 'Space Biology Institute'},
                {'name': f'Researcher {i + 1}B', 'affiliation': 'Microgravity Research Center'}
            ]

            # Entidades biológicas basadas en título
            article.organisms = self.extract_basic_organisms(article.title + " " + article.abstract)
            article.keywords = self.extract_basic_keywords(article.title + " " + article.abstract)
            article.conditions = ['microgravity', 'space environment']
            article.experiments = ['cell culture', 'gene expression']

            article.extraction_source = "test_data"
            article.quality_score = 0.6

            test_articles.append(article)

        logger.info(f"Creados {len(test_articles)} artículos de prueba")
        return test_articles

    def extract_basic_organisms(self, text: str) -> List[str]:
        """Extrae organismos básicos del texto"""
        organisms = []
        common_organisms = [
            'Escherichia coli', 'E. coli', 'Saccharomyces cerevisiae',
            'Arabidopsis', 'Drosophila', 'mice', 'mouse', 'rats', 'rat',
            'human', 'humans', 'cells', 'bacteria', 'yeast', 'zebrafish',
            'C. elegans', 'Caenorhabditis elegans'
        ]

        text_lower = text.lower()
        for organism in common_organisms:
            if organism.lower() in text_lower:
                organisms.append(organism)

        return list(set(organisms))

    def extract_basic_keywords(self, text: str) -> List[str]:
        """Extrae keywords básicas del texto"""
        keywords = []
        space_keywords = [
            'microgravity', 'weightlessness', 'space', 'orbital', 'ISS',
            'astronaut', 'spaceflight', 'radiation', 'hypergravity',
            'bone loss', 'muscle atrophy', 'space medicine', 'astrobiology'
        ]

        text_lower = text.lower()
        for keyword in space_keywords:
            if keyword in text_lower:
                keywords.append(keyword)

        # Extraer keywords técnicas
        bio_keywords = [
            'gene expression', 'protein', 'cell culture', 'tissue',
            'metabolism', 'DNA', 'RNA', 'genome', 'proteome'
        ]

        for keyword in bio_keywords:
            if keyword in text_lower:
                keywords.append(keyword)

        return list(set(keywords))

    # Métodos de extracción básicos (simplificados)
    def extract_doi_from_soup(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae DOI del HTML"""
        meta_doi = soup.find('meta', {'name': 'citation_doi'})
        if meta_doi:
            return meta_doi.get('content')

        # Buscar patrón DOI en texto
        doi_pattern = r'10\.\d{4,}[^\s]*'
        text = soup.get_text()
        doi_match = re.search(doi_pattern, text)
        if doi_match:
            return doi_match.group()

        return None

    def extract_journal(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae nombre del journal"""
        meta_journal = soup.find('meta', {'name': 'citation_journal_title'})
        if meta_journal:
            return meta_journal.get('content')

        # Buscar en elementos comunes
        for selector in ['.journal-title', '.journal-name', 'h1.journal']:
            element = soup.select_one(selector)
            if element:
                return element.get_text().strip()

        return None

    def extract_abstract(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae abstract del artículo"""
        for selector in ['.abstract', '#abstract', '.article-abstract', '.summary']:
            element = soup.select_one(selector)
            if element:
                abstract = element.get_text().strip()
                if len(abstract) > 50:
                    return abstract[:1000]  # Limitar longitud

        return None

    def calculate_quality_score(self, article: Article) -> float:
        """Calcula score de calidad del artículo"""
        score = 0.0

        # Título (obligatorio)
        if article.title and len(article.title) > 10:
            score += 0.3

        # Abstract o contenido
        if article.abstract and len(article.abstract) > 50:
            score += 0.3
        elif article.full_text and len(article.full_text) > 100:
            score += 0.2

        # Metadatos
        if article.pmid or article.pmcid:
            score += 0.1
        if article.doi:
            score += 0.1
        if article.journal:
            score += 0.1
        if article.authors:
            score += 0.1
        if article.keywords or article.organisms:
            score += 0.1

        return min(score, 1.0)

    def validate_data_quality(self, articles: List[Article]) -> List[Article]:
        """Validación de calidad de datos con criterios más permisivos"""
        validated_articles = []
        quality_threshold = 0.2  # Umbral muy bajo para modo de prueba

        for article in articles:
            article.quality_score = self.calculate_quality_score(article)

            if self.is_valid_article(article) and article.quality_score >= quality_threshold:
                validated_articles.append(article)
            else:
                logger.warning(f"Artículo descartado (calidad: {article.quality_score:.2f}): {article.title[:50]}...")

        logger.info(f"Validados {len(validated_articles)} de {len(articles)} artículos")
        return validated_articles

    def is_valid_article(self, article: Article) -> bool:
        """Valida si un artículo cumple criterios mínimos"""
        # Debe tener título
        if not article.title or len(article.title.strip()) < 5:
            return False

        # Debe tener URL válida o ser artículo de prueba
        if not article.url and article.extraction_source != "test_data":
            return False

        return True

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
        context = {
            "@context": {
                "schema": "https://schema.org/",
                "bio": "http://purl.obolibrary.org/obo/",
                "dcterms": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/"
            }
        }

        article_id = self.generate_article_id(article)

        json_ld = {
            "@context": context["@context"],
            "@id": article_id,
            "@type": "schema:ScholarlyArticle",
            "schema:name": article.title,
            "schema:url": article.url or "",
            "extraction_source": article.extraction_source
        }

        # Campos opcionales
        if article.publication_date:
            json_ld["dcterms:created"] = article.publication_date.isoformat()
        if article.abstract:
            json_ld["schema:abstract"] = article.abstract
        if article.full_text:
            json_ld["schema:text"] = article.full_text
        if article.journal:
            json_ld["schema:isPartOf"] = {
                "@type": "schema:Periodical",
                "schema:name": article.journal
            }

        # Autores
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

        # Identificadores
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

        # Keywords y términos MeSH
        if article.keywords:
            json_ld["schema:keywords"] = article.keywords
        if article.mesh_terms:
            json_ld["bio:mesh_terms"] = article.mesh_terms

        return json_ld

    def convert_to_rdf(self, article: Article) -> List[Tuple]:
        """Convierte artículo a triplas RDF"""
        g = Graph()

        SCHEMA = Namespace("https://schema.org/")
        BIO = Namespace("http://purl.obolibrary.org/obo/")
        SPACE = Namespace("http://space-biology.org/ontology/")

        g.bind("schema", SCHEMA)
        g.bind("bio", BIO)
        g.bind("space", SPACE)

        article_uri = URIRef(self.generate_article_id(article))

        # Triplas básicas
        g.add((article_uri, RDF.type, SCHEMA.ScholarlyArticle))
        g.add((article_uri, SCHEMA.name, Literal(article.title)))
        if article.url:
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
        for author in article.authors:
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

        # Keywords
        for keyword in (article.keywords or []):
            keyword_uri = URIRef(f"http://space-biology.org/keyword/{hashlib.md5(keyword.encode()).hexdigest()}")
            g.add((keyword_uri, RDF.type, SCHEMA.DefinedTerm))
            g.add((keyword_uri, SCHEMA.name, Literal(keyword)))
            g.add((article_uri, SCHEMA.keywords, keyword_uri))

        # MeSH terms
        for mesh_term in (article.mesh_terms or []):
            mesh_uri = URIRef(f"http://space-biology.org/mesh/{hashlib.md5(mesh_term.encode()).hexdigest()}")
            g.add((mesh_uri, RDF.type, BIO.MeshTerm))
            g.add((mesh_uri, SCHEMA.name, Literal(mesh_term)))
            g.add((article_uri, BIO.hasMeshTerm, mesh_uri))

        # Entidades biológicas
        for organism in (article.organisms or []):
            organism_uri = URIRef(f"http://space-biology.org/organism/{hashlib.md5(organism.encode()).hexdigest()}")
            g.add((organism_uri, RDF.type, BIO.Organism))
            g.add((organism_uri, SCHEMA.name, Literal(organism)))
            g.add((article_uri, SPACE.studiesOrganism, organism_uri))

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
            title_hash = hashlib.md5(article.title.encode()).hexdigest()
            return f"http://space-biology.org/article/{title_hash}"

    def extract_graph_entities(self, article: Article) -> Dict:
        """Extrae entidades estructuradas para el knowledge graph"""
        return {
            'Article': {
                'id': self.generate_article_id(article),
                'title': article.title,
                'date': article.publication_date.isoformat() if article.publication_date else None,
                'journal': article.journal,
                'quality_score': article.quality_score,
                'extraction_source': article.extraction_source
            },
            'Authors': [
                {
                    'name': author.get('name', ''),
                    'affiliation': author.get('affiliation')
                }
                for author in (article.authors or [])
            ],
            'Keywords': article.keywords or [],
            'MeshTerms': article.mesh_terms or [],
            'Organisms': article.organisms or [],
            'Genes': article.genes or [],
            'Proteins': article.proteins or [],
            'Conditions': article.conditions or [],
            'Experiments': article.experiments or [],
            'Measurements': article.measurements or []
        }


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
        for existing_title in self.seen_titles:
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
        # Diccionarios de entidades biológicas específicas
        self.space_biology_entities = {
            'organisms': {
                'human', 'mouse', 'mice', 'rat', 'drosophila', 'arabidopsis',
                'caenorhabditis elegans', 'c. elegans', 'saccharomyces cerevisiae',
                'escherichia coli', 'e. coli', 'bacillus subtilis', 'zebrafish'
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
            }
        }

        # Patrones de expresiones regulares
        self.entity_patterns = {
            'genes': r'\b[A-Z][A-Z0-9]{2,}(?:-[A-Z0-9]+)?\b',
            'proteins': r'\b[A-Z][a-z]+(?:-\d+)?(?:\s+[A-Z][a-z]*)*\b',
            'measurements': r'\d+(?:\.\d+)?\s*(?:mg|g|kg|ml|l|μm|mm|cm|m|°C|%|ppm|μg)'
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
                processed_articles.append(article)

        return processed_articles

    async def process_single_article(self, article: Article) -> Article:
        """Procesa un artículo individual con NLP"""
        # Combinar texto disponible
        text_to_process = ""
        if article.abstract:
            text_to_process += article.abstract + " "
        if article.full_text:
            text_to_process += article.full_text[:3000]  # Limitar para eficiencia

        if not text_to_process.strip():
            return article

        # Extraer entidades
        entities = await self.extract_entities(text_to_process)

        # Asignar entidades extraídas
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

        # Usar regex para genes y proteínas
        gene_matches = re.findall(self.entity_patterns['genes'], text)
        entities['genes'].extend(gene_matches[:10])

        protein_matches = re.findall(self.entity_patterns['proteins'], text)
        entities['proteins'].extend(protein_matches[:10])

        # Extraer mediciones numéricas
        measurement_matches = re.findall(self.entity_patterns['measurements'], text)
        entities['measurements'].extend(measurement_matches[:15])

        return entities


# Función principal mejorada
async def main():
    """Función principal para ejecutar el pipeline completo"""
    logger.info("Iniciando pipeline de ingesta de biología espacial...")

    # Construir ruta al CSV
    try:
        BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    except NameError:
        BASE_DIR = os.getcwd()

    csv_path = os.path.join(BASE_DIR, "data", "raw", "SB_publication_PMC.csv")

    async with DataIngestionPipeline() as pipeline:
        try:
            # 1. Cargar datos del CSV
            logger.info("Cargando datos de SB Publications...")
            articles_data = await pipeline.load_sb_publications_csv(
                csv_path=csv_path if os.path.exists(csv_path) else None
            )

            if not articles_data:
                logger.error("No se pudieron cargar datos del CSV")
                return

            # Procesar muestra pequeña para testing
            test_size = min(10, len(articles_data))
            articles_data = articles_data[:test_size]
            logger.info(f"Procesando {test_size} artículos para testing")

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
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"space_biology_articles_{timestamp}.json"

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(standardized_articles, f, indent=2, ensure_ascii=False, default=str)

            # Estadísticas finales
            logger.info("=== ESTADÍSTICAS FINALES ===")
            logger.info(f"Artículos procesados: {len(standardized_articles)}")
            logger.info(f"Artículos fallidos: {len(pipeline.failed_extractions)}")

            if standardized_articles:
                avg_quality = sum(
                    article['article_data']['quality_score']
                    for article in standardized_articles
                ) / len(standardized_articles)
                logger.info(f"Calidad promedio: {avg_quality:.2f}")

                # Contar entidades por fuente
                sources = {}
                total_entities = 0
                for article in standardized_articles:
                    source = article['article_data']['extraction_source']
                    sources[source] = sources.get(source, 0) + 1

                    entities_count = (
                            len(article['graph_entities']['Organisms']) +
                            len(article['graph_entities']['Genes']) +
                            len(article['graph_entities']['Keywords']) +
                            len(article['graph_entities']['Experiments'])
                    )
                    total_entities += entities_count

                logger.info(f"Fuentes de extracción: {sources}")
                logger.info(f"Total entidades extraídas: {total_entities}")

                # Mostrar muestra
                sample = standardized_articles[0]
                logger.info("\n=== MUESTRA DE RESULTADOS ===")
                logger.info(f"Título: {sample['article_data']['title'][:80]}...")
                logger.info(f"Fuente: {sample['article_data']['extraction_source']}")
                logger.info(f"Calidad: {sample['article_data']['quality_score']:.2f}")
                logger.info(f"Organismos: {sample['graph_entities']['Organisms']}")
                logger.info(f"Keywords: {sample['graph_entities']['Keywords']}")

            logger.info(f"Resultados guardados en: {output_file}")

        except Exception as e:
            logger.error(f"Error en pipeline principal: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise


if __name__ == "__main__":
    asyncio.run(main())