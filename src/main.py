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
    extraction_source: str = "web_scraping"
    llm_extracted: bool = False
    llm_confidence_score: float = 0.0

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


class LLMDataExtractor:
    """Extractor de datos usando APIs de LLMs gratuitas con fallbacks"""

    def __init__(self):
        self.llm_providers = {
            'deepseek': {
                'url': 'https://api.deepseek.com/v1/chat/completions',
                'api_key_env': 'DEEPSEEK_API_KEY',
                'model': 'deepseek-chat',
                'enabled': True
            },
            'groq': {
                'url': 'https://api.groq.com/openai/v1/chat/completions',
                'api_key_env': 'GROQ_API_KEY',
                'model': 'llama-3.1-8b-instant',
                'enabled': True
            },
            'together': {
                'url': 'https://api.together.xyz/v1/chat/completions',
                'api_key_env': 'TOGETHER_API_KEY',
                'model': 'meta-llama/Llama-3.2-3B-Instruct-Turbo',
                'enabled': True
            },
            'perplexity': {
                'url': 'https://api.perplexity.ai/chat/completions',
                'api_key_env': 'PERPLEXITY_API_KEY',
                'model': 'llama-3.1-sonar-small-128k-online',
                'enabled': True
            }
        }

        self.extraction_prompt = """
You are a scientific data extraction expert specializing in space biology research. 
Analyze the following scientific article text and extract structured information in JSON format.

Text to analyze:
{text}

Please extract and return ONLY a valid JSON object with the following structure:
{{
    "title": "Article title",
    "abstract": "Article abstract or summary",
    "journal": "Journal name if found",
    "authors": [
        {{"name": "Author Name", "affiliation": "Institution if found"}}
    ],
    "keywords": ["keyword1", "keyword2"],
    "organisms": ["organism1", "organism2"],
    "genes": ["gene1", "gene2"],
    "proteins": ["protein1", "protein2"],
    "conditions": ["space condition1", "microgravity", "radiation"],
    "experiments": ["experiment type1", "methodology"],
    "measurements": ["measurement1 with units", "parameter2"],
    "publication_date": "YYYY-MM-DD or null",
    "doi": "DOI if found or null",
    "confidence_score": 0.85
}}

Focus on space biology, microgravity research, and related fields. Return only valid JSON without explanations.
"""

    async def extract_with_llm(self, article_text: str, article_data: Dict) -> Optional[Article]:
        """Extrae datos del artículo usando LLMs con múltiples proveedores de fallback"""

        # Preparar texto para análisis (limitar longitud para APIs)
        text_to_analyze = self._prepare_text_for_llm(article_text, article_data)

        if not text_to_analyze.strip():
            return None

        # Intentar con cada proveedor de LLM
        for provider_name, provider_config in self.llm_providers.items():
            if not provider_config['enabled']:
                continue

            try:
                logger.info(f"Intentando extracción con {provider_name}...")
                extracted_data = await self._call_llm_api(provider_name, provider_config, text_to_analyze)

                if extracted_data:
                    article = self._create_article_from_llm_data(extracted_data, article_data)
                    if article:
                        article.extraction_source = f"llm_{provider_name}"
                        article.llm_extracted = True
                        logger.info(f"Extracción exitosa con {provider_name}")
                        return article

            except Exception as e:
                logger.warning(f"Error con {provider_name}: {e}")
                continue

        logger.warning("Todos los proveedores LLM fallaron, usando extracción básica")
        return None

    def _prepare_text_for_llm(self, article_text: str, article_data: Dict) -> str:
        """Prepara el texto del artículo para análisis LLM"""
        text_parts = []

        # Título
        if article_data.get('title'):
            text_parts.append(f"Title: {article_data['title']}")

        # URL para contexto
        if article_data.get('url'):
            text_parts.append(f"Source URL: {article_data['url']}")

        # Texto del artículo (limitado para APIs)
        if article_text:
            # Tomar los primeros 4000 caracteres para mantener dentro de límites de tokens
            content = article_text[:4000]
            text_parts.append(f"Content: {content}")

        return "\n\n".join(text_parts)

    async def _call_llm_api(self, provider_name: str, provider_config: Dict, text: str) -> Optional[Dict]:
        """Llama a la API del proveedor LLM específico"""

        # Verificar API key
        api_key = os.getenv(provider_config['api_key_env'])
        if not api_key:
            logger.warning(f"API key no encontrada para {provider_name}: {provider_config['api_key_env']}")
            return None

        headers = {
            'Authorization': f'Bearer {api_key}',
            'Content-Type': 'application/json'
        }

        # Preparar payload según el proveedor
        payload = {
            'model': provider_config['model'],
            'messages': [
                {
                    'role': 'user',
                    'content': self.extraction_prompt.format(text=text)
                }
            ],
            'temperature': 0.1,
            'max_tokens': 1000
        }

        # Ajustes específicos por proveedor
        if provider_name == 'groq':
            payload['stream'] = False
        elif provider_name == 'perplexity':
            payload['return_citations'] = False
            payload['return_images'] = False

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                        provider_config['url'],
                        headers=headers,
                        json=payload,
                        timeout=aiohttp.ClientTimeout(total=30)
                ) as response:

                    if response.status == 200:
                        data = await response.json()

                        # Extraer contenido de la respuesta
                        content = self._extract_content_from_response(data)
                        if content:
                            return self._parse_llm_json_response(content)
                    else:
                        logger.warning(f"{provider_name} API error: {response.status}")
                        error_text = await response.text()
                        logger.debug(f"Error details: {error_text}")

        except asyncio.TimeoutError:
            logger.warning(f"Timeout calling {provider_name} API")
        except Exception as e:
            logger.warning(f"Error calling {provider_name} API: {e}")

        return None

    def _extract_content_from_response(self, response_data: Dict) -> Optional[str]:
        """Extrae el contenido de la respuesta del LLM"""
        try:
            if 'choices' in response_data and response_data['choices']:
                choice = response_data['choices'][0]
                if 'message' in choice:
                    return choice['message']['content']
                elif 'text' in choice:
                    return choice['text']
        except Exception as e:
            logger.error(f"Error extracting content from LLM response: {e}")
        return None

    def _parse_llm_json_response(self, content: str) -> Optional[Dict]:
        """Parsea la respuesta JSON del LLM"""
        try:
            # Limpiar contenido para extraer JSON
            content = content.strip()

            # Buscar JSON en el contenido
            json_start = content.find('{')
            json_end = content.rfind('}') + 1

            if json_start != -1 and json_end > json_start:
                json_content = content[json_start:json_end]
                return json.loads(json_content)

        except json.JSONDecodeError as e:
            logger.warning(f"Error parsing LLM JSON response: {e}")
            logger.debug(f"Content was: {content}")
        except Exception as e:
            logger.error(f"Error processing LLM response: {e}")

        return None

    def _create_article_from_llm_data(self, llm_data: Dict, original_data: Dict) -> Optional[Article]:
        """Crea objeto Article desde los datos extraídos por LLM"""
        try:
            # Usar título original o extraído
            title = llm_data.get('title', original_data.get('title', ''))
            if not title:
                return None

            article = Article(
                title=title,
                url=original_data.get('url', ''),
                pmcid=original_data.get('pmcid')
            )

            # Mapear datos extraídos por LLM
            article.abstract = llm_data.get('abstract', '')
            article.journal = llm_data.get('journal', '')
            article.doi = llm_data.get('doi')
            article.llm_confidence_score = float(llm_data.get('confidence_score', 0.0))

            # Procesar fecha de publicación
            pub_date = llm_data.get('publication_date')
            if pub_date and pub_date != 'null':
                try:
                    article.publication_date = datetime.strptime(pub_date, '%Y-%m-%d')
                except:
                    pass

            # Procesar autores
            authors_data = llm_data.get('authors', [])
            if isinstance(authors_data, list):
                article.authors = [
                    {
                        'name': author.get('name', '') if isinstance(author, dict) else str(author),
                        'affiliation': author.get('affiliation') if isinstance(author, dict) else None
                    }
                    for author in authors_data
                    if author
                ]

            # Procesar listas de entidades
            list_fields = ['keywords', 'organisms', 'genes', 'proteins', 'conditions', 'experiments', 'measurements']
            for field in list_fields:
                field_data = llm_data.get(field, [])
                if isinstance(field_data, list):
                    setattr(article, field, [str(item) for item in field_data if item])

            article.quality_score = self._calculate_llm_quality_score(article)
            return article

        except Exception as e:
            logger.error(f"Error creating article from LLM data: {e}")
            return None

    def _calculate_llm_quality_score(self, article: Article) -> float:
        """Calcula score de calidad para artículo extraído por LLM"""
        score = 0.0

        # Título (obligatorio)
        if article.title and len(article.title) > 10:
            score += 0.2

        # Abstract
        if article.abstract and len(article.abstract) > 50:
            score += 0.3

        # Metadatos
        if article.journal:
            score += 0.1
        if article.doi:
            score += 0.1
        if article.authors:
            score += 0.1

        # Entidades extraídas
        entities_count = (
                len(article.keywords or []) +
                len(article.organisms or []) +
                len(article.genes or []) +
                len(article.conditions or [])
        )

        if entities_count > 5:
            score += 0.15
        elif entities_count > 0:
            score += 0.1

        # Bonus por confianza del LLM
        if article.llm_confidence_score > 0.8:
            score += 0.05

        return min(score, 1.0)


class DataIngestionPipeline:
    """Pipeline completo de ingesta de datos científicos con integración LLM"""

    def __init__(self):
        self.session = None
        self.processed_articles = []
        self.failed_extractions = []
        self.duplicate_detector = DuplicateDetector()
        self.request_delay = 3.0
        self.llm_extractor = LLMDataExtractor()
        self.llm_enabled = True
        self.test_mode = True

        # Cache para requests
        self.cached_session = requests_cache.CachedSession(
            'space_biology_cache',
            expire_after=3600
        )

        # Headers para web scraping
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }

    async def __aenter__(self):
        """Context manager para gestión de sesión async"""
        connector = aiohttp.TCPConnector(
            limit=5,
            limit_per_host=2,
            ttl_dns_cache=300,
            use_dns_cache=True,
            keepalive_timeout=60,
            enable_cleanup_closed=True
        )

        timeout = aiohttp.ClientTimeout(
            total=120,
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
            await asyncio.sleep(1)

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
        """Extracción de artículos con integración LLM"""
        extracted_articles = []
        semaphore = asyncio.Semaphore(1)  # Muy conservador para LLMs

        # Mezclar orden para evitar patrones
        articles_data = articles_data.copy()
        random.shuffle(articles_data)

        async def extract_single_article_wrapper(article_data):
            async with semaphore:
                try:
                    # Delay para respetar rate limits
                    delay = random.uniform(5, 10)  # Más tiempo por LLM calls
                    await asyncio.sleep(delay)

                    article = await self.extract_with_llm_and_fallbacks(article_data)
                    return article

                except Exception as e:
                    logger.error(f"Error extrayendo {article_data.get('title', 'Sin título')}: {e}")
                    self.failed_extractions.append(article_data)
                    return None

        # Procesamiento en lotes muy pequeños
        batch_size = 2  # Reducido para LLM processing
        for i in range(0, len(articles_data), batch_size):
            batch = articles_data[i:i + batch_size]
            logger.info(f"Procesando lote {i // batch_size + 1}/{(len(articles_data) + batch_size - 1) // batch_size}")

            tasks = [extract_single_article_wrapper(data) for data in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for result in results:
                if isinstance(result, Article):
                    extracted_articles.append(result)
                elif isinstance(result, Exception):
                    logger.error(f"Excepción en lote: {result}")

            # Pausa más larga entre lotes para LLM rate limits
            if i + batch_size < len(articles_data):
                batch_delay = random.uniform(15, 25)
                logger.info(f"Esperando {batch_delay:.1f}s entre lotes...")
                await asyncio.sleep(batch_delay)

        logger.info(f"Extraídos {len(extracted_articles)} artículos exitosamente")
        logger.info(f"Fallaron {len(self.failed_extractions)} extracciones")

        # Estadísticas LLM
        llm_extracted = sum(1 for a in extracted_articles if a.llm_extracted)
        logger.info(f"Artículos extraídos con LLM: {llm_extracted}/{len(extracted_articles)}")

        return extracted_articles

    async def extract_with_llm_and_fallbacks(self, article_data: Dict) -> Optional[Article]:
        """Extrae artículo con LLM como método principal y fallbacks tradicionales"""

        url = article_data.get('url', '')
        title = article_data.get('title', '')

        # Primer paso: Obtener contenido HTML
        article_text = await self.fetch_article_content(url)

        if not article_text and not title:
            return None

        # Método principal: Extracción con LLM
        if self.llm_enabled and (article_text or title):
            try:
                text_for_llm = article_text or title
                llm_article = await self.llm_extractor.extract_with_llm(text_for_llm, article_data)
                if llm_article and llm_article.quality_score > 0.3:
                    logger.info(f"Extracción LLM exitosa: {title[:50]}...")
                    return llm_article
            except Exception as e:
                logger.warning(f"Extracción LLM falló para {title[:50]}...: {e}")

        # Fallback 1: APIs públicas (PMC, PubMed)
        pmcid = article_data.get('pmcid')
        if pmcid:
            api_article = await self.try_pubmed_central_api(pmcid, title, url)
            if api_article:
                api_article.extraction_source = "pmc_api_fallback"
                return api_article

        # Fallback 2: Extracción básica de HTML
        if article_text:
            basic_article = await self.basic_html_extraction(article_text, article_data)
            if basic_article:
                basic_article.extraction_source = "html_fallback"
                return basic_article

        # Fallback 3: Artículo mínimo
        if self.test_mode:
            return self.create_minimal_article(article_data)

        return None

    async def fetch_article_content(self, url: str) -> Optional[str]:
        """Obtiene contenido del artículo desde URL"""
        try:
            async with self.session.get(url) as response:
                if response.status == 200:
                    html_content = await response.text()
                    soup = BeautifulSoup(html_content, 'html.parser')

                    # Extraer texto relevante
                    text_content = self.extract_text_from_soup(soup)
                    return text_content
                else:
                    logger.warning(f"Error HTTP {response.status} para {url}")

        except Exception as e:
            logger.warning(f"Error obteniendo contenido de {url}: {e}")

        return None

    def extract_text_from_soup(self, soup: BeautifulSoup) -> str:
        """Extrae texto relevante del HTML"""
        try:
            # Remover scripts y estilos
            for script in soup(["script", "style", "nav", "footer", "header"]):
                script.decompose()

            # Buscar secciones principales del artículo
            main_content = ""

            # Intentar abstract
            for selector in ['.abstract', '#abstract', '.article-abstract', '.summary']:
                element = soup.select_one(selector)
                if element:
                    main_content += f"Abstract: {element.get_text().strip()}\n\n"
                    break

            # Intentar contenido principal
            for selector in ['.article-content', '.main-content', '.content', 'main', 'article']:
                element = soup.select_one(selector)
                if element:
                    content_text = element.get_text().strip()
                    if len(content_text) > 500:
                        main_content += content_text[:3000]  # Limitar para LLM
                        break

            # Si no encuentra contenido específico, usar body
            if not main_content.strip():
                body = soup.find('body')
                if body:
                    main_content = body.get_text()[:3000]

            return main_content.strip()

        except Exception as e:
            logger.error(f"Error extrayendo texto de HTML: {e}")
            return ""

    async def basic_html_extraction(self, article_text: str, article_data: Dict) -> Optional[Article]:
        """Extracción básica de HTML sin LLM"""
        try:
            article = Article(
                title=article_data.get('title', ''),
                url=article_data.get('url', ''),
                pmcid=article_data.get('pmcid')
            )

            # Usar texto como abstract si es corto
            if len(article_text) < 1000:
                article.abstract = article_text
            else:
                # Intentar extraer abstract del texto
                lines = article_text.split('\n')
                for line in lines:
                    if 'abstract' in line.lower() and len(line) > 100:
                        article.abstract = line
                        break

                if not article.abstract:
                    article.abstract = article_text[:500] + "..."

            # Extracción básica de entidades
            article.organisms = self.extract_basic_organisms(article_text)
            article.keywords = self.extract_basic_keywords(article_text)
            article.conditions = self.extract_basic_conditions(article_text)

            article.quality_score = self.calculate_quality_score(article)
            return article if article.quality_score > 0.2 else None

        except Exception as e:
            logger.error(f"Error en extracción básica HTML: {e}")
            return None

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

        return list(set(keywords))

    def extract_basic_conditions(self, text: str) -> List[str]:
        """Extrae condiciones experimentales básicas"""
        conditions = []
        condition_keywords = [
            'microgravity', 'simulated microgravity', 'centrifuge',
            'space environment', 'radiation exposure', 'isolation',
            'confined space', 'altered gravity'
        ]

        text_lower = text.lower()
        for condition in condition_keywords:
            if condition in text_lower:
                conditions.append(condition)

        return list(set(conditions))

    async def try_pubmed_central_api(self, pmcid: str, title: str, url: str) -> Optional[Article]:
        """Intenta extraer usando APIs públicas de PMC"""
        try:
            api_base = "https://www.ncbi.nlm.nih.gov/research/bionlp/RESTful/pmcoa.cgi"
            api_url = f"{api_base}/BioC_json/{pmcid}/unicode"

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

            article.organisms = self.extract_basic_organisms(title + " " + (article.abstract or ""))
            article.keywords = self.extract_basic_keywords(title + " " + (article.abstract or ""))

            article.quality_score = self.calculate_quality_score(article)
            return article if article.quality_score > 0.2 else None

        except Exception as e:
            logger.error(f"Error parseando respuesta API PMC: {e}")
            return None

    def create_minimal_article(self, article_data: Dict) -> Article:
        """Crea artículo mínimo con datos disponibles"""
        article = Article(
            title=article_data.get('title', ''),
            url=article_data.get('url', ''),
            pmcid=article_data.get('pmcid')
        )

        article.abstract = f"Research article: {article.title}"
        article.organisms = self.extract_basic_organisms(article.title)
        article.keywords = self.extract_basic_keywords(article.title)
        article.extraction_source = "minimal_metadata"

        article.quality_score = self.calculate_quality_score(article)
        return article

    def calculate_quality_score(self, article: Article) -> float:
        """Calcula score de calidad del artículo"""
        score = 0.0

        if article.title and len(article.title) > 10:
            score += 0.3

        if article.abstract and len(article.abstract) > 50:
            score += 0.3
        elif article.full_text and len(article.full_text) > 100:
            score += 0.2

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

        # Bonus para extracciones LLM exitosas
        if article.llm_extracted and article.llm_confidence_score > 0.7:
            score += 0.1

        return min(score, 1.0)

    def validate_data_quality(self, articles: List[Article]) -> List[Article]:
        """Validación de calidad de datos"""
        validated_articles = []
        quality_threshold = 0.2

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
        if not article.title or len(article.title.strip()) < 5:
            return False

        if not article.url and article.extraction_source != "test_data":
            return False

        return True

    def standardize_format(self, articles: List[Article]) -> List[Dict]:
        """Convierte artículos a formato estándar JSON-LD"""
        standardized_articles = []

        for article in articles:
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
            "extraction_source": article.extraction_source,
            "llm_extracted": article.llm_extracted,
            "llm_confidence_score": article.llm_confidence_score
        }

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

        # Agregar información específica de LLM
        g.add((article_uri, SPACE.llmExtracted, Literal(article.llm_extracted)))
        if article.llm_confidence_score > 0:
            g.add((article_uri, SPACE.llmConfidenceScore, Literal(article.llm_confidence_score)))

        if article.journal:
            journal_uri = URIRef(
                f"http://space-biology.org/journal/{hashlib.md5(article.journal.encode()).hexdigest()}")
            g.add((journal_uri, RDF.type, SCHEMA.Periodical))
            g.add((journal_uri, SCHEMA.name, Literal(article.journal)))
            g.add((article_uri, SCHEMA.isPartOf, journal_uri))

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

        if article.doi:
            g.add((article_uri, SCHEMA.doi, Literal(article.doi)))
        if article.pmid:
            g.add((article_uri, BIO.pmid, Literal(article.pmid)))
        if article.pmcid:
            g.add((article_uri, BIO.pmcid, Literal(article.pmcid)))

        for keyword in (article.keywords or []):
            keyword_uri = URIRef(f"http://space-biology.org/keyword/{hashlib.md5(keyword.encode()).hexdigest()}")
            g.add((keyword_uri, RDF.type, SCHEMA.DefinedTerm))
            g.add((keyword_uri, SCHEMA.name, Literal(keyword)))
            g.add((article_uri, SCHEMA.keywords, keyword_uri))

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
                'extraction_source': article.extraction_source,
                'llm_extracted': article.llm_extracted,
                'llm_confidence_score': article.llm_confidence_score
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
        if article.doi and article.doi in self.seen_articles:
            return True
        if article.pmid and article.pmid in self.seen_articles:
            return True
        if article.pmcid and article.pmcid in self.seen_articles:
            return True

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

        self.entity_patterns = {
            'genes': r'\b[A-Z][A-Z0-9]{2,}(?:-[A-Z0-9]+)?\b',
            'proteins': r'\b[A-Z][a-z]+(?:-\d+)?(?:\s+[A-Z][a-z]*)*\b',
            'measurements': r'\d+(?:\.\d+)?\s*(?:mg|g|kg|ml|l|μm|mm|cm|m|°C|%|ppm|μg)'
        }

    async def process_articles(self, articles: List[Article]) -> List[Article]:
        """Procesa lista de artículos con NLP adicional para artículos no LLM"""
        processed_articles = []

        for article in articles:
            try:
                # Solo aplicar NLP adicional si no fue extraído por LLM
                if not article.llm_extracted:
                    processed_article = await self.process_single_article(article)
                    processed_articles.append(processed_article)
                else:
                    processed_articles.append(article)
                logger.info(f"Procesado NLP: {article.title[:50]}...")
            except Exception as e:
                logger.error(f"Error procesando NLP en artículo {article.title}: {e}")
                processed_articles.append(article)

        return processed_articles

    async def process_single_article(self, article: Article) -> Article:
        """Procesa un artículo individual con NLP"""
        text_to_process = ""
        if article.abstract:
            text_to_process += article.abstract + " "
        if article.full_text:
            text_to_process += article.full_text[:3000]

        if not text_to_process.strip():
            return article

        entities = await self.extract_entities(text_to_process)

        article.organisms.extend(entities.get('organisms', []))
        article.genes.extend(entities.get('genes', []))
        article.proteins.extend(entities.get('proteins', []))
        article.conditions.extend(entities.get('conditions', []))
        article.experiments.extend(entities.get('experiments', []))
        article.measurements.extend(entities.get('measurements', []))

        # Remover duplicados
        for field in ['organisms', 'genes', 'proteins', 'conditions', 'experiments', 'measurements']:
            setattr(article, field, list(set(getattr(article, field))))

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

        for organism in self.space_biology_entities['organisms']:
            if organism in text_lower:
                entities['organisms'].append(organism)

        for condition in self.space_biology_entities['space_conditions']:
            if condition in text_lower:
                entities['conditions'].append(condition)

        for system in self.space_biology_entities['biological_systems']:
            if system in text_lower:
                entities['conditions'].append(system)

        for experiment in self.space_biology_entities['experiments']:
            if experiment in text_lower:
                entities['experiments'].append(experiment)

        gene_matches = re.findall(self.entity_patterns['genes'], text)
        entities['genes'].extend(gene_matches[:10])

        protein_matches = re.findall(self.entity_patterns['proteins'], text)
        entities['proteins'].extend(protein_matches[:10])

        measurement_matches = re.findall(self.entity_patterns['measurements'], text)
        entities['measurements'].extend(measurement_matches[:15])

        return entities


# Función principal mejorada con integración LLM
async def main():
    """Función principal para ejecutar el pipeline completo con LLM"""
    logger.info("Iniciando pipeline de ingesta de biología espacial con integración LLM...")

    # Verificar configuración de LLMs
    logger.info("Verificando configuración de APIs LLM...")
    available_llms = []
    for provider, config in {
        'DEEPSEEK_API_KEY': 'DeepSeek',
        'GROQ_API_KEY': 'Groq',
        'TOGETHER_API_KEY': 'Together AI',
        'PERPLEXITY_API_KEY': 'Perplexity'
    }.items():
        if os.getenv(provider):
            available_llms.append(config)
        else:
            logger.warning(f"API key no configurada para {config}: {provider}")

    if available_llms:
        logger.info(f"LLMs disponibles: {', '.join(available_llms)}")
    else:
        logger.warning("No hay APIs LLM configuradas. El pipeline usará métodos de fallback.")

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

            # Procesar muestra para testing
            test_size = min(5, len(articles_data))  # Reducido por uso de LLM
            articles_data = articles_data[:test_size]
            logger.info(f"Procesando {test_size} artículos para testing con LLM")

            # 2. Extraer artículos con LLM
            logger.info("Extrayendo artículos con integración LLM...")
            extracted_articles = await pipeline.extract_papers(articles_data)

            # 3. Validar calidad
            logger.info("Validando calidad de datos...")
            validated_articles = pipeline.validate_data_quality(extracted_articles)

            # 4. Procesamiento NLP adicional
            logger.info("Iniciando procesamiento NLP complementario...")
            nlp_processor = BiologyNLPProcessor()
            processed_articles = await nlp_processor.process_articles(validated_articles)

            # 5. Estandarizar formato
            logger.info("Estandarizando formato...")
            standardized_articles = pipeline.standardize_format(processed_articles)

            # 6. Guardar resultados
            logger.info("Guardando resultados...")
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f"space_biology_articles_llm_{timestamp}.json"

            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(standardized_articles, f, indent=2, ensure_ascii=False, default=str)

            # Estadísticas finales mejoradas
            logger.info("=== ESTADÍSTICAS FINALES ===")
            logger.info(f"Artículos procesados: {len(standardized_articles)}")
            logger.info(f"Artículos fallidos: {len(pipeline.failed_extractions)}")

            if standardized_articles:
                # Estadísticas por método de extracción
                extraction_methods = {}
                llm_extracted_count = 0
                total_confidence = 0.0

                for article in standardized_articles:
                    article_data = article['article_data']
                    source = article_data['extraction_source']
                    extraction_methods[source] = extraction_methods.get(source, 0) + 1

                    if article_data['llm_extracted']:
                        llm_extracted_count += 1
                        total_confidence += article_data['llm_confidence_score']

                logger.info(f"Métodos de extracción: {extraction_methods}")
                logger.info(f"Artículos extraídos con LLM: {llm_extracted_count}/{len(standardized_articles)}")

                if llm_extracted_count > 0:
                    avg_llm_confidence = total_confidence / llm_extracted_count
                    logger.info(f"Confianza LLM promedio: {avg_llm_confidence:.2f}")

                avg_quality = sum(
                    article['article_data']['quality_score']
                    for article in standardized_articles
                ) / len(standardized_articles)
                logger.info(f"Calidad promedio: {avg_quality:.2f}")

                # Contar entidades extraídas
                total_entities = 0
                entity_types = {}
                for article in standardized_articles:
                    entities = article['graph_entities']
                    for entity_type, entity_list in entities.items():
                        if isinstance(entity_list, list):
                            count = len(entity_list)
                            entity_types[entity_type] = entity_types.get(entity_type, 0) + count
                            total_entities += count

                logger.info(f"Total entidades extraídas: {total_entities}")
                logger.info(f"Entidades por tipo: {entity_types}")

                # Mostrar muestra mejorada
                sample = standardized_articles[0]
                article_sample = sample['article_data']
                logger.info("\n=== MUESTRA DE RESULTADOS ===")
                logger.info(f"Título: {article_sample['title'][:80]}...")
                logger.info(f"Fuente: {article_sample['extraction_source']}")
                logger.info(f"LLM extraído: {article_sample['llm_extracted']}")
                if article_sample['llm_extracted']:
                    logger.info(f"Confianza LLM: {article_sample['llm_confidence_score']:.2f}")
                logger.info(f"Calidad: {article_sample['quality_score']:.2f}")
                logger.info(f"Organismos: {sample['graph_entities']['Organisms']}")
                logger.info(f"Keywords: {sample['graph_entities']['Keywords']}")
                logger.info(f"Condiciones: {sample['graph_entities']['Conditions']}")

            logger.info(f"Resultados guardados en: {output_file}")

        except Exception as e:
            logger.error(f"Error en pipeline principal: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise


if __name__ == "__main__":
    asyncio.run(main())