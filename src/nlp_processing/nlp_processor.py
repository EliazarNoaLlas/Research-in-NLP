import spacy
import re
from typing import Dict, List, Optional
import logging
from transformers import AutoTokenizer, AutoModel
import torch

from .patterns import ENTITY_PATTERNS, SPACE_BIOLOGY_ENTITIES

logger = logging.getLogger(__name__)


class BiologyNLPProcessor:
    """Procesador NLP especializado en biología espacial"""

    def __init__(self):
        self.scispacy_model = self._load_scispacy()
        self.tokenizer, self.biobert_model = self._load_biobert()

    def _load_scispacy(self):
        """Carga modelo ScispaCy"""
        try:
            return spacy.load("en_core_sci_lg")
        except OSError:
            logger.warning("ScispaCy modelo no encontrado")
            return None

    def _load_biobert(self):
        """Carga modelo BioBERT"""
        try:
            tokenizer = AutoTokenizer.from_pretrained('dmis-lab/biobert-v1.1')
            model = AutoModel.from_pretrained('dmis-lab/biobert-v1.1')
            return tokenizer, model
        except Exception as e:
            logger.warning(f"Error cargando BioBERT: {e}")
            return None, None

    async def process_articles(self, articles: List) -> List:
        """Procesa lista de artículos con NLP"""
        processed_articles = []

        for article in articles:
            try:
                processed_article = await self.process_single_article(article)
                processed_articles.append(processed_article)
            except Exception as e:
                logger.error(f"Error procesando NLP: {e}")
                processed_articles.append(article)

        return processed_articles

    async def process_single_article(self, article) -> Dict:
        """Procesa un artículo individual con NLP"""
        text_to_process = self._prepare_text(article)

        if not text_to_process.strip():
            return article

        entities = await self.extract_entities(text_to_process)
        self._assign_entities(article, entities)

        return article

    def _prepare_text(self, article) -> str:
        """Prepara texto para procesamiento NLP"""
        text = ""
        if article.abstract:
            text += article.abstract + " "
        if article.full_text:
            text += article.full_text[:5000]
        return text

    def _assign_entities(self, article, entities: Dict):
        """Asigna entidades extraídas al artículo"""
        for entity_type, values in entities.items():
            if hasattr(article, entity_type):
                current_values = getattr(article, entity_type)
                current_values.extend(values)
                setattr(article, entity_type, list(set(current_values)))

    async def extract_entities(self, text: str) -> Dict[str, List[str]]:
        """Extrae entidades biológicas del texto"""
        entities = {key: [] for key in SPACE_BIOLOGY_ENTITIES.keys()}
        entities.update({'genes': [], 'proteins': [], 'measurements': []})

        text_lower = text.lower()

        # Extraer entidades basadas en diccionarios
        for entity_type, terms in SPACE_BIOLOGY_ENTITIES.items():
            for term in terms:
                if term in text_lower:
                    entities[entity_type].append(term)

        # Extraer con regex
        entities['genes'].extend(re.findall(ENTITY_PATTERNS['genes'], text)[:10])
        entities['proteins'].extend(re.findall(ENTITY_PATTERNS['proteins'], text)[:10])
        entities['measurements'].extend(re.findall(ENTITY_PATTERNS['measurements'], text)[:15])

        # Mejorar con ScispaCy si está disponible
        if self.scispacy_model:
            entities = await self.enhance_with_scispacy(text, entities)

        return entities

    async def enhance_with_scispacy(self, text: str, entities: Dict) -> Dict:
        """Mejora extracción con ScispaCy"""
        try:
            doc = self.scispacy_model(text[:3000])

            for ent in doc.ents:
                entity_text = ent.text.lower().strip()

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