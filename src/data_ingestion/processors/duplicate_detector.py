from typing import Set, Dict
from fuzzywuzzy import fuzz
import logging

logger = logging.getLogger(__name__)


class DuplicateDetector:
    """Detector de artículos duplicados"""

    def __init__(self):
        self.seen_articles: Set[str] = set()
        self.seen_titles: Dict[str, object] = {}
        self.similarity_threshold = 85

    def is_duplicate(self, article) -> bool:
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

    def add_article(self, article):
        """Agrega artículo al registro de vistos"""
        if article.doi:
            self.seen_articles.add(article.doi)
        if article.pmid:
            self.seen_articles.add(article.pmid)
        if article.pmcid:
            self.seen_articles.add(article.pmcid)

        self.seen_titles[article.title.lower()] = article