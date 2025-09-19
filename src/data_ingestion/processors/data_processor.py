from typing import List
from ..models.article import Article
import logging

logger = logging.getLogger(__name__)


class DataProcessor:
    """Procesador de datos para validación y estandarización"""

    def __init__(self, quality_threshold: float = 0.5):
        self.quality_threshold = quality_threshold

    def validate_data_quality(self, articles: List[Article]) -> List[Article]:
        """Validación de calidad de datos"""
        validated_articles = []

        for article in articles:
            article.quality_score = self.calculate_quality_score(article)

            if self.is_valid_article(article):
                if article.quality_score >= self.quality_threshold:
                    validated_articles.append(article)
                else:
                    logger.warning(f"Artículo con baja calidad descartado: {article.title}")
            else:
                logger.warning(f"Artículo inválido descartado: {article.title}")

        logger.info(f"Validados {len(validated_articles)} de {len(articles)} artículos")
        return validated_articles

    def calculate_quality_score(self, article: Article) -> float:
        """Calcula score de calidad del artículo"""
        score = 0.0
        max_score = 10.0

        if article.title and len(article.title) > 10:
            score += 1.0

        if article.abstract and len(article.abstract) > 100:
            score += 2.0

        if article.full_text and len(article.full_text) > 1000:
            score += 2.0

        if article.pmid or article.pmcid:
            score += 1.0

        if article.doi:
            score += 1.0

        if article.journal:
            score += 1.0

        if article.publication_date:
            score += 0.5

        if article.authors and len(article.authors) > 0:
            score += 1.0

        if (article.keywords and len(article.keywords) > 0) or (article.mesh_terms and len(article.mesh_terms) > 0):
            score += 0.5

        return min(score / max_score, 1.0)

    def is_valid_article(self, article: Article) -> bool:
        """Valida si un artículo cumple criterios mínimos"""
        if not article.title or len(article.title.strip()) < 10:
            return False

        if not article.url or not self.is_valid_url(article.url):
            return False

        if not article.abstract and not article.full_text:
            return False

        if article.publication_date:
            current_year = datetime.now().year
            pub_year = article.publication_date.year
            if pub_year > current_year or pub_year < 1950:
                return False

        return True

    def is_valid_url(self, url: str) -> bool:
        """Valida si una URL es válida"""
        from urllib.parse import urlparse
        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except:
            return False