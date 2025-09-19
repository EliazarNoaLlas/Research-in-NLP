from .base_extractor import BaseExtractor
from bs4 import BeautifulSoup
from typing import Dict, Optional, Tuple, List
import re
from datetime import datetime


class PMCExtractor(BaseExtractor):
    """Extractor específico para artículos de PMC"""

    async def extract(self, article_data: Dict) -> Optional[Dict]:
        """Extrae metadatos de un artículo de PMC"""
        # Implementación específica para PMC
        pass

    def extract_pmcid_from_soup(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae PMCID del HTML"""
        meta_pmcid = soup.find('meta', {'name': 'citation_pmid'})
        if meta_pmcid:
            return f"PMC{meta_pmcid.get('content', '')}"

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

        for link in soup.find_all('a', href=True):
            href = link['href']
            if 'pubmed' in href.lower():
                match = re.search(r'pubmed/(\d+)', href)
                if match:
                    return match.group(1)

        return None