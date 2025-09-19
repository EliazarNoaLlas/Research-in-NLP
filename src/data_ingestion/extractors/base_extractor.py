from abc import ABC, abstractmethod
from typing import Dict, List, Optional
from bs4 import BeautifulSoup
import re


class BaseExtractor(ABC):
    """Clase base para extractores de artículos"""

    @abstractmethod
    async def extract(self, article_data: Dict) -> Optional[Dict]:
        pass

    def extract_pmcid_from_url(self, url: str) -> Optional[str]:
        """Extrae PMCID de URL de PMC"""
        if 'pmc/articles/PMC' in url:
            match = re.search(r'PMC(\d+)', url)
            if match:
                return f"PMC{match.group(1)}"
        return None

    def extract_doi_from_soup(self, soup: BeautifulSoup) -> Optional[str]:
        """Extrae DOI del HTML"""
        meta_doi = soup.find('meta', {'name': 'citation_doi'})
        if meta_doi:
            return meta_doi.get('content')

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