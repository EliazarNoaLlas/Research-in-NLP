from typing import Dict
import hashlib
from datetime import datetime


class JSONLDGenerator:
    """Generador de JSON-LD para artículos científicos"""

    def __init__(self):
        self.context = {
            "@context": {
                "schema": "https://schema.org/",
                "bio": "http://purl.obolibrary.org/obo/",
                "dcterms": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/"
            }
        }

    def convert_to_json_ld(self, article) -> Dict:
        """Convierte artículo a formato JSON-LD"""
        article_id = self._generate_article_id(article)

        json_ld = {
            "@context": self.context["@context"],
            "@id": article_id,
            "@type": "schema:ScholarlyArticle",
            "schema:name": article.title,
            "schema:url": article.url,
            "dcterms:created": article.publication_date.isoformat() if article.publication_date else None,
            "schema:abstract": article.abstract,
            "schema:text": article.full_text
        }

        self._add_journal(json_ld, article)
        self._add_authors(json_ld, article)
        self._add_identifiers(json_ld, article)
        self._add_keywords(json_ld, article)

        return json_ld

    def _generate_article_id(self, article) -> str:
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

    def _add_journal(self, json_ld: Dict, article):
        """Agrega journal al JSON-LD"""
        if article.journal:
            json_ld["schema:isPartOf"] = {
                "@type": "schema:Periodical",
                "schema:name": article.journal
            }

    def _add_authors(self, json_ld: Dict, article):
        """Agrega autores al JSON-LD"""
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

    def _add_identifiers(self, json_ld: Dict, article):
        """Agrega identificadores al JSON-LD"""
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

    def _add_keywords(self, json_ld: Dict, article):
        """Agrega keywords y términos MeSH al JSON-LD"""
        if article.keywords:
            json_ld["schema:keywords"] = article.keywords

        if article.mesh_terms:
            json_ld["bio:mesh_terms"] = article.mesh_terms