from datetime import datetime
from typing import Optional
import re


def parse_date_string(date_str: str) -> Optional[datetime]:
    """Parse diferentes formatos de fecha"""
    if not date_str:
        return None

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


def extract_publication_date_from_text(text: str) -> Optional[datetime]:
    """Extrae fecha de publicación del texto"""
    date_patterns = [
        r'Published[:\s]+([A-Za-z]+\s+\d{1,2},?\s+\d{4})',
        r'(\d{4}-\d{2}-\d{2})',
        r'([A-Za-z]+\s+\d{1,2},?\s+\d{4})'
    ]

    for pattern in date_patterns:
        match = re.search(pattern, text)
        if match:
            return parse_date_string(match.group(1))

    return None