import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent.parent

# Configuración de API
PUBMED_API_BASE = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/"
PMC_BASE_URL = "https://www.ncbi.nlm.nih.gov/pmc/articles/"

# Límites y timeouts
REQUEST_TIMEOUT = 60
MAX_CONCURRENT_REQUESTS = 5
RATE_LIMIT_DELAY = 0.5

# Umbrales de calidad
QUALITY_THRESHOLD = 0.5
SIMILARITY_THRESHOLD = 85

# Paths de datos
DATA_DIR = BASE_DIR / "data"
CACHE_PATH = DATA_DIR / "cache" / "space_biology_cache"
OUTPUT_DIR = DATA_DIR / "outputs"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Headers para web scraping
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
}

# URLs de datos
SB_PUBLICATIONS_CSV_URL = "https://raw.githubusercontent.com/jgalazka/SB_publications/main/SB_publications_PMC.csv"