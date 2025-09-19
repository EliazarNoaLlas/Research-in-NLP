from dataclasses import dataclass, asdict, field
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4


@dataclass
class Author:
    name: str
    affiliation: Optional[str] = None
    orcid: Optional[str] = None


@dataclass
class Article:
    """Estructura de datos para artículos científicos"""
    title: str
    url: str
    id: str = field(default_factory=lambda: str(uuid4()))
    pmcid: Optional[str] = None
    pmid: Optional[str] = None
    doi: Optional[str] = None
    authors: List[Author] = field(default_factory=list)
    affiliations: List[str] = field(default_factory=list)
    journal: Optional[str] = None
    publication_date: Optional[datetime] = None
    abstract: Optional[str] = None
    full_text: Optional[str] = None
    keywords: List[str] = field(default_factory=list)
    mesh_terms: List[str] = field(default_factory=list)
    organisms: List[str] = field(default_factory=list)
    genes: List[str] = field(default_factory=list)
    proteins: List[str] = field(default_factory=list)
    conditions: List[str] = field(default_factory=list)
    experiments: List[str] = field(default_factory=list)
    measurements: List[str] = field(default_factory=list)
    quality_score: float = 0.0
    extraction_timestamp: datetime = field(default_factory=datetime.now)
    embeddings: Optional[List[float]] = None

    def to_dict(self) -> Dict:
        return asdict(self)

    def to_json(self) -> str:
        import json
        from datetime import datetime

        def json_serializer(obj):
            if isinstance(obj, datetime):
                return obj.isoformat()
            return str(obj)

        return json.dumps(self.to_dict(), default=json_serializer, ensure_ascii=False)

    def __post_init__(self):
        """Inicializar listas si son None"""
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