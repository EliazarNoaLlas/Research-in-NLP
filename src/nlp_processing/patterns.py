# Patrones de expresiones regulares para extracción de entidades
ENTITY_PATTERNS = {
    'genes': r'\b[A-Z][A-Z0-9]{2,}(?:-[A-Z0-9]+)?\b',
    'proteins': r'\b[A-Z][a-z]+(?:-\d+)?(?:\s+[A-Z][a-z]*)*\b',
    'measurements': r'\d+(?:\.\d+)?\s*(?:mg|g|kg|ml|l|μm|mm|cm|m|°C|%|ppm|μg)',
    'time_periods': r'\d+\s*(?:days?|weeks?|months?|years?|hrs?|hours?|mins?|minutes?)'
}

# Diccionarios de entidades biológicas específicas de biología espacial
SPACE_BIOLOGY_ENTITIES = {
    'organisms': {
        'human', 'mouse', 'mice', 'rat', 'drosophila', 'arabidopsis',
        'caenorhabditis elegans', 'c. elegans', 'saccharomyces cerevisiae',
        'escherichia coli', 'e. coli', 'bacillus subtilis'
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
    },
    'measurements': {
        'body mass', 'bone density', 'muscle strength', 'heart rate',
        'blood pressure', 'hormone levels', 'gene expression levels',
        'protein concentration', 'cell viability', 'proliferation rate'
    }
}