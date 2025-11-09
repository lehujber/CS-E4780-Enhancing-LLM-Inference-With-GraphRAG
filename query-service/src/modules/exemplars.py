# few_shot_exemplars.py
from dataclasses import dataclass
from typing import List
import re

# pip install scikit-learn
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


@dataclass
class Exemplar:
    question: str
    query: str  # a known-good, single-line Cypher


# ---- Seed with your domain examples (edit/extend as you wish) ----
EXEMPLARS: List[Exemplar] = [
    Exemplar(
        "Which scholars won the Nobel Physics prize after 1950?",
        "MATCH (s:Scholar)-[:WON]->(p:Prize) WHERE toLower(p.category)=toLower('physics') AND p.awardYear>1950 RETURN s.knownName, p.category, p.awardYear"
    ),
    Exemplar(
        "List laureates affiliated with University of Cambridge",
        "MATCH (s:Scholar)-[:AFFILIATED_WITH]->(i:Institution) WHERE toLower(i.name) CONTAINS toLower('university of cambridge') RETURN s.knownName, i.name"
    ),
    Exemplar(
        "Show economics prize winners and their award years",
        "MATCH (s:Scholar)-[:WON]->(p:Prize) WHERE toLower(p.category)=toLower('economics') RETURN s.knownName, p.awardYear"
    ),
    Exemplar(
        "Find chemistry laureates born before 1900",
        "MATCH (s:Scholar)-[:WON]->(p:Prize) WHERE toLower(p.category)=toLower('chemistry') AND s.birthDate<'1900-01-01' RETURN s.knownName, s.birthDate, p.awardYear"
    ),
    Exemplar(
        "Who won a medicine prize at Karolinska Institutet?",
        "MATCH (s:Scholar)-[:WON]->(p:Prize) MATCH (s)-[:AFFILIATED_WITH]->(i:Institution) WHERE toLower(p.category)=toLower('medicine') AND toLower(i.name) CONTAINS toLower('karolinska institutet') RETURN s.knownName, i.name, p.awardYear"
    ),
]


class FewShotRetriever:
    def __init__(self, exemplars: List[Exemplar]):
        self.exemplars = exemplars
        self.vectorizer = TfidfVectorizer(ngram_range=(1, 2), min_df=1)
        self.matrix = self.vectorizer.fit_transform([e.question for e in exemplars])

    def top_k(self, question: str, k: int = 3) -> List[Exemplar]:
        qv = self.vectorizer.transform([question])
        sims = cosine_similarity(qv, self.matrix)[0]
        order = sims.argsort()[::-1][:k]
        return [self.exemplars[i] for i in order]


def format_fewshot_block(exemplars: List[Exemplar]) -> str:
    """Format exemplars as a compact prompt block."""
    lines = []
    for i, ex in enumerate(exemplars, 1):
        one_line = re.sub(r"\s+", " ", ex.query.strip())
        lines.append(f"Example {i}:")
        lines.append(f"Question: {ex.question}")
        lines.append(f"Cypher: {one_line}")
        lines.append("")  # blank line
    return "\n".join(lines).strip()


# Module-level retriever (built once).
_retriever = FewShotRetriever(EXEMPLARS)


def get_fewshot_block(question: str, k: int = 3) -> str:
    """Public helper: returns a formatted few-shot block for a given question."""
    examples = _retriever.top_k(question, k=k)
    return format_fewshot_block(examples)


# Optional helpers if you want to mutate exemplars at runtime:
def add_exemplar(question: str, query: str) -> None:
    """Append a new exemplar and rebuild the retriever."""
    global _retriever
    EXEMPLARS.append(Exemplar(question=question, query=query))
    _retriever = FewShotRetriever(EXEMPLARS)


__all__ = [
    "Exemplar",
    "EXEMPLARS",
    "FewShotRetriever",
    "get_fewshot_block",
    "add_exemplar",
    "format_fewshot_block",
]
