# --- deps ---
import os
from typing import Any
from pydantic import BaseModel, Field
from dotenv import load_dotenv

import dspy
import kuzu  # pip install kuzu

from .exemplars import get_fewshot_block
# --- LM config (OpenRouter example; swap to your provider/model as needed) ---
load_dotenv()
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
lm = dspy.LM(
    model="openrouter/google/gemini-2.0-flash-001",
    api_base="https://openrouter.ai/api/v1",
    api_key=OPENROUTER_API_KEY,
)
dspy.configure(lm=lm)

# --- Kuzu schema extraction ---
def get_schema_dict(conn: kuzu.Connection) -> dict[str, list[dict]]:
    schema = {"nodes": [], "edges": []}

    # node labels
    resp = conn.execute("CALL SHOW_TABLES() WHERE type = 'NODE' RETURN *;")
    node_labels = [row[1] for row in resp]  # row[1] is table name

    # rel labels + endpoints
    resp = conn.execute("CALL SHOW_TABLES() WHERE type = 'REL' RETURN *;")
    rel_labels = [row[1] for row in resp]
    rels = []
    for r in rel_labels:
        cr = conn.execute(f"CALL SHOW_CONNECTION('{r}') RETURN *;")
        for row in cr:
            rels.append({"name": r, "from": row[0], "to": row[1]})

    # node properties
    for lbl in node_labels:
        props = []
        info = conn.execute(f"CALL TABLE_INFO('{lbl}') RETURN *;")
        for row in info:
            # row[1]=name, row[2]=type
            props.append({"name": row[1], "type": row[2]})
        schema["nodes"].append({"label": lbl, "properties": props})

    # rel properties
    for rel in rels:
        props = []
        info = conn.execute(f"CALL TABLE_INFO('{rel['name']}') RETURN *;")
        for row in info:
            props.append({"name": row[1], "type": row[2]})
        schema["edges"].append(
            {
                "label": rel["name"],
                "from": rel["from"],
                "to": rel["to"],
                "properties": props,
            }
        )
    return schema

# --- Pydantic models for structured IO (used by DSPy) ---
class Query(BaseModel):
    query: str = Field(description="Valid Cypher query with no newlines")

class Property(BaseModel):
    name: str
    type: str = Field(description="Data type of the property")

class Node(BaseModel):
    label: str
    properties: list[Property] | None = None

class Edge(BaseModel):
    label: str = Field(description="Relationship label")
    from_: Node = Field(alias="from", description="Source node label")
    to: Node = Field(alias="to", description="Target node label")
    properties: list[Property] | None = None

class GraphSchema(BaseModel):
    nodes: list[Node]
    edges: list[Edge]

# --- DSPy Signatures ---
class PruneSchema(dspy.Signature):
    """
    Return ONLY the subset of the labelled property graph schema relevant to the question.
    Include only nodes/edges/properties needed to answer the question.
    """
    question: str = dspy.InputField()
    input_schema: Any = dspy.InputField()         # dict is fine
    pruned_schema: GraphSchema = dspy.OutputField()

class Text2Cypher(dspy.Signature):
    """
    Translate the question into a valid Cypher query that respects the (pruned) schema.

    <SYNTAX>
    - Match scholar names on `knownName`.
    - For countries/cities/continents/institutions, match on `name`.
    - Use short alphanumeric variable names (a1, r1, etc.).
    - Respect relationship directions (FROM -> TO).
    - For string comparisons: lowercase both sides, use WHERE + CONTAINS.
    - Do NOT use APOC.
    </SYNTAX>

    Use the FEW-SHOT EXAMPLES to imitate structure and style.

    <RETURN_RESULTS>
    - Return property values (not whole nodes/edges).
    - Integers as integers.
    - Do not coerce types.
    - No extraneous keywords outside the query.
    </RETURN_RESULTS>
    """
    question: str = dspy.InputField()
    input_schema: Any = dspy.InputField()
    fewshot_examples: str = dspy.InputField()   # NEW
    query: Query = dspy.OutputField()

# --- Compiled modules ---
_prune = dspy.Predict(PruneSchema)
_text2cypher = dspy.Predict(Text2Cypher)

def generate_cypher(question: str, conn: kuzu.Connection) -> tuple[str, dict]:
    """
    1) Extract full schema from Kuzu
    2) Prune schema w.r.t. the question
    3) Select few-shot exemplars based on similarity
    4) Generate Cypher from pruned schema + few-shot context
    """
    full_schema = get_schema_dict(conn)
    pruned = _prune(question=question, input_schema=full_schema).pruned_schema.model_dump()

    fewshot_block = get_fewshot_block(question, k=3)

    cy = _text2cypher(
        question=question,
        input_schema=pruned,
        fewshot_examples=fewshot_block,  
    ).query.query

    return cy, pruned

if __name__ == "__main__":
    db = kuzu.Database("nobel.kuzu", read_only=True)
    conn = kuzu.Connection(db)
    cypher, pruned = generate_cypher(
        "Which scholars won prizes in Physics and were affiliated with University of Cambridge?",
        conn,
    )
    print("PRUNED SCHEMA:", pruned)
    print("GENERATED CYPHER:", cypher)
