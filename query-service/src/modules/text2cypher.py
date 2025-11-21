# --- deps ---
import re
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
    "openrouter/google/gemini-2.0-flash-001",
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

class RepairCypher(dspy.Signature):
    """
    The previous Cypher query was invalid. Fix it based on the error message.
    Ensure the query respects the schema and syntax rules.
    
    <HINTS>
    - All the dates in the database are strings.
    - If you need to do a comparison on a date property, convert it using `date(<val>)`.
    - Use `date_part('year', date(<val>))` instead of `year(val)`. Expected: (STRING,DATE) -> INT64 (Return type)
    - Do not use `datetime().year`.
    - Use `date()` to get current date if needed, but prefer matching on properties.
    </HINTS>
    """
    question: str = dspy.InputField()
    input_schema: Any = dspy.InputField()
    full_schema: Any = dspy.InputField()
    invalid_query: str = dspy.InputField()
    error_message: str = dspy.InputField()
    query: Query = dspy.OutputField()

# --- Compiled modules ---
_prune = dspy.Predict(PruneSchema)
_text2cypher = dspy.Predict(Text2Cypher)
_repair = dspy.Predict(RepairCypher)

def post_process_cypher(query: str) -> str:
    """
    Applies rule-based fixes to the generated Cypher query.
    """
    # 1. Ensure proper property projection
    # Map variable -> Label
    var_to_label = {}
    # Regex for (var:Label)
    for match in re.finditer(r"\(\s*([a-zA-Z0-9_]+)\s*:\s*([a-zA-Z0-9_]+)", query):
        var, label = match.groups()
        var_to_label[var] = label
        
    if var_to_label:
        # Split by RETURN to find the last clause
        parts = re.split(r"(?i)(\bRETURN\b)", query)
        if len(parts) >= 3:
            last_body = parts[-1]
            
            # Separate body from suffix (ORDER BY, LIMIT, SKIP)
            suffix_pattern = re.compile(r"(\s+(?:ORDER\s+BY|SKIP|LIMIT).*)", re.DOTALL | re.IGNORECASE)
            suffix_match = suffix_pattern.search(last_body)
            
            if suffix_match:
                content = last_body[:suffix_match.start()]
                suffix = suffix_match.group(1)
            else:
                content = last_body
                suffix = ""
                
            def replace_var(m):
                v = m.group(1)
                if v in var_to_label:
                    prop = "knownName" if var_to_label[v] == "Scholar" else "name"
                    return f"{v}.{prop}"
                return v
            
            # Replace bare variables not followed by . or (
            new_content = re.sub(r"\b([a-zA-Z0-9_]+)\b(?!\s*[\.\(])", replace_var, content)
            
            parts[-1] = new_content + suffix
            query = "".join(parts)

    # 2. Enforce lowercase comparisons
    # Properties to wrap in toLower()
    target_props = [
        "name", "scholar_type", "fullName", "knownName", 
        "gender", "prize_id", "category", "motivation"
    ]
    
    # Regex components
    # Match properties: variable.property
    props_pattern = r"\b\w+\.(?:" + "|".join(target_props) + r")\b"
    # Match string literals: '...' or "..."
    str_pattern = r"'[^']*'|\"[^\"]*\""
    
    # Combined target: property OR string
    target_pattern = f"(?:{props_pattern}|{str_pattern})"
    
    # Regex to find targets, optionally preceded by toLower(
    # Group 'prefix': matches 'toLower(' (case-insensitive, with optional spaces)
    # Group 'target': matches the property or string
    pattern = re.compile(r"(?P<prefix>(?i:toLower\s*\(\s*))?(?P<target>" + target_pattern + r")")
    
    def replace(match):
        if match.group("prefix"):
            # Already wrapped, return as is
            return match.group(0)
        return f"toLower({match.group('target')})"
        
    query = pattern.sub(replace, query)

    # 3. Ensure LIMIT 100 if not present
    if not re.search(r"\bLIMIT\s+\d+", query, re.IGNORECASE):
        query = query.rstrip()
        if query.endswith(";"):
            query = query[:-1] + " LIMIT 100;"
        else:
            query += " LIMIT 100"

    return query

def generate_cypher(question: str, full_schema: dict[str, list[dict]]) -> tuple[str, dict]:
    """
    1) Extract full schema from Kuzu
    2) Prune schema w.r.t. the question
    3) Select few-shot exemplars based on similarity
    4) Generate Cypher from pruned schema + few-shot context
    """
    pruned = _prune(question=question, input_schema=full_schema).pruned_schema.model_dump()

    fewshot_block = get_fewshot_block(question, k=3)

    cy = _text2cypher(
        question=question,
        input_schema=pruned,
        fewshot_examples=fewshot_block,  
    ).query.query

    cy = post_process_cypher(cy)

    return cy, pruned

def repair_cypher(question: str, invalid_query: str, error_message: str, schema: dict, full_schema: dict) -> str:
    cy = _repair(
        question=question,
        input_schema=schema,
        full_schema=full_schema,
        invalid_query=invalid_query,
        error_message=error_message
    ).query.query
    
    return post_process_cypher(cy)

if __name__ == "__main__":
    db = kuzu.Database("nobel.kuzu", read_only=True)
    conn = kuzu.Connection(db)
    cypher, pruned = generate_cypher(
        "Which scholars won prizes in Physics and were affiliated with University of Cambridge?",
        conn,
    )
    print("PRUNED SCHEMA:", pruned)
    print("GENERATED CYPHER:", cypher)
