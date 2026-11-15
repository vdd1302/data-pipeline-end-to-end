"""M4.3.5 - TopCV skill normalization V1 (offline, deterministic).

Inputs: raw_payload['skill_tags_raw'], raw_payload['job_requirements_raw'].
Staging:
  skills_clean: TEXT[] or SQL NULL (Python None)
  skill_evidence_clean: JSONB array or SQL NULL
  skill_normalization_version: TEXT
  quality_flags: caller merges with other cleaners' flags
DW:
  dim_skill.skill_name / normalization_version
  bridge_snapshot_skill.extraction_method (one row per skill per snapshot)

Policy:
* Tag list is highest-confidence. Preserve a tag even if taxonomy does not
  recognize it; flag it for later vocabulary review.
* Requirements scan is a conservative whitelist for EXPLICIT mentions only,
  not inference that the employer necessarily requires each mentioned skill.
* Overlapping tag & requirements mentions create one skill; prefer 'tag' as
  extraction_method, keep both evidence sources.
* Only parse requirements, NEVER job description, benefits or SEO metadata.
* No inferred tags when no explicit evidence exists: return None.
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any

SKILL_NORMALIZATION_VERSION = "topcv-skill-rules-v2"


def _tidy(text: str) -> str:
    text = unicodedata.normalize("NFKC", text)
    return re.sub(r"\s+", " ", text).strip()


# Exact aliases only: never substring-match a tag's label.
_CANONICAL_ALIASES: dict[str, tuple[str, ...]] = {
    "Python": ("python", "python development", "python programming"),
    "SQL": ("sql", "structured query language"),
    "MySQL": ("mysql",),
    "PostgreSQL": ("postgres", "postgresql"),
    "Microsoft SQL Server": ("sql server", "microsoft sql server", "mssql"),
    "Apache Spark": ("spark", "apache spark"),
    "PySpark": ("pyspark", "py spark"),
    "AWS": ("aws", "amazon web services"),
    "Google Cloud Platform": ("gcp", "google cloud", "google cloud platform"),
    "Microsoft Azure": ("azure", "microsoft azure"),
    "Git": ("git",),
    "Jira": ("jira",),
    "Confluence": ("confluence",),
    "CI/CD": ("ci/cd", "ci - cd", "continuous integration/continuous delivery"),
    "Apache Airflow": ("airflow", "apache airflow"),
    "Apache Kafka": ("kafka", "apache kafka"),
    "dbt": ("dbt", "data build tool"),
    "Docker": ("docker",),
    "Kubernetes": ("kubernetes", "k8s"),
    "Databricks": ("databricks",),
    "Power BI": ("power bi", "powerbi"),
    "Tableau": ("tableau",),
    "Scala": ("scala",),
    "Java": ("java",),
    "Linux": ("linux",),
    "Snowflake": ("snowflake",),
    "BigQuery": ("bigquery", "google bigquery"),
    "Amazon Redshift": ("redshift", "amazon redshift"),
    "ETL": ("etl",),
    "ELT": ("elt",),
}

_EXTRA_ALIASES = {
    'JavaScript': ('javascript','java script','js'),
    'TypeScript': ('typescript','type script','ts'),
    'Node.js': ('node.js','nodejs','node js'),
    'React': ('react','reactjs','react.js','react js'),
    'React Native': ('react native',),
    'Vue.js': ('vue.js','vuejs','vue js'),
    'Angular': ('angular','angularjs'),
    'HTML5': ('html','html5'), 'CSS3': ('css','css3'),
    'C#': ('c#','c sharp'), 'C++': ('c++','cpp'),
    '.NET': ('.net','dotnet'), 'ASP.NET': ('asp.net','asp net'),
    'Go': ('go','golang','go programming'), 'R': ('r','r programming','r language'),
    'Rust': ('rust',), 'PHP': ('php',), 'Laravel': ('laravel',),
    'Spring Boot': ('spring boot','springboot'),
    'Redis': ('redis',), 'MongoDB': ('mongodb','mongo db'),
    'Elasticsearch': ('elasticsearch','elastic search'),
    'Django': ('django',), 'Flask': ('flask',), 'FastAPI': ('fastapi','fast api'),
    'REST API': ('rest api','restful api','restful'), 'GraphQL': ('graphql',),
    'Microservices': ('microservices','microservice'),
    'Jenkins': ('jenkins',), 'GitHub Actions': ('github actions',),
    'Terraform': ('terraform',), 'Ansible': ('ansible',),
    'Prometheus': ('prometheus',), 'Grafana': ('grafana',), 'Nginx': ('nginx',),
    'TensorFlow': ('tensorflow','tensor flow'), 'PyTorch': ('pytorch','py torch'),
    'scikit-learn': ('scikit-learn','sklearn','scikit learn'),
    'LLM': ('llm','large language model','large language models'),
    'NLP': ('nlp','natural language processing'), 'Computer Vision': ('computer vision',),
    'Machine Learning': ('machine learning',), 'Deep Learning': ('deep learning',),
    'Statistics': ('statistics','thống kê'), 'Excel': ('excel','microsoft excel'),
    'BPMN': ('bpmn',), 'UML': ('uml',), 'Figma': ('figma',),
    'Agile': ('agile',), 'Scrum': ('scrum',),
    'Penetration Testing': ('penetration testing','pentest','pentesting'),
    'SIEM': ('siem',), 'SOC': ('soc',), 'OWASP': ('owasp',),
    'Apache Flink': ('flink','apache flink'), 'Apache Hadoop': ('hadoop','apache hadoop'),
}
_CANONICAL_ALIASES.update(_EXTRA_ALIASES)

_ALIAS_LOOKUP = {
    alias.casefold(): name
    for name, aliases in _CANONICAL_ALIASES.items()
    for alias in aliases
}

# Targeted literal mentions. Terms like AI, Go, R, C are intentionally absent
# because they cause a large number of false positives in ordinary sentences.
_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("Microsoft SQL Server", re.compile(r"\b(?:microsoft\s+)?sql\s+server\b|\bmssql\b", re.I)),
    ("Google Cloud Platform", re.compile(r"\bgoogle\s+cloud(?:\s+platform)?\b|\bgcp\b", re.I)),
    ("Amazon Redshift", re.compile(r"\b(?:amazon\s+)?redshift\b", re.I)),
    ("Apache Airflow", re.compile(r"\b(?:apache\s+)?airflow\b", re.I)),
    ("Apache Kafka", re.compile(r"\b(?:apache\s+)?kafka\b", re.I)),
    ("Apache Spark", re.compile(r"\b(?:apache\s+)?spark\b", re.I)),
    ("PySpark", re.compile(r"\bpyspark\b|\bpy\s+spark\b", re.I)),
    ("Microsoft Azure", re.compile(r"\b(?:microsoft\s+)?azure\b", re.I)),
    ("AWS", re.compile(r"\baws\b|\bamazon\s+web\s+services\b", re.I)),
    ("Python", re.compile(r"\bpython\b", re.I)),
    ("PostgreSQL", re.compile(r"\bpostgres(?:ql)?\b", re.I)),
    ("MySQL", re.compile(r"\bmysql\b", re.I)),
    ("SQL", re.compile(r"\bsql\b", re.I)),
    ("Git", re.compile(r"\bgit\b", re.I)),
    ("Jira", re.compile(r"\bjira\b", re.I)),
    ("Confluence", re.compile(r"\bconfluence\b", re.I)),
    ("CI/CD", re.compile(r"\bci\s*/\s*cd\b", re.I)),
    ("dbt", re.compile(r"\bdbt\b|\bdata\s+build\s+tool\b", re.I)),
    ("Docker", re.compile(r"\bdocker\b", re.I)),
    ("Kubernetes", re.compile(r"\bkubernetes\b|\bk8s\b", re.I)),
    ("Databricks", re.compile(r"\bdatabricks\b", re.I)),
    ("Power BI", re.compile(r"\bpower\s*bi\b", re.I)),
    ("Tableau", re.compile(r"\btableau\b", re.I)),
    ("Scala", re.compile(r"\bscala\b", re.I)),
    ("Java", re.compile(r"\bjava\b", re.I)),
    ("Linux", re.compile(r"\blinux\b", re.I)),
    ("Snowflake", re.compile(r"\bsnowflake\b", re.I)),
    ("BigQuery", re.compile(r"\b(?:google\s+)?bigquery\b", re.I)),
    ("ETL", re.compile(r"\betl\b", re.I)),
    ("ELT", re.compile(r"\belt\b", re.I)),
)

# Short acronyms are trusted as exact tags, but not inferred from ordinary prose.
_PROSE_AMBIGUOUS = {'go','r','js','ts','soc'}
_PATTERNS += tuple(
    (name, re.compile(r'(?<!\w)(?:' + '|'.join(re.escape(alias) for alias in aliases
        if alias not in _PROSE_AMBIGUOUS) + r')(?!\w)' +
        (r'(?!\s+(?:to|quickly|promptly)\b)' if name == 'React' else ''), re.I))
    for name, aliases in _EXTRA_ALIASES.items()
    if any(alias not in _PROSE_AMBIGUOUS for alias in aliases)
)


def canonical_skill_names() -> frozenset[str]:
    return frozenset(_CANONICAL_ALIASES)

# Avoid treating statements of NON-requirement as required-skill evidence.
# This is deliberately narrow and conservative, and future policy may refine it.
_NEGATION = re.compile(
    r"\b(?:not\s+(?:required|necessary|mandatory)|no\s+(?:knowledge|experience)\s+(?:of|in|with)|"
    r"không\s+(?:yêu\s+cầu|bắt\s+buộc|cần))\b", re.I
)


@dataclass(frozen=True)
class SkillResult:
    skills_clean: list[str] | None = None
    skill_evidence_clean: list[dict[str, Any]] | None = None
    skill_normalization_version: str = SKILL_NORMALIZATION_VERSION
    quality_flags: list[str] = field(default_factory=list)


def _requirement_mentions(text: str) -> list[tuple[str, str]]:
    """Return (canonical skill, matched substring), in document order.

    Longest matching phrases win when tokens overlap (SQL Server vs SQL).
    Each sentence/line is checked for a simple explicit non-requirement cue.
    """
    found: list[tuple[int, int, str, str]] = []
    for name, pattern in _PATTERNS:
        for match in pattern.finditer(text):
            # Restrict negation check to the immediate clause around the term;
            # treat explicit 'not required' nearby as exclusion.
            left = text[max(0, match.start() - 35):match.start()]
            right = text[match.end():min(len(text), match.end() + 35)]
            left = re.split(r"[\n.;!?]", left)[-1]
            right = re.split(r"[\n.;!?]", right)[0]
            if _NEGATION.search(left) or _NEGATION.search(right):
                continue
            found.append((match.start(), match.end(), name, match.group(0)))

    # Same start: longer mention first; overlapping mention: keep the longest.
    found.sort(key=lambda item: (item[0], -(item[1] - item[0]), item[2]))
    accepted: list[tuple[int, int, str, str]] = []
    for start, end, name, matched in found:
        if any(start < previous_end and end > previous_start
               for previous_start, previous_end, _, _ in accepted):
            continue
        accepted.append((start, end, name, matched))

    return [(name, matched) for _, _, name, matched in accepted]


def normalize_skills(
    skill_tags_raw: list[str] | None,
    job_requirements_raw: str | None = None,
) -> SkillResult:
    """Normalize explicit TopCV tags and a conservative keyword scan.

    skill_evidence_clean format (JSONB-ready):
      [{"skill_name": "Python", "extraction_method": "tag",
        "evidence_sources": ["tag", "requirements_keyword"],
        "raw_evidence": ["python", "Python"],
        "normalization_version": "topcv-skill-rules-v1"}]

    For the DW bridge, use the single extraction_method field per skill;
    evidence_sources/raw_evidence preserve what produced it.
    """
    flags: list[str] = []
    found: dict[str, dict[str, Any]] = {}

    if skill_tags_raw is None:
        flags.append("skill_tags_missing")
        tags: list[str] = []
    elif not isinstance(skill_tags_raw, list):
        flags.append("skill_tags_invalid_type")
        tags = []
    else:
        tags = skill_tags_raw
        if len(tags) == 0:
            flags.append("skill_tags_empty")

    def add(name: str, method: str, evidence: str) -> None:
        key = name.casefold()
        if key not in found:
            found[key] = {
                "skill_name": name,
                "extraction_method": method,
                "evidence_sources": [method],
                "raw_evidence": [evidence],
                "normalization_version": SKILL_NORMALIZATION_VERSION,
            }
        else:
            record = found[key]
            if method not in record["evidence_sources"]:
                record["evidence_sources"].append(method)
            if evidence not in record["raw_evidence"]:
                record["raw_evidence"].append(evidence)
            # Tags take priority for the single bridge extraction_method.
            if method == "tag":
                record["extraction_method"] = "tag"

    for item in tags:
        if not isinstance(item, str):
            flags.append("skill_tag_invalid_item")
            continue
        label = _tidy(item)
        if not label:
            flags.append("skill_tag_empty_item")
            continue
        canonical = _ALIAS_LOOKUP.get(label.casefold())
        if canonical is None:
            # Unknown tags are still explicit source data, so preserve them.
            canonical = label
            flags.append("skill_unknown_tag_review")
        add(canonical, "tag", label)

    if job_requirements_raw is not None and not isinstance(job_requirements_raw, str):
        flags.append("skill_requirements_invalid_type")
    elif job_requirements_raw:
        requirements = unicodedata.normalize("NFKC", job_requirements_raw)
        for name, evidence in _requirement_mentions(requirements):
            add(name, "requirements_keyword", evidence)

    if not found:
        flags.append("skills_not_identified")
        return SkillResult(quality_flags=list(dict.fromkeys(flags)))

    if any(
        "requirements_keyword" in record["evidence_sources"]
        for record in found.values()
    ):
        flags.append("skills_requirements_keyword_present")

    rows = list(found.values())
    return SkillResult(
        skills_clean=[row["skill_name"] for row in rows],
        skill_evidence_clean=rows,
        quality_flags=list(dict.fromkeys(flags)),
    )
