"""
Fetch HigherEdJobs job descriptions, extract a summary + education requirements,
and (optionally) enrich them with an LLM.

Why this exists
---------------
The old approach (see fetch_descriptions_archive.py) rendered every detail page in
Selenium with fixed 3-5s sleeps and a 10s wait-timeout per job, running serially -
roughly 6-12s per job, i.e. 30-60 min for a few hundred postings.

Two things were verified while building this module (Step 0 of the plan):
  1. HigherEdJobs detail pages sit behind an Imperva Incapsula JS challenge, so a
     plain `requests` GET returns a ~1KB stub, not content. Selenium (which already
     passes the challenge for the search pages) is required. There is therefore no
     "requests-first" fast path for detail pages on this site.
  2. The description lives in `<div id="jobDesc">` (plus a short `<div id="jobStatement">`);
     there is no JobPosting JSON-LD. Expired postings render a "no longer an active
     posting" stub.

So the speed-ups here are: reuse a single Selenium driver, wait on the real content
selector instead of a fixed sleep, drop the redundant sleeps, and - the biggest win -
cache every fetched description by `job_code` in a parquet file so re-runs only fetch
new postings. Optional AI enrichment runs as a single Anthropic batch (Haiku 4.5) and
is also cached, so it too is paid for only once per posting.
"""

import json
import os
import random
import re
import time
from datetime import date
from pathlib import Path
from typing import Dict, List, Optional

import polars as pl
from bs4 import BeautifulSoup

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

from scrappy_RA.utils import selenium_utils


# --- Configuration ---------------------------------------------------------

CACHE_PATH = Path("./scrappy_RA/data_saved_locally/higher_ed/descriptions_cache.parquet")

# The content selector we wait for and parse. #jobDesc is the full posting body;
# #jobStatement is a short institutional statement shown above it.
CONTENT_WAIT_SELECTOR = "#jobDesc, #mainContent, #job"

# Markers that a posting has been taken down (the CSV can be weeks/months stale).
DELETED_MARKERS = ("no longer an active posting", "position deleted")

# Education/qualification keywords for the free regex extractor. Word-boundary
# anchored so "education" doesn't match inside unrelated words.
EDU_RE = re.compile(
    r"\b(degree|bachelor'?s?|master'?s?|ph\.?\s?d\.?|doctora(?:te|l)|diploma|"
    r"education|qualif\w+|ged|baccalaureate|undergraduate|graduate)\b",
    re.IGNORECASE,
)

SUMMARY_CHARS = 500

# Cache schema. AI columns stay null until enrich_with_ai() fills them.
_CACHE_COLUMNS = [
    "job_code",
    "description",
    "summary",
    "education_requirements",
    "ai_summary",
    "ai_education_requirements",
    "ai_degree_level",
    "deleted",
    "fetched_date",
]


# --- Fetching --------------------------------------------------------------

def fetch_description_html(driver, url: str, wait_time: int = 20) -> str:
    """
    Navigate `driver` to `url`, wait for the real content to render, and return
    the page HTML. No fixed pre-sleep: we wait on the content selector instead
    and add only a short settle. Returns whatever HTML is present on timeout so
    the parser can still detect a deleted/blocked stub.
    """
    driver.get(url)
    try:
        WebDriverWait(driver, wait_time).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, CONTENT_WAIT_SELECTOR))
        )
    except TimeoutException:
        pass
    time.sleep(random.uniform(0.4, 0.9))  # brief settle for late-rendering JS
    return driver.page_source


def parse_description(html: str) -> Dict:
    """
    Pull the job description out of a rendered detail page.

    Order: JSON-LD JobPosting (rare on this site, but cheap to check) -> the
    #jobStatement + #jobDesc containers -> broader #job / #mainContent fallback.
    Flags deleted postings so callers can skip them.
    """
    soup = BeautifulSoup(html, "html.parser")
    body_text = soup.get_text(" ", strip=True)
    lowered = body_text.lower()

    if any(marker in lowered for marker in DELETED_MARKERS):
        return {"description": "", "education_requirements_jsonld": "", "deleted": True}

    edu_jsonld = ""
    jsonld = _find_jsonld_jobposting(soup)
    if jsonld:
        edu_jsonld = _stringify(jsonld.get("educationRequirements")) or _stringify(
            jsonld.get("qualifications")
        )

    # Primary: the real HigherEdJobs description containers. #jobDesc is the
    # substantive posting body and leads so the summary reflects the role;
    # #jobStatement is usually an institutional/EEO statement, appended after.
    parts: List[str] = []
    for div_id in ("jobDesc", "jobStatement"):
        el = soup.find("div", id=div_id)
        if el:
            text = el.get_text(separator=" ", strip=True)
            if text:
                parts.append(text)
    description = "\n\n".join(parts)

    # Fallback: broader wrappers (also covers minor template variations).
    if not description:
        for div_id in ("job", "mainContent"):
            el = soup.find("div", id=div_id)
            if el:
                description = el.get_text(separator=" ", strip=True)
                if description:
                    break

    # Last-ditch: JSON-LD description field.
    if not description and jsonld:
        description = _strip_html(jsonld.get("description", ""))

    return {
        "description": description,
        "education_requirements_jsonld": edu_jsonld,
        "deleted": False,
    }


def _find_jsonld_jobposting(soup: BeautifulSoup) -> Optional[dict]:
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = tag.string or tag.get_text() or ""
        if not raw.strip():
            continue
        try:
            data = json.loads(raw)
        except (ValueError, TypeError):
            continue
        for candidate in data if isinstance(data, list) else [data]:
            if isinstance(candidate, dict) and candidate.get("@type") == "JobPosting":
                return candidate
    return None


def _stringify(value) -> str:
    if not value:
        return ""
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, dict):
        # e.g. {"@type": "EducationalOccupationalCredential", "credentialCategory": "..."}
        return _stringify(value.get("credentialCategory") or value.get("name"))
    if isinstance(value, list):
        return ", ".join(s for s in (_stringify(v) for v in value) if s)
    return str(value).strip()


def _strip_html(text: str) -> str:
    if not text or "<" not in text:
        return (text or "").strip()
    return BeautifulSoup(text, "html.parser").get_text(" ", strip=True)


# --- Free (regex) extraction -----------------------------------------------

def extract_education_requirements(text: str, max_sentences: int = 8) -> str:
    """Return sentences from `text` that mention a degree/qualification keyword."""
    if not text:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", text)
    hits = [s.strip() for s in sentences if EDU_RE.search(s)]
    return " ".join(hits[:max_sentences])


def make_summary(text: str, max_chars: int = SUMMARY_CHARS) -> str:
    """First ~max_chars of the description, trimmed to a word boundary."""
    if not text:
        return ""
    text = " ".join(text.split())  # collapse whitespace
    if len(text) <= max_chars:
        return text
    cut = text[:max_chars]
    if " " in cut:
        cut = cut[: cut.rfind(" ")]
    return cut + "…"  # ellipsis


# --- Cache -----------------------------------------------------------------

def _empty_cache() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "job_code": pl.Utf8,
            "description": pl.Utf8,
            "summary": pl.Utf8,
            "education_requirements": pl.Utf8,
            "ai_summary": pl.Utf8,
            "ai_education_requirements": pl.Utf8,
            "ai_degree_level": pl.Utf8,
            "deleted": pl.Boolean,
            "fetched_date": pl.Utf8,
        }
    )


def _load_cache() -> pl.DataFrame:
    if CACHE_PATH.exists():
        try:
            cache = pl.read_parquet(CACHE_PATH)
            # Ensure all expected columns exist (forward-compat with older caches).
            for col in _CACHE_COLUMNS:
                if col not in cache.columns:
                    dtype = pl.Boolean if col == "deleted" else pl.Utf8
                    cache = cache.with_columns(pl.lit(None, dtype=dtype).alias(col))
            return cache.with_columns(pl.col("job_code").cast(pl.Utf8))
        except Exception as e:
            print(f"  Warning: could not read cache ({e}); starting fresh.")
    return _empty_cache()


def _write_cache(cache: pl.DataFrame) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    cache.write_parquet(CACHE_PATH)


# --- Orchestration ---------------------------------------------------------

def fetch_job_descriptions(
    df: pl.DataFrame,
    driver=None,
    ai_enrich: bool = False,
    ai_model: str = "claude-haiku-4-5",
    desc_limit: Optional[int] = None,
) -> pl.DataFrame:
    """
    Add `description`, `summary`, and `education_requirements` columns to `df`
    (keyed on `job_code`), fetching only postings not already cached. When
    `ai_enrich` is True, also add `ai_summary`, `ai_education_requirements`, and
    `ai_degree_level` via a single Anthropic batch over the newly-seen postings.

    `desc_limit`, if set, caps how many *new* (not-yet-cached) postings are
    fetched this run - handy for a quick test or to spread fetching across runs
    (each run picks up where the last left off, since cached rows are skipped).
    All rows still get the description columns via the join; postings past the
    cap simply come back null until a later run fetches them.

    `df` must have `job_code` and `url` columns. If `driver` is None, one is
    created via selenium_utils.setup_driver() and closed when done.
    """
    if df is None or df.is_empty():
        return df
    if "job_code" not in df.columns or "url" not in df.columns:
        print("  fetch_job_descriptions: df is missing 'job_code'/'url'; skipping.")
        return df

    df = df.with_columns(pl.col("job_code").cast(pl.Utf8))
    cache = _load_cache()
    cached_codes = set(cache["job_code"].to_list())

    # Postings we still need to fetch: have a job_code + url, not already cached.
    to_fetch = df.filter(
        (pl.col("job_code").is_not_null())
        & (pl.col("job_code") != "")
        & (pl.col("url").is_not_null())
        & (pl.col("url") != "")
        & (~pl.col("job_code").is_in(list(cached_codes)))
    ).unique(subset=["job_code"], keep="first", maintain_order=True)

    n_uncached = to_fetch.height
    if desc_limit is not None and to_fetch.height > desc_limit:
        to_fetch = to_fetch.head(desc_limit)

    capped = f", capped at {desc_limit}" if desc_limit is not None else ""
    print(f"\nFetching descriptions: {to_fetch.height} this run "
          f"({n_uncached} uncached{capped}) / "
          f"{df.height} total ({len(cached_codes)} already cached).")

    new_rows: List[Dict] = []
    if to_fetch.height:
        own_driver = driver is None
        if own_driver:
            driver = selenium_utils.setup_driver()
        try:
            today = date.today().isoformat()
            n_failed = 0
            for i, row in enumerate(to_fetch.iter_rows(named=True), 1):
                title = (row.get("title") or "")[:55]
                print(f"  [{i}/{to_fetch.height}] {title}")

                # Fetch, retrying once if the page comes back empty and is not a
                # confirmed deletion - an empty non-deleted page is usually a bot
                # challenge/incomplete render, which a reload after a pause clears.
                parsed = {"description": "", "education_requirements_jsonld": "",
                          "deleted": False}
                for attempt in range(2):
                    try:
                        html = fetch_description_html(driver, row["url"])
                        parsed = parse_description(html)
                    except Exception as e:
                        print(f"      fetch error: {e}")
                        parsed = {"description": "", "education_requirements_jsonld": "",
                                  "deleted": False}
                    if parsed["description"] or parsed.get("deleted"):
                        break
                    if attempt == 0:
                        time.sleep(random.uniform(2.5, 4.0))  # let a challenge clear

                # Only cache resolved results (real description, or confirmed
                # deletion). Transient failures are left uncached so the next run
                # retries them instead of persisting a blank forever.
                if not (parsed["description"] or parsed.get("deleted")):
                    n_failed += 1
                    print("      unresolved (challenge/empty) - will retry next run")
                    time.sleep(random.uniform(0.6, 1.2))
                    continue

                desc = parsed["description"]
                edu = extract_education_requirements(desc)
                if not edu and parsed.get("education_requirements_jsonld"):
                    edu = parsed["education_requirements_jsonld"]

                new_rows.append({
                    "job_code": row["job_code"],
                    "description": desc,
                    "summary": make_summary(desc),
                    "education_requirements": edu,
                    "ai_summary": None,
                    "ai_education_requirements": None,
                    "ai_degree_level": None,
                    "deleted": bool(parsed.get("deleted")),
                    "fetched_date": today,
                })
                time.sleep(random.uniform(0.6, 1.2))  # polite pacing
        finally:
            if own_driver:
                try:
                    driver.quit()
                    print("  Description driver closed.")
                except Exception:
                    pass

        if new_rows:
            cache = pl.concat([cache, pl.DataFrame(new_rows, schema=cache.schema)])
            cache = cache.unique(subset=["job_code"], keep="last")
            _write_cache(cache)
        n_desc = sum(1 for r in new_rows if r["description"])
        n_del = sum(1 for r in new_rows if r["deleted"])
        print(f"  Fetched {n_desc} descriptions ({n_del} deleted/expired, "
              f"{n_failed} unresolved) -> cache now {cache.height} rows.")

    # Optional AI enrichment of any cached rows still missing it.
    if ai_enrich:
        cache = enrich_with_ai(cache, model=ai_model)

    # Join the cache onto the caller's df.
    join_cols = ["job_code", "description", "summary", "education_requirements"]
    if ai_enrich:
        join_cols += ["ai_summary", "ai_education_requirements", "ai_degree_level"]
    return df.join(cache.select(join_cols), on="job_code", how="left")


# --- Optional AI enrichment (Anthropic Batches API) ------------------------

_AI_TOOL = {
    "name": "record_job_analysis",
    "description": "Record a structured analysis of a job posting.",
    "input_schema": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "string",
                "description": "2-3 sentence plain-language summary of the role.",
            },
            "education_requirements": {
                "type": "string",
                "description": "The education/degree requirements, noting required "
                               "vs. preferred. Empty string if none are stated.",
            },
            "degree_level": {
                "type": "string",
                "enum": ["None stated", "Associate", "Bachelor", "Master",
                         "Doctorate", "Professional"],
                "description": "Highest degree level required (not preferred).",
            },
        },
        "required": ["summary", "education_requirements", "degree_level"],
    },
}


def enrich_with_ai(
    cache: pl.DataFrame,
    model: str = "claude-haiku-4-5",
    max_chars: int = 4000,
    poll_seconds: int = 20,
    max_wait_seconds: int = 3600,
) -> pl.DataFrame:
    """
    Fill ai_summary / ai_education_requirements / ai_degree_level for cached rows
    that have a real description but no AI analysis yet, using one Anthropic batch
    (50% cheaper). Requires ANTHROPIC_API_KEY in the environment. On any problem
    (missing key/SDK, batch error) it warns and returns `cache` unchanged.
    """
    try:
        import anthropic
        from anthropic.types.message_create_params import MessageCreateParamsNonStreaming
        from anthropic.types.messages.batch_create_params import Request
    except ImportError:
        print("  AI enrichment: `anthropic` not installed; skipping.")
        return cache

    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("  AI enrichment: ANTHROPIC_API_KEY not set; skipping.")
        return cache

    todo = cache.filter(
        (pl.col("description").is_not_null())
        & (pl.col("description").str.len_chars() > 100)
        & (pl.col("ai_summary").is_null())
    )
    if todo.is_empty():
        print("  AI enrichment: nothing new to analyze.")
        return cache

    print(f"  AI enrichment: submitting {todo.height} postings as a batch ({model})...")
    client = anthropic.Anthropic()

    requests = []
    for row in todo.iter_rows(named=True):
        prompt = (
            "Analyze this higher-education job posting. Use the "
            "record_job_analysis tool.\n\n" + row["description"][:max_chars]
        )
        requests.append(
            Request(
                custom_id=str(row["job_code"]),
                params=MessageCreateParamsNonStreaming(
                    model=model,
                    max_tokens=1024,
                    tools=[_AI_TOOL],
                    tool_choice={"type": "tool", "name": "record_job_analysis"},
                    messages=[{"role": "user", "content": prompt}],
                ),
            )
        )

    try:
        batch = client.messages.batches.create(requests=requests)
    except Exception as e:
        print(f"  AI enrichment: batch submission failed ({e}); skipping.")
        return cache

    print(f"    batch {batch.id} submitted; polling...")
    waited = 0
    while waited < max_wait_seconds:
        batch = client.messages.batches.retrieve(batch.id)
        if batch.processing_status == "ended":
            break
        time.sleep(poll_seconds)
        waited += poll_seconds
    if batch.processing_status != "ended":
        print(f"  AI enrichment: batch not finished after {max_wait_seconds}s; "
              f"results will be picked up on a later run. (batch {batch.id})")
        return cache

    results: Dict[str, Dict[str, str]] = {}
    for result in client.messages.batches.results(batch.id):
        if result.result.type != "succeeded":
            continue
        for block in result.result.message.content:
            if getattr(block, "type", None) == "tool_use":
                data = block.input or {}
                results[result.custom_id] = {
                    "ai_summary": data.get("summary", ""),
                    "ai_education_requirements": data.get("education_requirements", ""),
                    "ai_degree_level": data.get("degree_level", ""),
                }
                break

    print(f"    got {len(results)} analyses back; merging into cache.")
    if not results:
        return cache

    # Overwrite the three AI columns for the rows we just analyzed.
    def _pick(job_code, field, current):
        r = results.get(str(job_code))
        return r[field] if r else current

    cache = cache.with_columns([
        pl.struct(["job_code", "ai_summary"]).map_elements(
            lambda s: _pick(s["job_code"], "ai_summary", s["ai_summary"]),
            return_dtype=pl.Utf8,
        ).alias("ai_summary"),
        pl.struct(["job_code", "ai_education_requirements"]).map_elements(
            lambda s: _pick(s["job_code"], "ai_education_requirements",
                            s["ai_education_requirements"]),
            return_dtype=pl.Utf8,
        ).alias("ai_education_requirements"),
        pl.struct(["job_code", "ai_degree_level"]).map_elements(
            lambda s: _pick(s["job_code"], "ai_degree_level", s["ai_degree_level"]),
            return_dtype=pl.Utf8,
        ).alias("ai_degree_level"),
    ])
    _write_cache(cache)
    return cache
