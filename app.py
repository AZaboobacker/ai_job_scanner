import os
import re
import io
import json
import time
import requests
import pandas as pd
from datetime import datetime, timedelta
import streamlit as st
import math

# ---------------- Basic Config ----------------
ADZUNA_COUNTRY = "us"
ADZUNA_BASE = f"https://api.adzuna.com/v1/api/jobs/{ADZUNA_COUNTRY}/search/{{page}}"
ARBEITNOW_URL = "https://arbeitnow.com/api/job-board-api"
USAJOBS_BASE = "https://data.usajobs.gov/api/Search"
REMOTIVE_URL = "https://remotive.com/api/remote-jobs"
JSEARCH_URL = "https://jsearch.p.rapidapi.com/search"
JOOBLE_URL_FMT = "https://jooble.org/api/{key}"

st.set_page_config(page_title="Job Search CRM", page_icon="🧭", layout="wide")
st.title("🧭 Job Search Automation & CRM")
st.caption("Fetch jobs from multiple APIs, score against your resume (deterministic or OpenAI), enrich with company info, and track applications.")

# ---------------- Sidebar ----------------
st.sidebar.title("🔧 Settings")
role_query = st.sidebar.text_input("Role / Keywords", value="Financial Analyst")
location_query = st.sidebar.text_input("Locations (comma-separated)", value="", help="Example: New Jersey, New York, Remote")
days_back = st.sidebar.number_input("Posted within last N days", min_value=1, max_value=60, value=30, step=1)
max_jobs = st.sidebar.number_input("Max jobs per source", min_value=50, max_value=2000, value=500, step=50)

st.sidebar.markdown("### Sources")
use_adzuna   = st.sidebar.checkbox("Adzuna", value=True)
use_usajobs  = st.sidebar.checkbox("USAJOBS (Federal)", value=True)
use_arbeit   = st.sidebar.checkbox("Arbeitnow (public)", value=True)
use_remotive = st.sidebar.checkbox("Remotive (remote)", value=True)
use_jsearch  = st.sidebar.checkbox("JSearch (RapidAPI)", value=True)
use_jooble   = st.sidebar.checkbox("Jooble", value=True)

st.sidebar.markdown("---")
st.sidebar.markdown("### API Keys")
adzuna_app_id  = st.sidebar.text_input("ADZUNA_APP_ID", os.getenv("ADZUNA_APP_ID",""))
adzuna_app_key = st.sidebar.text_input("ADZUNA_APP_KEY", os.getenv("ADZUNA_APP_KEY",""), type="password")
usajobs_key    = st.sidebar.text_input("USAJOBS_KEY", os.getenv("USAJOBS_KEY",""), type="password")
usajobs_user_agent = st.sidebar.text_input("USAJOBS_USER_AGENT (email)", os.getenv("USAJOBS_USER_AGENT",""))
rapidapi_key   = st.sidebar.text_input("RAPIDAPI_KEY (for JSearch)", os.getenv("RAPIDAPI_KEY",""), type="password")
jooble_key     = st.sidebar.text_input("JOOBLE_KEY", os.getenv("JOOBLE_KEY",""), type="password")
clearbit_key   = st.sidebar.text_input("CLEARBIT_KEY (optional)", os.getenv("CLEARBIT_KEY",""), type="password")

st.sidebar.markdown("---")
st.sidebar.markdown("### OpenAI Matching (optional)")
use_openai = st.sidebar.checkbox("Use OpenAI for resume↔JD matching", value=True)
openai_api_key = st.sidebar.text_input("OPENAI_API_KEY", os.getenv("OPENAI_API_KEY",""), type="password")

st.sidebar.markdown("---")
debug_mode = st.sidebar.checkbox("Show debug info", value=False)

uploaded_resume = st.file_uploader("Upload your resume (PDF, DOCX, or TXT)", type=["pdf","docx","txt"])
run_btn = st.sidebar.button("🔍 Run Search")

# ---------------- Columns spec ----------------
COLS = ["source","company","title","location","employment_type","remote_hybrid_onsite",
        "posted_date","salary_currency","salary_min","salary_max","salary_text",
        "visa_sponsorship","experience_level","apply_url","job_url",
        "skills_must","skills_nice","keywords","jd_text",
        "match_score","skill_match_count","missing_skills","notes"]

TRACKER_EXTRA = ["matched_skills","status",
                 "company_domain","company_logo","company_size","company_industry","company_country","company_hq",
                 "suggested recoomendations"]
TRACKER_COLS = COLS + TRACKER_EXTRA

# ---------------- Normalization & Helpers ----------------
def normalize_token(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9+]+", " ", s).strip()
    return s

SYNONYM_MAP = {
    "aws sagemaker": ["sagemaker", "amazon sagemaker"],
    "postgresql": ["postgres", "postgre sql"],
    "microsoft excel": ["excel", "advanced excel"],
    "power bi": ["powerbi", "ms power bi"],
    "sql": ["t sql", "mysql", "postgresql"],
    "tableau": ["tableau desktop", "tableau server"],
    "kpi": ["kpis", "key performance indicators"],
    "salesforce": ["sfdc"],
    "oracle": ["oracle erp", "oracle ebs"],
}

KEYWORD_HINTS = [
    "mlops", "llm", "genai", "fp&a", "fpna", "budgeting", "forecasting", "variance analysis",
    "financial modeling", "dashboards", "tableau", "power bi", "excel", "sql", "snowflake",
    "python", "pricing models", "kpi", "kpis", "gaap"
]

MUST_PATTERNS = [r"must\s+have[:\- ]", r"required[:\- ]", r"requirements?[:\- ]"]
NICE_PATTERNS = [r"nice\s+to\s+have[:\- ]", r"preferred[:\- ]", r"bonus[:\- ]"]

def expand_synonyms(skills_set):
    expanded = set(skills_set)
    for canonical, alts in SYNONYM_MAP.items():
        if canonical in skills_set or any(a in skills_set for a in alts):
            expanded.add(canonical)
            expanded.update(alts)
    return expanded

def read_resume_text(uploaded_file) -> str:
    if not uploaded_file:
        return ""
    name = uploaded_file.name.lower()
    uploaded_file.seek(0)
    try:
        if name.endswith(".txt"):
            return uploaded_file.read().decode("utf-8", errors="ignore")
        elif name.endswith(".pdf"):
            import pdfminer.high_level
            uploaded_file.seek(0)
            return pdfminer_high_level_extract(uploaded_file)
        elif name.endswith(".docx"):
            import docx2txt
            uploaded_file.seek(0)
            return docx2txt.process(uploaded_file)
    except Exception:
        return ""
    return ""

def pdfminer_high_level_extract(file_obj):
    import pdfminer.high_level
    try:
        return pdfminer.high_level.extract_text(file_obj)
    except Exception:
        return ""

def parse_resume(uploaded_file):
    text = read_resume_text(uploaded_file)
    tokens = set()
    for line in (text or "").splitlines():
        line = normalize_token(line)
        for tok in line.split():
            if len(tok) > 1:
                tokens.add(tok)

    seed_skills = [
        "financial modeling","financial reporting","project management","data analysis",
        "tableau","oracle","salesforce","microsoft excel","google suite","forecasting",
        "budgeting","kpi","kpis","pricing models","power bi","sql","variance analysis","python"
    ]
    skills = set()
    normalized_blob = " ".join(sorted(tokens))
    for s in seed_skills:
        if normalize_token(s) in normalized_blob:
            skills.add(normalize_token(s))
    for kw in ["excel","sql","tableau","oracle","salesforce","power bi","python"]:
        if normalize_token(kw) in tokens:
            skills.add(normalize_token(kw))
    return expand_synonyms(skills), text

def extract_skills_from_jd(jd_text: str):
    jd = jd_text or ""
    lines = [l.strip() for l in re.split(r"[\\n•\\-•]+", jd)]
    must, nice, keywords = set(), set(), set()
    block = "other"
    for l in lines:
        l_low = l.lower()
        if any(re.search(p, l_low) for p in MUST_PATTERNS):
            block = "must"; continue
        if any(re.search(p, l_low) for p in NICE_PATTERNS):
            block = "nice"; continue
        toks = re.findall(r"[A-Za-z0-9\\+\\#\\./ ]{2,}", l)
        skillish = [normalize_token(t) for t in toks if len(t.strip())>1]
        candidates = []
        for s in skillish:
            if any(k in s for k in ["excel","sql","tableau","power bi","oracle","salesforce","python","forecast","budget","model","gaap"]):
                candidates.append(s.strip())
        if block == "must":
            must.update(candidates)
        elif block == "nice":
            nice.update(candidates)
        else:
            for k in KEYWORD_HINTS:
                if k in l_low:
                    keywords.add(k)
    return list(must), list(nice), list(keywords)

def compute_match_breakdown(row, resume_skills):
    W_MUST, W_NICE, W_KEY = 2.0, 1.0, 0.5
    must = set([normalize_token(x) for x in (row.get("skills_must") or "").split(",") if x.strip()])
    nice = set([normalize_token(x) for x in (row.get("skills_nice") or "").split(",") if x.strip()])
    keys = set([normalize_token(x) for x in (row.get("keywords") or "").split(",") if x.strip()])
    resume = expand_synonyms(set([normalize_token(x) for x in resume_skills]))

    matched_must = sorted(must & resume)
    matched_nice = sorted(nice & resume)
    matched_keys = sorted(keys & resume)
    must_matches = len(matched_must)
    nice_matches = len(matched_nice)
    key_matches  = len(matched_keys)

    denom = (len(must)*W_MUST) + (len(nice)*W_NICE) + (len(keys)*W_KEY)
    denom = denom if denom > 0 else 1.0
    score = (must_matches*W_MUST + nice_matches*W_NICE + key_matches*W_KEY) / denom if denom > 0 else 0.0
    score_pct = round(score * 100, 1)

    missing = list((must | nice) - resume)
    matched_all = sorted(set(matched_must + matched_nice + matched_keys))
    return score_pct, (must_matches + nice_matches + key_matches), ", ".join(sorted(missing)), ", ".join(matched_all)

# ---------- Recommendation generator ----------
RECO_TEMPLATES = [
    ("excel", "Add a bullet proving advanced Excel (e.g., pivot tables, INDEX/MATCH, PowerQuery, XLOOKUP)."),
    ("power bi", "Show BI dashboards: include 1–2 KPIs and a link or screenshot reference."),
    ("tableau", "Include a Tableau dashboard project with metrics/KPIs; mention data size and outcome."),
    ("sql", "Quantify SQL work (joins/CTEs/window functions) with row counts and performance gains."),
    ("python", "Mention Python analysis/automation (pandas/numpy) with a measurable result."),
    ("oracle", "Reference experience with Oracle ERP/GL reports or reconciliations."),
    ("salesforce", "Call out Salesforce reporting or data hygiene/automation you led."),
    ("forecast", "Add FP&A forecasting (monthly/quarterly) with accuracy % or variance reduction."),
    ("budget", "Show budgeting ownership: budget size and variance analysis."),
    ("gaap", "Mention GAAP exposure: reconciliations, revenue recognition or close activities."),
    ("kpi", "List relevant KPIs you tracked (e.g., CAC, LTV, GM%, OpEx, Working Capital)."),
    ("pricing", "Add pricing/margin analysis with modeled scenarios and impact."),
    ("snowflake", "If relevant, note SQL/Snowflake with dbt/ELT exposure."),
]

def build_recommendations(missing_skills_str: str, topn: int = 4) -> str:
    missing = [x.strip() for x in (missing_skills_str or "").split(",") if x.strip()]
    if not missing:
        return "Looks good—no obvious gaps versus JD keywords."
    recos = []
    for key, tip in RECO_TEMPLATES:
        if any(key in m for m in missing):
            recos.append(f"{key}: {tip}")
        if len(recos) >= topn:
            break
    if len(recos) < topn:
        for m in missing:
            if all(not m.startswith(r.split(':')[0]) for r in recos):
                recos.append(f"{m}: consider adding a concrete example/result that proves this skill.")
            if len(recos) >= topn:
                break
    return " | ".join(recos[:topn])

# ---------- OpenAI LLM scorer ----------
def llm_normalize_list(items):
    norm = []
    seen = set()
    for it in items or []:
        s = re.sub(r"[^a-z0-9+.#/ ]+", " ", str(it).lower()).strip()
        s = re.sub(r"\s+", " ", s)
        if len(s) >= 2 and s not in seen:
            seen.add(s); norm.append(s)
    return norm

def llm_score_resume_job(openai_api_key, resume_text, jd_text, debug=False):
    if not openai_api_key or not jd_text:
        return None, None
    system = (
        "You extract skills from job descriptions and resumes, then compute a match. "
        "Return STRICT JSON ONLY with keys: skills_must, skills_nice, keywords, "
        "matched_skills, missing_skills, match_score. "
        "Normalize synonyms (e.g., 'Postgres'->'postgresql', 'AWS SageMaker'->'sagemaker'). "
        "Weighting: must=2.0, nice=1.0, keywords=0.5. Score 0-100."
    )
    user = {"job_description": jd_text[:18000], "resume": (resume_text or "")[:18000]}
    try:
        payload = {
            "model": "gpt-4o-mini",
            "input": [
                {"role": "system", "content": system},
                {"role": "user", "content": json.dumps(user)}
            ],
            "response_format": { "type": "json_object" },
        }
        headers = {"Authorization": f"Bearer {openai_api_key}"}
        r = requests.post("https://api.openai.com/v1/responses", headers=headers, json=payload, timeout=45)
        raw = None
        if r.status_code == 200:
            data = r.json()
            text = ""
            if "output_text" in data:
                text = data["output_text"]
            elif "choices" in data and data["choices"]:
                text = data["choices"][0].get("message", {}).get("content", "")
            elif "output" in data and isinstance(data["output"], dict) and "text" in data["output"]:
                text = data["output"]["text"]
            elif "content" in data and isinstance(data["content"], str):
                text = data["content"]
            raw = text
            if not text:
                return None, {"error":"empty-openai-content","status":r.status_code}
            parsed = json.loads(text)
            for k in ["skills_must","skills_nice","keywords","matched_skills","missing_skills"]:
                parsed[k] = llm_normalize_list(parsed.get(k, []))
            try:
                parsed["match_score"] = round(float(parsed.get("match_score", 0)), 1)
            except:
                parsed["match_score"] = 0.0
            return parsed, {"status":r.status_code,"raw":raw[:500] if raw else None}
        else:
            return None, {"error":"openai-non-200","status":r.status_code,"body":r.text[:400]}
    except Exception as e:
        return None, {"error":"openai-exception","detail":str(e)}

# ---------------- Fetchers (same as previous build) ----------------
def fetch_adzuna(query, location, days_back, limit, app_id, app_key):
    out = []
    if not app_id or not app_key:
        return out
    per_page = 50
    max_pages = max(1, min(40, (limit // per_page) + 3))
    for page in range(1, max_pages+1):
        params = {
            "app_id": app_id,
            "app_key": app_key,
            "results_per_page": per_page,
            "what": query,
            "where": location or "",
            "content-type": "application/json",
            "max_days_old": days_back,
            "sort_by": "date"
        }
        url = ADZUNA_BASE.format(page=page)
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code != 200:
                break
            data = r.json()
            for item in data.get("results", []):
                created = item.get("created")
                posted_date = ""
                if created:
                    try:
                        posted_date = datetime.fromisoformat(created.replace("Z","+00:00")).date().isoformat()
                    except Exception:
                        posted_date = ""
                jd_text = item.get("description") or ""
                contract_time = item.get("contract_time")
                contract_type = item.get("contract_type")
                employment_type = ("Full-time" if (contract_time == "full_time" or contract_type == "permanent") else
                                   "Contract" if (contract_type == "contract") else "")
                location_area = item.get("location", {}).get("display_name", "")
                remote_hybrid_onsite = ""
                low = jd_text.lower()
                if "remote" in low: remote_hybrid_onsite = "Remote"
                elif "hybrid" in low: remote_hybrid_onsite = "Hybrid"
                elif location_area: remote_hybrid_onsite = "Onsite"
                out.append({
                    "source": "Adzuna",
                    "company": (item.get("company") or {}).get("display_name",""),
                    "title": item.get("title",""),
                    "location": location_area or "",
                    "employment_type": employment_type,
                    "remote_hybrid_onsite": remote_hybrid_onsite,
                    "posted_date": posted_date,
                    "salary_currency": item.get("salary_currency","") or "",
                    "salary_min": item.get("salary_min","") or "",
                    "salary_max": item.get("salary_max","") or "",
                    "salary_text": "",
                    "visa_sponsorship": "Unknown",
                    "experience_level": "",
                    "apply_url": item.get("redirect_url","") or "",
                    "job_url": item.get("redirect_url","") or "",
                    "skills_must": "",
                    "skills_nice": "",
                    "keywords": "",
                    "jd_text": jd_text,
                })
            if len(out) >= limit:
                break
        except Exception:
            break
        time.sleep(0.1)
    return out[:limit]

def fetch_arbeitnow(days_back, limit, query):
    out = []
    try:
        r = requests.get(ARBEITNOW_URL, timeout=20)
        if r.status_code != 200:
            return out
        data = r.json()
        jobs = data.get("data", [])
        cutoff = datetime.utcnow().date() - timedelta(days=days_back)
        for j in jobs:
            title = j.get("title") or ""
            if query and query.lower() not in title.lower():
                continue
            date_str = j.get("created_at")
            posted_date = ""
            try:
                posted_date = datetime.fromisoformat(date_str.replace("Z","+00:00")).date().isoformat()
            except Exception:
                posted_date = ""
            if posted_date and posted_date < cutoff.isoformat():
                continue
            company = j.get("company_name") or ""
            locs = ", ".join(j.get("locations", []) or [])
            tags = ", ".join(j.get("tags", []) or [])
            apply_url = j.get("url") or ""
            description = j.get("description") or ""
            out.append({
                "source": "Arbeitnow",
                "company": company,
                "title": title,
                "location": locs,
                "employment_type": "",
                "remote_hybrid_onsite": "Remote" if "remote" in (description.lower() + title.lower()) else "",
                "posted_date": posted_date,
                "salary_currency": "",
                "salary_min": "",
                "salary_max": "",
                "salary_text": "",
                "visa_sponsorship": "Unknown",
                "experience_level": "",
                "apply_url": apply_url,
                "job_url": apply_url,
                "skills_must": "",
                "skills_nice": "",
                "keywords": tags,
                "jd_text": description,
            })
            if len(out) >= limit:
                break
    except Exception:
        return out
    return out

def fetch_usajobs(query, location, days_back, limit, api_key, user_agent):
    out = []
    if not api_key or not user_agent:
        return out
    headers = {"User-Agent": user_agent, "Host": "data.usajobs.gov", "Authorization-Key": api_key}
    params = {"Keyword": query, "ResultsPerPage": min(500, limit), "SortField": "PostingDate", "SortDirection": "Descending"}
    if location:
        params["LocationName"] = location
    try:
        r = requests.get(USAJOBS_BASE, headers=headers, params=params, timeout=25)
        if r.status_code != 200:
            return out
        data = r.json()
        records = (data.get("SearchResult", {}) or {}).get("SearchResultItems", [])
        cutoff = datetime.utcnow().date() - timedelta(days=days_back)
        for item in records:
            d = item.get("MatchedObjectDescriptor", {})
            title = d.get("PositionTitle","")
            company = d.get("OrganizationName","") or ""
            locs = d.get("PositionLocation",[]) or []
            loc_txt = ", ".join([l.get("LocationName","") for l in locs]) if locs else ""
            posted_date = ""
            try:
                pd0 = d.get("PublicationStartDate") or d.get("PositionStartDate") or ""
                if pd0: posted_date = datetime.fromisoformat(pd0.replace("Z","+00:00")).date().isoformat()
            except Exception:
                posted_date = ""
            if posted_date and posted_date < cutoff.isoformat():
                continue
            pay = d.get("PositionRemuneration") or [{}]
            salary_min = pay[0].get("MinimumRange","") if pay else ""
            salary_max = pay[0].get("MaximumRange","") if pay else ""
            salary_currency = pay[0].get("CurrencyCode","") if pay else ""
            apply_url = (d.get("ApplyURI") or [None])[0] or ""
            job_url = d.get("PositionURI","") or apply_url
            jd_text = d.get("UserArea",{}).get("Details",{}).get("JobSummary","") or d.get("QualificationSummary","") or ""
            schedule = (d.get("PositionSchedule") or [{}])[0].get("Name","")
            remote_hybrid_onsite = "Remote" if "remote" in jd_text.lower() else ("Hybrid" if "hybrid" in jd_text.lower() else ("Onsite" if loc_txt else ""))
            experience_level = ""
            grade = (d.get("JobGrade") or [{}])[0].get("Code","")
            if grade: experience_level = f"GS-{grade}"
            out.append({
                "source": "USAJOBS",
                "company": company,
                "title": title,
                "location": loc_txt,
                "employment_type": schedule,
                "remote_hybrid_onsite": remote_hybrid_onsite,
                "posted_date": posted_date,
                "salary_currency": salary_currency,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_text": "",
                "visa_sponsorship": "Unknown",
                "experience_level": experience_level,
                "apply_url": apply_url,
                "job_url": job_url,
                "skills_must": "",
                "skills_nice": "",
                "keywords": "",
                "jd_text": jd_text,
            })
            if len(out) >= limit:
                break
    except Exception:
        return out
    return out

def fetch_remotive(query, days_back, limit, locations):
    out = []
    try:
        r = requests.get(REMOTIVE_URL, params={"search": query}, timeout=20)
        if r.status_code != 200:
            return out
        data = r.json()
        jobs = data.get("jobs", []) or []
        cutoff = datetime.utcnow().date() - timedelta(days=days_back)
        for j in jobs:
            date_str = j.get("publication_date","")
            posted_date = ""
            try:
                posted_date = datetime.fromisoformat(date_str.replace("Z","+00:00")).date().isoformat()
            except Exception:
                posted_date = ""
            if posted_date and posted_date < cutoff.isoformat():
                continue
            loc = j.get("candidate_required_location","") or ""
            if locations:
                ll = loc.lower()
                keep = False
                for L in locations:
                    Llow = L.lower()
                    if Llow in ll or (Llow in ["remote", "anywhere"] and ("remote" in ll or "anywhere" in ll or "worldwide" in ll)):
                        keep = True; break
                if not keep:
                    continue
            jd_text = j.get("description","") or ""
            out.append({
                "source": "Remotive",
                "company": j.get("company_name",""),
                "title": j.get("title",""),
                "location": loc,
                "employment_type": j.get("job_type",""),
                "remote_hybrid_onsite": "Remote",
                "posted_date": posted_date,
                "salary_currency": "",
                "salary_min": "",
                "salary_max": "",
                "salary_text": j.get("salary","") or "",
                "visa_sponsorship": "Unknown",
                "experience_level": "",
                "apply_url": j.get("url",""),
                "job_url": j.get("url",""),
                "skills_must": "",
                "skills_nice": "",
                "keywords": ", ".join(j.get("tags",[]) or []),
                "jd_text": jd_text,
            })
            if len(out) >= limit:
                break
    except Exception:
        return out
    return out

def fetch_jsearch(query, location, days_back, limit, rapidapi_key):
    out = []
    if not rapidapi_key:
        return out
    headers = {"X-RapidAPI-Key": rapidapi_key, "X-RapidAPI-Host": "jsearch.p.rapidapi.com"}
    params = {"query": f"{query} {location or ''}".strip(), "date_posted": "month", "page": "1", "num_pages": str(max(1, min(20, limit//20 + 1)))}
    try:
        r = requests.get(JSEARCH_URL, headers=headers, params=params, timeout=25)
        if r.status_code != 200:
            return out
        data = r.json()
        for item in data.get("data", []) or []:
            posted_date = ""
            try:
                pd0 = item.get("job_posted_at_datetime_utc")
                if pd0: posted_date = datetime.fromisoformat(pd0.replace("Z","+00:00")).date().isoformat()
            except Exception:
                posted_date = ""
            jd_text = item.get("job_description","") or ""
            out.append({
                "source": "JSearch",
                "company": item.get("employer_name","") or "",
                "title": item.get("job_title","") or "",
                "location": item.get("job_city","") or item.get("job_state","") or item.get("job_country","") or "",
                "employment_type": item.get("job_employment_type","") or "",
                "remote_hybrid_onsite": "Remote" if item.get("job_is_remote") else "",
                "posted_date": posted_date,
                "salary_currency": "",
                "salary_min": "",
                "salary_max": "",
                "salary_text": item.get("job_salary","") or "",
                "visa_sponsorship": "Unknown",
                "experience_level": "",
                "apply_url": item.get("job_apply_link","") or "",
                "job_url": item.get("job_apply_link","") or "",
                "skills_must": "",
                "skills_nice": "",
                "keywords": "",
                "jd_text": jd_text,
            })
            if len(out) >= limit:
                break
    except Exception:
        return out
    return out

def fetch_jooble(query, location, days_back, limit, jooble_key):
    out = []
    if not jooble_key:
        return out
    try:
        url = JOOBLE_URL_FMT.format(key=jooble_key)
        payload = {"keywords": query, "location": location or "", "page": 1, "searchMode": 1}
        r = requests.post(url, json=payload, timeout=25)
        if r.status_code != 200:
            return out
        data = r.json()
        jobs = data.get("jobs", []) or []
        cutoff = datetime.utcnow().date() - timedelta(days=days_back)
        for j in jobs:
            posted_date = ""
            try:
                posted_date = (j.get("updated") or j.get("posted")) or ""
            except Exception:
                posted_date = ""
            if posted_date and posted_date < cutoff.isoformat():
                continue
            out.append({
                "source": "Jooble",
                "company": j.get("company","") or "",
                "title": j.get("title","") or "",
                "location": j.get("location","") or "",
                "employment_type": j.get("type","") or "",
                "remote_hybrid_onsite": "Remote" if "remote" in (j.get("snippet","").lower() + j.get("title","").lower()) else "",
                "posted_date": posted_date,
                "salary_currency": "",
                "salary_min": "",
                "salary_max": "",
                "salary_text": j.get("salary","") or "",
                "visa_sponsorship": "Unknown",
                "experience_level": "",
                "apply_url": j.get("link","") or "",
                "job_url": j.get("link","") or "",
                "skills_must": "",
                "skills_nice": "",
                "keywords": "",
                "jd_text": j.get("snippet","") or "",
            })
            if len(out) >= limit:
                break
    except Exception:
        return out
    return out

# ---------------- Excel Export ----------------
def to_excel(df: pd.DataFrame, exact_cols=True) -> bytes:
    cols = COLS if exact_cols else TRACKER_COLS
    existing = [c for c in cols if c in df.columns]
    # coerce NaN -> ""
    df = df.copy()
    df[existing] = df[existing].fillna("")
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
        df[existing].to_excel(writer, index=False, sheet_name="jobs")
        ws = writer.sheets["jobs"]
        ws.autofilter(0, 0, len(df), len(existing)-1)
        for i in range(len(existing)):
            ws.set_column(i, i, 20)
    return output.getvalue()

def parse_locations(raw: str):
    if not raw.strip():
        return []
    return [x.strip() for x in raw.split(",") if x.strip()]

# ---------------- Main Run ----------------
if run_btn:
    resume_skills, resume_text = parse_resume(uploaded_resume)
    st.success(f"Parsed {len(resume_skills)} resume skills: " + (", ".join(sorted(resume_skills)) if resume_skills else "None"))

    all_rows = []
    locations = parse_locations(location_query)
    locs_to_query = locations if locations else [""]

    if use_adzuna:
        st.write("Querying Adzuna…")
        for L in locs_to_query:
            all_rows += fetch_adzuna(role_query, L, days_back, max_jobs, adzuna_app_id, adzuna_app_key)

    if use_usajobs:
        st.write("Querying USAJOBS…")
        for L in locs_to_query:
            all_rows += fetch_usajobs(role_query, L, days_back, max_jobs, usajobs_key, usajobs_user_agent)

    if use_arbeit:
        st.write("Querying Arbeitnow…")
        ab = fetch_arbeitnow(days_back, max_jobs, role_query)
        if locations:
            tmp = []
            for r in ab:
                loc = (r.get("location","") or "").lower()
                keep = any(L.lower() in loc for L in locations) or ("remote" in loc and any(L.lower() == "remote" for L in locations))
                if keep:
                    tmp.append(r)
            ab = tmp
        all_rows += ab

    if use_remotive:
        st.write("Querying Remotive…")
        all_rows += fetch_remotive(role_query, days_back, max_jobs, locations)

    if use_jsearch:
        st.write("Querying JSearch…")
        for L in locs_to_query:
            all_rows += fetch_jsearch(role_query, L, days_back, max_jobs, rapidapi_key)

    if use_jooble:
        st.write("Querying Jooble…")
        for L in locs_to_query:
            all_rows += fetch_jooble(role_query, L, days_back, max_jobs, jooble_key)

    # Deduplicate
    seen = set()
    deduped = []
    for r in all_rows:
        key = (r.get("company",""), r.get("title",""), r.get("location",""), r.get("job_url",""))
        if key not in seen:
            seen.add(key)
            deduped.append(r)

    # Extract skills, score, and build suggestions with robust fallback
    openai_failures = 0
    for row in deduped:
        jd_text = row.get("jd_text","") or ""

        llm = None
        meta = None
        if use_openai and openai_api_key:
            llm, meta = llm_score_resume_job(openai_api_key, resume_text, jd_text, debug=debug_mode)
            if debug_mode and meta and meta.get("error"):
                st.warning(f"OpenAI issue for '{row.get('title','')[:40]}': {meta}")

        # Use LLM if present AND non-empty; else deterministic fallback
        used_llm = False
        if llm and (llm.get("match_score") is not None):
            row["skills_must"] = ", ".join(llm.get("skills_must", []))
            row["skills_nice"] = ", ".join(llm.get("skills_nice", []))
            row["keywords"]    = ", ".join(llm.get("keywords", []))
            row["match_score"] = llm.get("match_score", 0.0)
            row["missing_skills"] = ", ".join(llm.get("missing_skills", []))
            row["matched_skills"] = ", ".join(llm.get("matched_skills", []))
            row["skill_match_count"] = len(llm.get("matched_skills", []))
            used_llm = True

        if (not used_llm) or (row.get("match_score") in [None, "", float('nan')]):
            # Deterministic path (ensures non-empty columns)
            must, nice, keys = extract_skills_from_jd(jd_text)
            row["skills_must"] = ", ".join(sorted(set(must)))
            row["skills_nice"] = ", ".join(sorted(set(nice)))
            existing_keys = [k.strip() for k in (row.get("keywords","") or "").split(",") if k.strip()]
            keys = sorted(set(existing_keys + keys))
            row["keywords"] = ", ".join(keys)
            score_pct, match_count, missing, matched = compute_match_breakdown(row, resume_skills)
            row["match_score"] = score_pct
            row["skill_match_count"] = match_count
            row["missing_skills"] = missing
            row["matched_skills"] = matched

        row["notes"] = row.get("notes","")
        row["suggested recoomendations"] = build_recommendations(row.get("missing_skills",""))

    df = pd.DataFrame(deduped)

    # Coerce NaN to empty strings for display
    for c in ["match_score","skill_match_count","missing_skills","matched_skills","skills_must","skills_nice","keywords","notes","suggested recoomendations"]:
        if c in df.columns:
            df[c] = df[c].fillna("")

    # Enrich companies (optional Clearbit)
    st.write("Enriching company info (public autocomplete; optional company enrichment if CLEARBIT_KEY set)…")
    def clearbit_autocomplete(company_name):
        try:
            if not company_name:
                return {}
            r = requests.get("https://autocomplete.clearbit.com/v1/companies/suggest",
                             params={"query": company_name}, timeout=10)
            if r.status_code != 200:
                return {}
            items = r.json() or []
            if not items:
                return {}
            first = items[0]
            return {"company_domain": first.get("domain",""), "company_name_norm": first.get("name",""), "company_logo": first.get("logo","")}
        except Exception:
            return {}
    def clearbit_company(domain, api_key):
        if not api_key or not domain:
            return {}
        try:
            r = requests.get("https://company.clearbit.com/v2/companies/find",
                             params={"domain": domain},
                             headers={"Authorization": f"Bearer {api_key}"},
                             timeout=10)
            if r.status_code != 200:
                return {}
            d = r.json() or {}
            return {"company_size": (d.get("metrics") or {}).get("employeesRange",""),
                    "company_industry": (d.get("category") or {}).get("industry",""),
                    "company_country": (d.get("geo") or {}).get("country",""),
                    "company_hq": (d.get("geo") or {}).get("city","")}
        except Exception:
            return {}

    for col in ["company_domain","company_logo","company_size","company_industry","company_country","company_hq"]:
        if col not in df.columns:
            df[col] = ""
    cache = {}
    for i, row in df.iterrows():
        comp = (row.get("company") or "").strip()
        if not comp:
            continue
        info = cache.get(comp)
        if not info:
            ac = clearbit_autocomplete(comp)
            info = dict(ac)
            if ac.get("company_domain"):
                info.update(clearbit_company(ac["company_domain"], clearbit_key))
            cache[comp] = info
        for k in ["company_domain","company_logo","company_size","company_industry","company_country","company_hq"]:
            df.at[i, k] = info.get(k, "")

    # High-value missing skills (aggregate)
    missing_counts = {}
    for ms in df.get("missing_skills", []):
        for s in [x.strip() for x in (ms or "").split(",") if x.strip()]:
            missing_counts[s] = missing_counts.get(s, 0) + 1
    top_missing = sorted(missing_counts.items(), key=lambda x: x[1], reverse=True)[:20]
    if top_missing:
        st.subheader("High-Value Missing Skills (across jobs)")
        st.write(pd.DataFrame(top_missing, columns=["skill","count"]))

    # Editable tracker view
    st.subheader("All Jobs (editable tracker view)")
    if "status" not in df.columns:
        df["status"] = ""
    editable_cols = ["source","company","title","location","posted_date","match_score","matched_skills","missing_skills","suggested recoomendations","experience_level","employment_type","remote_hybrid_onsite","salary_text","company_industry","company_size","company_hq","status","notes"]
    for c in editable_cols:
        if c not in df.columns:
            df[c] = ""
    edited = st.data_editor(df[editable_cols], num_rows="dynamic", use_container_width=True)
    for col in ["status","notes","suggested recoomendations"]:
        if col in edited.columns:
            df[col] = edited[col]

    # Primary export (spec columns only)
    primary_df = df.copy()
    for c in COLS:
        if c not in primary_df.columns:
            primary_df[c] = ""
    st.download_button(
        "⬇️ Download job_search_master.xlsx (spec-compliant)",
        data=to_excel(primary_df[COLS], exact_cols=True),
        file_name="job_search_master.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

    # Tracker export (includes suggestions & enrichment)
    for c in TRACKER_COLS:
        if c not in df.columns:
            df[c] = ""
    st.download_button(
        "⬇️ Download job_tracker.xlsx (with status, suggestions & enrichment)",
        data=to_excel(df[TRACKER_COLS], exact_cols=False),
        file_name="job_tracker.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )

else:
    st.info("Optional OpenAI-powered matching is enabled in the sidebar. If match columns are blank, enable debug and check for API errors; the app now guarantees a deterministic fallback so match fields are always filled.")