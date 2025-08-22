# Job Search Automation & CRM (OpenAI + Multi-Source)

A Streamlit app that **aggregates jobs** from multiple free APIs, compares them against your **resume** (PDF/DOCX/TXT), calculates a **match score**, and lets you track your applications like a mini-CRM.

---

## 🚀 Features
- **Multi-source jobs**: Adzuna, USAJOBS, Arbeitnow, Remotive, JSearch (RapidAPI), Jooble  
- **Multi-location search**: Enter multiple locations (comma-separated) like `New Jersey, New York, Remote`  
- **AI-powered matching**: OpenAI LLM extracts skills and computes match %  
- **Deterministic fallback**: Regex-based scorer ensures no empty matches if API fails  
- **Skill insights**: matched vs. missing skills, skill match count, and aggregated "high-value missing skills"  
- **Suggested recommendations**: resume edits per job in a dedicated column  
- **Company enrichment**: fetch company domain, logo, size, industry, HQ  
- **CRM-style tracker**: mark jobs as Applied / Interested / Interviewing, and edit notes inline  
- **Exports**:
  - `job_search_master.xlsx` (spec columns only)
  - `job_tracker.xlsx` (adds enrichment, suggestions & statuses)

---

## 🖥️ Run Locally
```bash
# 1) Create & activate virtualenv
python -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate

# 2) Install dependencies
pip install -r requirements.txt

# 3) Run the app
export OPENAI_API_KEY=sk-...   # or paste in the sidebar
streamlit run app.py
