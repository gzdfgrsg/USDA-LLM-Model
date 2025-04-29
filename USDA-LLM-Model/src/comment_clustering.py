import pandas as pd
import json
from openai import OpenAI
from dotenv import load_dotenv
import os
from ast import literal_eval
import re
from collections import Counter
from math import ceil

# Load OpenAI API key
load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

# File paths
INPUT_FILE = r"C:\Users\jcstr\OneDrive\Desktop\LLM Final Outputs\FSIS-2011-0012 1300 Comments\processed_comments.csv"
CATEGORIZED_OUTPUT = "processed_with_categories.csv"
SORTED_OUTPUT = "sorted_by_issue.csv"
GPT_RAW_RESPONSE_LOG = "gpt_issue_grouping_raw.txt"
CATEGORY_MAPPING_LOG = "gpt_category_consolidation.txt"

# Load and clean input
df = pd.read_csv(INPUT_FILE)
df = df[df["issues"].notna()].copy()
df["issues"] = df["issues"].apply(
    lambda x: [i.strip() for i in x.split(",")] if isinstance(x, str) else []
)

# Flatten and count issue frequency
issue_counter = Counter(issue for issues in df["issues"] for issue in issues)
all_issues = list(issue_counter.keys())
print(f"📊 Total unique issues: {len(all_issues)}")

# GPT prompt formatter
def build_prompt(issues):
    issues = sorted(set(issues))  # Remove duplicates just in case
    return f"""
You are helping organize public policy comment data.

Here is a list of issues extracted from public comments. Your task is to group closely related issues into meaningful, broader categories.

INSTRUCTIONS:
- Return only 8–15 categories TOTAL.
- Merge any similar or overlapping terms (e.g., \"worker safety\", \"worker safety and health\", \"workplace conditions\") into a single category.
- If in doubt, consolidate — avoid creating overly granular or redundant categories.
- Categories should reflect common themes across many issues.

Format the result as JSON like this:
[
  {{
    "category": "Broad Issue Category",
    "related_issues": ["Exact issue string 1", "Exact issue string 2"]
  }},
  ...
]

ISSUES:
{chr(10).join("- " + issue for issue in issues)}
"""

# GPT JSON extractor
def extract_json_block(text):
    text = re.sub(r"```(?:json)?", "", text).strip("` \n")
    match = re.search(r"\[\s*\{[\s\S]*?\}\s*\]", text)
    if match:
        json_str = match.group()
        try:
            return json.loads(json_str)
        except json.JSONDecodeError as e:
            print(f"⚠️ JSON decode failed: {e}")
    with open("gpt_issue_grouping_failed_batch.txt", "w", encoding="utf-8") as f:
        f.write(text)
    raise ValueError("❌ GPT returned invalid or incomplete JSON.")

# Process in batches
BATCH_SIZE = 500
num_batches = ceil(len(all_issues) / BATCH_SIZE)
issue_to_category = {}
all_categories = set()

for i in range(num_batches):
    batch_issues = all_issues[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
    print(f"🔄 Processing batch {i+1} of {num_batches} with {len(batch_issues)} issues...")
    prompt = build_prompt(batch_issues)

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are a policy analyst categorizing public issues."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.2
    )

    response_text = response.choices[0].message.content.strip()
    with open(GPT_RAW_RESPONSE_LOG, "a", encoding="utf-8") as f:
        f.write(f"\n\n--- Batch {i+1} ---\n\n")
        f.write(response_text)

    try:
        issue_groupings = extract_json_block(response_text)
        for group in issue_groupings:
            category = group["category"]
            all_categories.add(category)
            related_issues = group["related_issues"][:200]  # Limit to first 200 to prevent overflow
            for issue in related_issues:
                issue_to_category[issue] = category
    except ValueError as e:
        print(f"❌ Skipping batch {i+1} due to JSON parsing error.")
        continue

# Consolidate category names
def consolidate_categories(categories):
    prompt = f"""
You are helping to clean and consolidate category names from a public policy comment analysis.
Below is a list of categories that may contain overlaps or near-duplicates. Your task is to merge similar ones.

Return the output as a JSON dictionary where the keys are original category names, and the values are the merged category names.
Example:
{{
  "Worker Safety and Health": "Worker Safety",
  "Worker Safety and Conditions": "Worker Safety"
}}

CATEGORIES:
{chr(10).join("- " + cat for cat in sorted(categories))}
"""

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "You are an expert in data cleanup."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.2
    )

    raw_text = response.choices[0].message.content.strip()
    with open(CATEGORY_MAPPING_LOG, "w", encoding="utf-8") as f:
        f.write(raw_text)

    try:
        return json.loads(re.sub(r"```(?:json)?", "", raw_text).strip("` \n"))
    except json.JSONDecodeError:
        print("⚠️ Failed to parse category consolidation JSON. Using original categories.")
        return {cat: cat for cat in categories}

category_mapping = consolidate_categories(all_categories)
issue_to_category = {k: category_mapping.get(v, v) for k, v in issue_to_category.items()}

def map_to_categories(issues):
    categories = set()
    for issue in issues:
        category = issue_to_category.get(issue)
        if category:
            categories.add(category)
    return list(categories)

df["high_level_issues"] = df["issues"].apply(map_to_categories)
df.to_csv(CATEGORIZED_OUTPUT, index=False)
print(f"✅ Categorized data saved to: {CATEGORIZED_OUTPUT}")

df_exploded = df.explode("high_level_issues")
df_exploded = df_exploded.rename(columns={"high_level_issues": "issue_category"})
df_exploded = df_exploded[df_exploded["issue_category"].notna()]
df_sorted = df_exploded.sort_values(by=["issue_category", "comment_id"])
df_sorted.to_csv(SORTED_OUTPUT, index=False)
print(f"✅ Sorted data saved to: {SORTED_OUTPUT}")
