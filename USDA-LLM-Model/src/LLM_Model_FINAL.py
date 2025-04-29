import os
import json
import time
import pandas as pd
import pdfplumber
from openai import OpenAI
from dotenv import load_dotenv, find_dotenv
import pytesseract
from pdf2image import convert_from_path
from PIL import Image

# 🔹 Load API Key Securely from .env File
env_path = find_dotenv(".env")  # Ensures correct file is found
if env_path:
    load_dotenv(env_path)
else:
    print(" WARNING: .env file not found! Make sure it exists.")

# 🔹 Retrieve API Key from Environment
API_KEY = os.getenv("OPENAI_API_KEY")

# 🔹 Check if API Key is Loaded Correctly
if not API_KEY:
    raise ValueError("❌ ERROR: API key not found. Ensure it is set correctly in the .env file.")

# 🔹 Initialize OpenAI Client
client = OpenAI(api_key=API_KEY)

# 🔹 CONFIGURATION: Set Paths Here
JSON_FOLDER = r"C:\Users\jcstr\Downloads\USDA_JSON"  # Path to JSON files
PDF_FOLDER = r"C:\Users\jcstr\Downloads\USDA_JSON\attachments"  # Path to folder containing local PDFs
OUTPUT_FILE = "processed_comments.csv"  # Output CSV file
USE_API = True  # Set to False if testing without OpenAI API
MAX_TOKENS = 50000  # Keep within OpenAI's input size limit

# 🔹 Set Tesseract Path (Only Needed for Windows)
pytesseract.pytesseract.tesseract_cmd = r"C:\Program Files\Tesseract-OCR"  # Update this path if needed


def extract_text_from_pdf(pdf_path):
    """Extracts text from a locally stored PDF file using both pdfplumber and OCR (Tesseract)."""
    text = ""

    #  First Try Extracting Text Normally Using pdfplumber
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + "\n"

    except Exception as e:
        print(f"⚠️ Error extracting text from {pdf_path} using pdfplumber: {e}")

    # ✅ If No Text is Found, Use OCR (Tesseract)
    if not text.strip():
        print(f"🔍 No text found in {pdf_path}, running OCR...")
        try:
            images = convert_from_path(pdf_path)
            ocr_text = ""
            for i, img in enumerate(images):
                ocr_text += pytesseract.image_to_string(img) + "\n"
            text = ocr_text.strip()

        except Exception as e:
            print(f"⚠️ OCR failed for {pdf_path}: {e}")
            return "Unknown (OCR failed)"

    return text.strip() if text else "Unknown (PDF unreadable)"


def classify_comment_by_issue(comment_text, pdf_attached=False):
    """
    Classifies the comment and extracts:
      - who_type (individual, organization, or anonymous)
      - who_name (name/identifier of the commenter)
      - what (change or action requested, in detail)
      - why (detailed reasons for the request)
      - issues (list of strings, with specific detail)
      - scientific_legal_support ("Yes"/"No")
    """

    # If we are NOT using the API (for local testing), return sample data
    if not USE_API:
        return {
            "who_type": "Test",
            "who_name": "Test Name",
            "what": "Test Request",
            "why": "Test Reason",
            "issues": ["Test Issue"],
            "scientific_legal_support": "No",
        }

    # If text is too large, truncate to avoid token-limit issues
    if len(comment_text) > MAX_TOKENS:
        comment_text = comment_text[:MAX_TOKENS] + " [TRUNCATED]"

    # 🔹 Prompt to encourage detailed "what", "why", and specific issues
    prompt = f"""
You are an expert policy analyst analyzing public comments. Your task is to extract detailed information from the comment provided.

INSTRUCTIONS:
1. Identify who is making the comment:
   - "who_type": one of ["individual", "organization", "anonymous"].
     If not explicitly stated, infer from context. 
     Otherwise, use "anonymous."
   - "who_name": if it's specified or can be inferred (e.g., "John Doe," "National Tribal Fisheries Association"). 
     If no name is given and you cannot infer, return "Unknown."

2. For "what": Provide a detailed explanation of what the commenter is requesting.
   If the commenter has multiple requests, list them all in one string (comma- or semicolon-delimited).

3. For "why": Provide a detailed explanation of the reasons for this request 
   or the concerns they raise. Reflect each concern with nuance, rather than a brief phrase.

4. For "issues": List all specific issues the commenter addresses in an array of strings.
   - Avoid overly broad categories like "Water Quality" if the comment references something more specific 
     (e.g., "PCB contamination in local waterways," "Impacts on tribal fishers," etc.).
   - If multiple distinct issues are raised, include each as a separate element in the array.

5. For "scientific_legal_support": 
   - Return "Yes" if the text or attachments reference research data, footnotes, official legal citations (e.g., 40 CFR, USC), 
     case law references, or bracketed footnote markers (e.g., "[1]", "[2]").
   - Return "Yes" if {pdf_attached} is True and the text suggests the PDF may contain references or citations.
   - Otherwise, return "No."

RESPONSE FORMAT (valid JSON):
{{
  "who_type": "organization",
  "who_name": "National Tribal Fisheries Association",
  "what": "Requests stricter limits on PCB discharge; Requests additional funding for water testing",
  "why": "Concerned about contamination of fish consumed by tribal communities; Believes current policy does not adequately protect subsistence fishers",
  "issues": ["PCB contamination", "Impacts on tribal fishers", "Public health risks"],
  "scientific_legal_support": "Yes"
}}

COMMENT TO ANALYZE:
\"\"\"{comment_text}\"\"\"
"""

    try:
        response = client.chat.completions.create(
            model="gpt-4-turbo",
            messages=[
                {"role": "system", "content": "You are an expert policy analyst."},
                {"role": "user", "content": prompt}
            ],
            temperature=0.0
        )
        time.sleep(1)

        content = response.choices[0].message.content.strip()
        clean_json = extract_json_block(content)
        response_data = json.loads(clean_json)

        # Parse issues as a list (in case GPT returns a string)
        if isinstance(response_data.get("issues", []), list):
            parsed_issues = response_data["issues"]
        else:
            parsed_issues = [response_data.get("issues", "Needs manual review")]

        return {
            "who_type": response_data.get("who_type", "Unknown"),
            "who_name": response_data.get("who_name", "Unknown"),
            "what": response_data.get("what", "Unknown"),
            "why": response_data.get("why", "Unknown"),
            "issues": parsed_issues,
            "scientific_legal_support": response_data.get("scientific_legal_support", "No"),
        }

    except (json.JSONDecodeError, AttributeError):
        print("⚠️ JSONDecodeError or invalid response. Using fallback.")
        return {
            "who_type": "Unknown",
            "who_name": "Unknown",
            "what": "Unknown",
            "why": "Unknown",
            "issues": ["Needs manual review"],
            "scientific_legal_support": "No",
        }

    except Exception as e:
        print(f"⚠️ API Error: {e}. Using fallback.")
        return {
            "who_type": "Unknown",
            "who_name": "Unknown",
            "what": "Unknown",
            "why": "Unknown",
            "issues": ["Needs manual review"],
            "scientific_legal_support": "No",
        }


def process_json_comments(json_file, pdf_folder):
    """
    Processes all comments stored in a JSON file and extracts text from local PDF attachments.
    Returns a list of dictionaries, each containing relevant comment data.
    """
    results = []

    # Load the JSON data
    with open(json_file, "r", encoding="utf-8") as f:
        comments_data = json.load(f)

    print(f"📂 Loaded {len(comments_data)} comments from JSON: {json_file}")

    # Iterate over each comment in the JSON
    for comment in comments_data:
        comment_id = comment.get("comment_id", f"unknown_{len(results)}")
        text_raw = comment.get("text")
        comment_text = text_raw.strip() if isinstance(text_raw, str) else ""
        attachments = comment.get("attachments", [])
        comment_link = f"https://www.regulations.gov/comment/{comment_id}"

        extracted_texts = []

        # Count how many PDFs are attached
        pdf_count = sum(
            1 for att in attachments
            if att.get("file_path", "").lower().endswith(".pdf")
        )
        pdf_attached = (pdf_count > 0)

        # If the comment has actual text, append it
        if comment_text and comment_text.lower() not in ["see attached file(s)", "see attached"]:
            extracted_texts.append(f"Comment Text: {comment_text}")

        # Process local PDF attachments (extract text via extract_text_from_pdf)
        for attachment in attachments:
            file_path = attachment.get("file_path", "")
            if file_path.endswith(".pdf"):
                local_pdf_path = os.path.join(pdf_folder, os.path.basename(file_path))
                if os.path.exists(local_pdf_path):
                    print(f"🔍 Extracting local PDF for comment {comment_id}: {local_pdf_path}")
                    pdf_text = extract_text_from_pdf(local_pdf_path)
                    if pdf_text:
                        extracted_texts.append(f"Extracted PDF Text: {pdf_text}")
                else:
                    print(f"⚠️ PDF file not found: {local_pdf_path}")

        # Combine all extracted text
        combined_text = "\n\n".join(extracted_texts).strip()

        # Skip comment if it has no text and no PDFs with extractable content
        if not combined_text:
            print(f"⚠️ Skipping empty comment: {comment_id}")
            continue

        # Classify the combined text with GPT, passing the pdf_attached flag
        summary = classify_comment_by_issue(combined_text, pdf_attached=pdf_attached)

        # Build final data for CSV
        data = {
            "comment_id": comment_id,
            "comment_link": comment_link,
            "who_type": summary["who_type"],
            "who_name": summary["who_name"],
            "what": summary["what"],
            "why": summary["why"],
            "issues": ", ".join(summary["issues"]),
            "scientific_legal_support": summary["scientific_legal_support"],
            "pdf_attachments_present": "Yes" if pdf_attached else "No",
            "pdf_attachments_count": pdf_count
        }

        results.append(data)
        print(f"✅ Processed JSON comment: {comment_id}")

    return results


def process_all_comments(json_folder, pdf_folder, output_file):
    """
    Processes multiple JSON files in a folder and extracts all comments.
    Saves results to a single CSV.
    """
    results = []

    # ✅ Loop through all JSON files in the folder
    for filename in os.listdir(json_folder):
        if filename.endswith(".json"):  # Only process JSON files
            json_file_path = os.path.join(json_folder, filename)
            print(f"📂 Processing JSON file: {json_file_path}")
            new_results = process_json_comments(json_file_path, pdf_folder)
            results.extend(new_results)

    # ✅ Save all results to a single CSV file
    if results:
        df = pd.DataFrame(results)
        df.to_csv(output_file, mode='w', index=False)
        print(f"\n✅ Processed {len(results)} total comments from all JSON files.")
        print(f"Output saved to {output_file}")
    else:
        print("⚠️ No comments were processed. Check if JSON files contain valid data.")


import re

def extract_json_block(text):
    """
    Extracts the first JSON block from a string using regex.
    Returns the JSON string or raises ValueError if not found.
    """
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        return match.group()
    else:
        raise ValueError("No valid JSON found in response.")





# 🔹 Run the Script
if __name__ == "__main__":
    process_all_comments(JSON_FOLDER, PDF_FOLDER, OUTPUT_FILE)
