import requests
import os
import json
import re
from time import sleep

# API base URLs and your API key
DOCUMENTS_URL = "https://api.regulations.gov/v4/documents"
COMMENTS_URL = "https://api.regulations.gov/v4/comments"
API_KEY = "YOUR REGULATIONS.GOV API KEY HERE"

def extract_docket_id_or_document_id(link):
    """
    Extracts the docket ID or document ID from the provided regulations.gov link.
    """
    if "document" in link:
        match = re.search(r'document/([A-Z0-9-]+-\d+)', link)
        if match:
            return match.group(1), "document"
    elif "docket" in link:
        match = re.search(r'docket/([A-Z0-9-]+)', link)
        if match:
            return match.group(1), "docket"
    print("Invalid link format. Could not extract docket or document ID.")
    return None, None

def fetch_object_id_from_document(document_id):
    """
    Fetches the objectId for a specific document ID.
    """
    url = f"{DOCUMENTS_URL}/{document_id}?api_key={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get("data", {}).get("attributes", {}).get("objectId")
    else:
        print(f"Error fetching document: {response.status_code} - {response.text}")
        return None

def fetch_full_comment_data(self_link):
    """
    Fetches the full comment details using the 'self' link.
    """
    url = f"{self_link}?api_key={API_KEY}"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json().get("data", {})
    else:
        print(f"Error fetching full comment data: {response.status_code} - {response.text}")
        return None

def get_comments_by_object_id(object_id, page_size=250, page_number=1):
    """
    Fetches a page of comments for a specific objectId.
    """
    url = f"{COMMENTS_URL}?filter[commentOnId]={object_id}&page[size]={page_size}&page[number]={page_number}&api_key={API_KEY}"
    url += "&sort=lastModifiedDate"

    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching comments: {response.status_code} - {response.text}")
        return None

def fetch_all_comments(object_id, filename, total_comments):
    """
    Fetches all comments for a given objectId, including only the required fields and downloading attachments.
    """
    cleaned_comments = []
    page_number = 1
    page_size = 250  # Default page size for maximum efficiency
    downloaded_comments = 0

    while True:
        print(f"Fetching page {page_number} for objectId {object_id}...")
        response = get_comments_by_object_id(object_id, page_size=page_size, page_number=page_number)

        if not response or "data" not in response or not response["data"]:
            break  # Exit loop if no more data

        for comment in response["data"]:
            if downloaded_comments >= total_comments:
                break

            if "links" in comment and "self" in comment["links"]:
                full_comment = fetch_full_comment_data(comment["links"]["self"])
                if full_comment:
                    # Extract required fields
                    comment_id = full_comment.get("id", "")
                    comment_text = full_comment.get("attributes", {}).get("comment", "")
                    attachments_metadata = fetch_attachments(comment_id)

                    # Add the cleaned comment to the list
                    cleaned_comment = {
                        "comment_id": comment_id,
                        "text": comment_text,
                        "attachments": attachments_metadata
                    }
                    cleaned_comments.append(cleaned_comment)

                    # Increment comment counter
                    downloaded_comments += 1

            sleep(0.1)  # Avoid overwhelming the API with rapid requests

        save_comments_incrementally(cleaned_comments, filename, page_number)
        cleaned_comments = []  # Reset for next batch

        if downloaded_comments >= total_comments or len(response["data"]) < page_size:
            break

        page_number += 1

    print(f"Completed fetching {downloaded_comments} comments for objectId {object_id}.")

def fetch_attachments(comment_id):
    """
    Fetches and downloads attachments for a specific comment.
    """
    attachments_url = f"https://api.regulations.gov/v4/comments/{comment_id}/attachments?api_key={API_KEY}"
    response = requests.get(attachments_url)
    if response.status_code == 200:
        attachments_metadata = []
        attachments = response.json().get("data", [])
        for attachment_index, attachment in enumerate(attachments):
            file_formats = attachment.get("attributes", {}).get("fileFormats", [])
            if not isinstance(file_formats, list):
                print(f"No valid file formats found for attachment in comment {comment_id}")
                continue
            for file_format in file_formats:
                file_url = file_format.get("fileUrl")
                if file_url:
                    file_path = download_file(file_url, comment_id, attachment_index)
                    attachments_metadata.append({"url": file_url, "file_path": file_path})
        return attachments_metadata
    else:
        print(f"Failed to retrieve attachments for comment {comment_id}: {response.status_code}")
        return []

def download_file(url, comment_id, attachment_index):
    """
    Downloads a single file from the provided URL and saves it with the appropriate extension.
    """
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        # Determine file extension
        content_type = response.headers.get("Content-Type")
        extension = get_extension_from_content_type(content_type)
        if not extension:
            extension = "dat"  # Default extension if type is unknown

        # Save the file with a unique name
        os.makedirs("./Downloads/USDA_JSON/attachments", exist_ok=True)
        file_path = f"./Downloads/USDA_JSON/attachments/{comment_id}_{attachment_index}.{extension}"
        with open(file_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)
        print(f"Downloaded attachment {attachment_index} for comment {comment_id} to {file_path}")
        return file_path
    else:
        print(f"Failed to download file from {url}: {response.status_code}")
        return None

def get_extension_from_content_type(content_type):
    """
    Maps Content-Type to a file extension.
    """
    if content_type == "application/pdf":
        return "pdf"
    elif content_type == "image/jpeg":
        return "jpg"
    elif content_type == "image/png":
        return "png"
    elif content_type == "application/zip":
        return "zip"
    # Add more content types as needed
    return None

def save_comments_incrementally(comments, filename, page_number):
    """
    Saves a single page of cleaned comments to a JSON file incrementally.
    """
    page_filename = filename.replace(".json", f"_page_{page_number}.json")
    os.makedirs(os.path.dirname(page_filename), exist_ok=True)
    with open(page_filename, 'w', encoding='utf-8') as f:
        json.dump(comments, f, indent=4, ensure_ascii=False)
    print(f"Page {page_number} comments saved to {page_filename}")

def main():
    link = input("Enter the docket or document link: ")
    id_value, id_type = extract_docket_id_or_document_id(link)

    if not id_value:
        return

    total_comments = input("Enter the number of comments to download (or type 'all' to download everything): ").strip().lower()
    if total_comments == "all":
        total_comments = float("inf")  # Download all comments
    else:
        total_comments = int(total_comments)

    if id_type == "document":
        print(f"Fetching objectId for document ID: {id_value}...")
        object_id = fetch_object_id_from_document(id_value)
        if object_id:
            print(f"ObjectId retrieved: {object_id}")
            filename = f"./Downloads/USDA_JSON/comments_{id_value}.json"
            fetch_all_comments(object_id, filename, total_comments)
        else:
            print("No objectId found for the document.")
    else:
        print("Invalid ID type. Exiting.")

if __name__ == "__main__":
    main()
