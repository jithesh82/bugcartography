# Find and fetch archived web pages from Common Crawl for the given domain www.underarmour.com
import requests
import json
import os
from urllib.parse import urljoin

def find_common_crawl_archives(domain, index="CC-MAIN-2024-51"):
    """
    Query Common Crawl index to find archived pages for a domain
    Returns list of JSON objects with metadata about archived pages
    """
    url = f"https://index.commoncrawl.org/{index}-index?url={domain}&output=json"
    print(f"[*] Querying Common Crawl index: {url}")
    
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            archives = response.text.strip().split('\n')
            return [json.loads(archive) for archive in archives if archive]
        else:
            print(f"[-] Error: Status code {response.status_code}")
            return []
    except Exception as e:
        print(f"[-] Error querying index: {e}")
        return []


def fetch_page_from_common_crawl(archive_info):
    """
    Fetch the actual HTML content from Common Crawl for a given archive record
    """
    try:
        # Construct the Wayback Machine URL to fetch the actual page
        url = f"https://web.archive.org/web/{archive_info['timestamp']}/{archive_info['url']}"
        print(f"[*] Fetching: {url}")
        
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            return response.text
        else:
            print(f"[-] Failed to fetch (status {response.status_code})")
            return None
    except Exception as e:
        print(f"[-] Error fetching page: {e}")
        return None


def save_html_page(content, domain, timestamp, output_dir="archived_pages"):
    """
    Save fetched HTML content to a file
    """
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    filename = f"{output_dir}/{domain}_{timestamp}.html"
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"[+] Saved: {filename}")
        return filename
    except Exception as e:
        print(f"[-] Error saving file: {e}")
        return None


def fetch_common_crawl_pages(domain, max_pages=10, index="CC-MAIN-2024-51"):
    """
    Main function to find and fetch archived pages for a domain
    """
    print(f"\n[*] Starting Common Crawl fetch for domain: {domain}")
    print(f"[*] Using index: {index}")
    print(f"[*] Max pages to fetch: {max_pages}\n")
    
    # Find archives
    archives = find_common_crawl_archives(domain, index)
    print(f"[+] Found {len(archives)} archived pages\n")
    
    if not archives:
        print("[-] No archives found")
        return []
    
    saved_files = []
    
    # Fetch and save pages (limit to max_pages)
    for i, archive_info in enumerate(archives[:max_pages], 1):
        print(f"\n[{i}/{min(len(archives), max_pages)}] Processing archive:")
        print(f"    URL: {archive_info['url']}")
        print(f"    Timestamp: {archive_info['timestamp']}")
        
        # Fetch the page
        html_content = fetch_page_from_common_crawl(archive_info)
        
        if html_content:
            # Save the page
            domain_clean = domain.replace('/', '_').replace('.', '_')
            filename = save_html_page(
                html_content, 
                domain_clean, 
                archive_info['timestamp']
            )
            if filename:
                saved_files.append({
                    'filename': filename,
                    'url': archive_info['url'],
                    'timestamp': archive_info['timestamp']
                })
    
    print(f"\n[+] Successfully fetched and saved {len(saved_files)} pages\n")
    return saved_files


# Main execution
if __name__ == "__main__":
    domain = "underarmour.com"
    
    # Fetch pages from Common Crawl
    results = fetch_common_crawl_pages(domain, max_pages=1)
    
    # Print summary
    if results:
        print("[+] Saved pages summary:")
        for result in results:
            print(f"    - {result['filename']} ({result['timestamp']})")

