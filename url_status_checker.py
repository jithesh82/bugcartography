#!/usr/bin/env python3
"""
URL Status Code Checker for Bug Bounty Reconnaissance
Checks HTTP status codes for URLs from analysis report and prioritizes by severity
"""

import json
import requests
import sys
from collections import defaultdict
from datetime import datetime
from urllib.parse import urlparse
import time

# Status code severity mapping
STATUS_SEVERITY = {
    # Most interesting for bug bounty
    200: "ACCESSIBLE",           # Found and accessible
    201: "ACCESSIBLE",           # Created
    204: "ACCESSIBLE",           # No content but exists
    301: "REDIRECT",             # Moved permanently
    302: "REDIRECT",             # Moved temporarily
    400: "CLIENT_ERROR",         # Bad request
    401: "PROTECTED",            # Authentication required
    403: "FORBIDDEN",            # Forbidden
    404: "NOT_FOUND",            # Not found
    405: "NOT_ALLOWED",          # Method not allowed
    429: "RATE_LIMITED",         # Rate limited
    500: "SERVER_ERROR",         # Server error
    502: "SERVER_ERROR",         # Bad gateway
    503: "SERVER_ERROR",         # Service unavailable
}

def get_status_severity(status_code):
    """Get severity for a status code"""
    status_category = status_code // 100
    
    # Priority: 200 > 401/403 > 500 > 3xx > 404 > others
    if status_code == 200:
        return 1, "ACCESSIBLE"
    elif status_code in (401, 403):
        return 2, "PROTECTED"
    elif status_code >= 500:
        return 3, "SERVER_ERROR"
    elif 300 <= status_code < 400:
        return 4, "REDIRECT"
    elif status_code == 404:
        return 5, "NOT_FOUND"
    else:
        return 6, f"STATUS_{status_code}"


def check_url_status(url, timeout=10):
    """
    Check HTTP status code for a URL
    Returns: (status_code, response_time, headers, error_msg)
    """
    try:
        start_time = time.time()
        
        # Use HEAD request first (faster)
        response = requests.head(
            url,
            timeout=timeout,
            allow_redirects=False,
            verify=False
        )
        
        response_time = time.time() - start_time
        
        return response.status_code, response_time, response.headers, None
        
    except requests.exceptions.ConnectTimeout:
        return None, None, None, "Connection timeout"
    except requests.exceptions.ReadTimeout:
        return None, None, None, "Read timeout"
    except requests.exceptions.ConnectionError:
        return None, None, None, "Connection error"
    except requests.exceptions.InvalidURL:
        return None, None, None, "Invalid URL"
    except Exception as e:
        return None, None, None, str(e)[:50]


def load_json_report(json_file="url_analysis_report.json"):
    """Load the JSON analysis report"""
    try:
        with open(json_file, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"[-] File not found: {json_file}")
        return None
    except json.JSONDecodeError:
        print(f"[-] Invalid JSON in: {json_file}")
        return None


def extract_urls_from_report(report):
    """Extract all URLs from the analysis report"""
    urls_by_risk = defaultdict(list)
    
    findings = report.get('findings', {})
    
    for risk_level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
        if risk_level not in findings:
            continue
        
        patterns = findings[risk_level]
        
        for pattern, url_list in patterns.items():
            for url_item in url_list:
                url = url_item.get('url')
                if url:
                    urls_by_risk[risk_level].append({
                        'url': url,
                        'pattern': pattern,
                        'line': url_item.get('line')
                    })
    
    return urls_by_risk


def check_all_urls(urls_by_risk):
    """Check status codes for all URLs"""
    results = {
        'CRITICAL': [],
        'HIGH': [],
        'MEDIUM': [],
        'LOW': []
    }
    
    total_urls = sum(len(urls) for urls in urls_by_risk.values())
    checked_count = 0
    
    print(f"\n[*] Checking {total_urls} URLs...\n")
    
    for risk_level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
        urls = urls_by_risk.get(risk_level, [])
        
        if not urls:
            continue
        
        print(f"[{risk_level}] Checking {len(urls)} URLs...")
        
        for url_data in urls:
            checked_count += 1
            url = url_data['url']
            
            print(f"  [{checked_count}/{total_urls}] {url[:70]}...", end='', flush=True)
            
            status_code, response_time, headers, error = check_url_status(url)
            
            if status_code:
                severity, status_name = get_status_severity(status_code)
                print(f" -> {status_code} ({status_name}) [{response_time:.2f}s]")
                
                results[risk_level].append({
                    'url': url,
                    'pattern': url_data['pattern'],
                    'status_code': status_code,
                    'status_name': status_name,
                    'severity': severity,
                    'response_time': response_time,
                    'content_type': headers.get('content-type', 'N/A') if headers else 'N/A'
                })
            else:
                print(f" -> ERROR: {error}")
                results[risk_level].append({
                    'url': url,
                    'pattern': url_data['pattern'],
                    'status_code': None,
                    'status_name': 'ERROR',
                    'severity': 7,
                    'response_time': None,
                    'error': error
                })
    
    return results


def generate_status_report(results, output_file="url_status_report.txt"):
    """Generate status code report ordered by severity"""
    
    # Flatten and sort all results by severity and risk level
    all_findings = []
    
    risk_order = {'CRITICAL': 1, 'HIGH': 2, 'MEDIUM': 3, 'LOW': 4}
    
    for risk_level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
        for finding in results[risk_level]:
            all_findings.append((risk_level, finding))
    
    # Sort by: risk level first, then by status severity
    all_findings.sort(
        key=lambda x: (risk_order[x[0]], x[1].get('severity', 999))
    )
    
    with open(output_file, 'w') as f:
        f.write("=" * 100 + "\n")
        f.write("URL STATUS CODE REPORT - BUG BOUNTY RECONNAISSANCE\n")
        f.write("=" * 100 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 100 + "\n\n")
        
        # Summary statistics
        status_counts = defaultdict(int)
        risk_counts = defaultdict(int)
        
        for risk_level, finding in all_findings:
            risk_counts[risk_level] += 1
            status_name = finding.get('status_name', 'UNKNOWN')
            status_counts[status_name] += 1
        
        f.write("SUMMARY\n")
        f.write("-" * 100 + "\n")
        f.write("By Risk Level:\n")
        for risk_level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
            count = risk_counts[risk_level]
            if count > 0:
                f.write(f"  {risk_level:10}: {count:3} URLs\n")
        
        f.write("\nBy Status:\n")
        for status_name in sorted(status_counts.keys()):
            count = status_counts[status_name]
            f.write(f"  {status_name:20}: {count:3} URLs\n")
        
        f.write(f"\nTotal URLs Checked: {len(all_findings)}\n\n")
        
        # Detailed findings sorted by severity
        current_risk = None
        
        for risk_level, finding in all_findings:
            if risk_level != current_risk:
                current_risk = risk_level
                f.write("\n")
                f.write("=" * 100 + "\n")
                f.write(f"{risk_level} RISK FINDINGS\n")
                f.write("=" * 100 + "\n\n")
            
            url = finding.get('url', 'N/A')
            status = finding.get('status_code', 'ERROR')
            status_name = finding.get('status_name', 'UNKNOWN')
            pattern = finding.get('pattern', 'N/A')
            response_time = finding.get('response_time')
            error = finding.get('error')
            content_type = finding.get('content_type', 'N/A')
            
            f.write(f"URL: {url}\n")
            f.write(f"Pattern: {pattern}\n")
            
            if status:
                f.write(f"Status Code: {status} ({status_name})\n")
                if response_time:
                    f.write(f"Response Time: {response_time:.2f}s\n")
                f.write(f"Content-Type: {content_type}\n")
            else:
                f.write(f"Status: ERROR\n")
                f.write(f"Error: {error}\n")
            
            f.write("\n")
        
        # Recommendations
        f.write("\n")
        f.write("=" * 100 + "\n")
        f.write("RECOMMENDATIONS FOR TESTING\n")
        f.write("=" * 100 + "\n\n")
        
        accessible_200 = sum(1 for r, f in all_findings if f.get('status_code') == 200)
        protected = sum(1 for r, f in all_findings if f.get('status_code') in (401, 403))
        errors = sum(1 for r, f in all_findings if f.get('status_code', 0) >= 500)
        
        f.write(f"1. IMMEDIATE TESTING (Status 200 - Accessible):\n")
        f.write(f"   {accessible_200} URLs are directly accessible\n")
        f.write(f"   Priority: Test these first for vulnerabilities\n\n")
        
        f.write(f"2. AUTHENTICATION TESTING (Status 401/403 - Protected):\n")
        f.write(f"   {protected} URLs require authentication/authorization\n")
        f.write(f"   Priority: Test for auth bypass, privilege escalation\n\n")
        
        f.write(f"3. SERVER ERRORS (Status 5xx):\n")
        f.write(f"   {errors} URLs returned server errors\n")
        f.write(f"   Priority: May reveal stack traces or sensitive info\n\n")
        
        f.write("4. WORKFLOW:\n")
        f.write("   a) Test CRITICAL risk + 200 status URLs first\n")
        f.write("   b) Then test HIGH risk + 200 status\n")
        f.write("   c) Test authentication endpoints (401/403)\n")
        f.write("   d) Investigate server errors (5xx)\n")
        f.write("   e) Check redirects and follow chains\n\n")


def generate_json_status_report(results, output_file="url_status_report.json"):
    """Generate JSON status report"""
    
    all_findings = []
    
    for risk_level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
        for finding in results[risk_level]:
            finding['risk_level'] = risk_level
            all_findings.append(finding)
    
    report = {
        "timestamp": datetime.now().isoformat(),
        "total_urls": len(all_findings),
        "findings": all_findings
    }
    
    with open(output_file, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"[+] JSON report saved: {output_file}")


def main():
    print("[*] URL Status Code Checker - Bug Bounty Reconnaissance\n")
    
    # Load the analysis report
    print("[*] Loading analysis report...")
    report = load_json_report("url_analysis_report.json")
    
    if not report:
        sys.exit(1)
    
    # Extract URLs by risk level
    print("[*] Extracting URLs from report...")
    urls_by_risk = extract_urls_from_report(report)
    
    total_urls = sum(len(urls) for urls in urls_by_risk.values())
    if total_urls == 0:
        print("[-] No URLs found in report")
        sys.exit(1)
    
    print(f"[+] Found {total_urls} URLs across all risk levels\n")
    
    # Suppress SSL warnings
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    # Check all URLs
    results = check_all_urls(urls_by_risk)
    
    # Generate reports
    print("\n[*] Generating reports...")
    generate_status_report(results, "url_status_report.txt")
    generate_json_status_report(results, "url_status_report.json")
    
    print("[+] Report saved: url_status_report.txt")
    print("[+] JSON report saved: url_status_report.json\n")
    
    # Print summary
    print("=" * 100)
    print("SUMMARY")
    print("=" * 100)
    
    for risk_level in ['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']:
        findings = results[risk_level]
        if findings:
            accessible = sum(1 for f in findings if f.get('status_code') == 200)
            protected = sum(1 for f in findings if f.get('status_code') in (401, 403))
            errors = sum(1 for f in findings if f.get('status_code', 0) >= 500)
            not_found = sum(1 for f in findings if f.get('status_code') == 404)
            
            print(f"\n{risk_level} RISK ({len(findings)} URLs):")
            print(f"  ✓ Accessible (200):  {accessible}")
            print(f"  🔒 Protected (401/403): {protected}")
            print(f"  ⚠️  Server Errors (5xx): {errors}")
            print(f"  ✗ Not Found (404):   {not_found}")


if __name__ == "__main__":
    main()
