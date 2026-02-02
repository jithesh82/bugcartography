#!/usr/bin/env python3
"""
URL Pattern Analyzer for Bug Bounty Reconnaissance
Extracts URLs from archived HTML pages and identifies high-risk patterns
"""

import os
import re
import json
from pathlib import Path
from collections import defaultdict
from urllib.parse import urljoin, urlparse
from datetime import datetime

# URL Patterns organized by risk level
PATTERNS = {
    "CRITICAL": [
        r'/admin',
        r'/administrator',
        r'/admin[-_]panel',
        r'/dashboard',
        r'/control[-_]panel',
        r'/superuser',
        r'/superadmin',
        r'/wp-admin',
        r'/phpmyadmin',
        r'/adminer',
        r'\.env',
        r'\.sql',
        r'\.sql\.bak',
        r'\.git/',
        r'\.svn/',
        r'/debug\.php',
        r'[?&]debug=',
        r'[?&]test=',
    ],
    "HIGH": [
        r'/api/v[0-9]',
        r'/api/internal',
        r'/api/private',
        r'/graphql',
        r'/webhook',
        r'/callback',
        r'/oauth',
        r'/sso',
        r'/saml',
        r'/integrations',
        r'/third[-_]party',
        r'/staging',
        r'/qa/',
        r'/sandbox',
        r'/actuator',
        r'/metrics',
        r'/health',
        r'/status',
    ],
    "MEDIUM": [
        r'/user/?$',
        r'/users/',
        r'/profile',
        r'/login',
        r'/register',
        r'/signup',
        r'/reset[-_]password',
        r'/forgot[-_]password',
        r'/upload',
        r'/file[-_]upload',
        r'/download',
        r'/export',
        r'/api/',
        r'/management',
        r'/moderator',
        r'[?&]id=',
        r'[?&]user_id=',
        r'[?&]admin=',
    ],
    "LOW": [
        r'[?&]key=',
        r'[?&]token=',
        r'[?&]api_key=',
        r'[?&]redirect=',
        r'[?&]url=',
        r'[?&]return_to=',
        r'[?&]email=',
        r'[?&]username=',
        r'/rest/',
        r'/rpc/',
        r'/dev',
        r'/test',
        r'/.well-known/',
    ]
}


def classify_url(url):
    """
    Classify URL by risk level based on patterns
    """
    url_lower = url.lower()
    
    for risk_level in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        for pattern in PATTERNS[risk_level]:
            if re.search(pattern, url_lower):
                return risk_level, pattern
    
    return None, None


def analyze_url_file(url_file="test_urls.txt"):
    """
    Analyze URLs from a text file (one URL per line)
    """
    results = {
        "CRITICAL": defaultdict(list),
        "HIGH": defaultdict(list),
        "MEDIUM": defaultdict(list),
        "LOW": defaultdict(list),
    }
    
    all_urls = []
    matched_urls = 0
    
    if not os.path.exists(url_file):
        print(f"[-] File not found: {url_file}")
        return results
    
    print(f"[*] Reading URLs from: {url_file}\n")
    
    try:
        with open(url_file, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        print(f"[*] Found {len(lines)} URLs to analyze\n")
        
        for line_num, line in enumerate(lines, 1):
            url = line.strip()
            
            # Skip empty lines and comments
            if not url or url.startswith('#'):
                continue
            
            all_urls.append(url)
            risk_level, pattern = classify_url(url)
            
            if risk_level:
                matched_urls += 1
                results[risk_level][pattern].append({
                    'url': url,
                    'line': line_num,
                    'pattern': pattern
                })
                print(f"[{risk_level:8}] Line {line_num}: {url}")
        
        print(f"\n[+] Analyzed {len(all_urls)} URLs")
        print(f"[+] Matched {matched_urls} URLs with risk patterns\n")
        
    except Exception as e:
        print(f"[-] Error reading file: {e}")
    
    return results


def generate_report(results, output_file="url_analysis_report.txt"):
    """
    Generate a detailed report
    """
    with open(output_file, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("URL PATTERN ANALYSIS REPORT - BUG BOUNTY RECONNAISSANCE\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write("=" * 80 + "\n\n")
        
        # Summary
        total_findings = sum(
            len(urls) for risk_level in results.values() for urls in risk_level.values()
        )
        
        f.write("SUMMARY\n")
        f.write("-" * 80 + "\n")
        for risk_level in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            count = sum(len(urls) for urls in results[risk_level].values())
            f.write(f"{risk_level:10}: {count:5} findings\n")
        f.write(f"{'TOTAL':10}: {total_findings:5} findings\n\n")
        
        # Detailed findings by risk level
        for risk_level in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
            findings = results[risk_level]
            
            if not findings:
                continue
            
            f.write("\n")
            f.write("=" * 80 + "\n")
            f.write(f"{risk_level} RISK - {len(findings)} Pattern Types Found\n")
            f.write("=" * 80 + "\n\n")
            
            for pattern, urls in sorted(findings.items(), key=lambda x: -len(x[1])):
                f.write(f"\nPattern: {pattern}\n")
                f.write(f"Matches: {len(urls)}\n")
                f.write("-" * 80 + "\n")
                
                for item in sorted(urls, key=lambda x: x['url']):
                    f.write(f"  URL: {item['url']}\n")
                    if 'file' in item:
                        f.write(f"  Source File: {item['file']}\n")
                    if 'line' in item:
                        f.write(f"  Line Number: {item['line']}\n")
                    f.write("\n")
        
        # Recommendations
        f.write("\n")
        f.write("=" * 80 + "\n")
        f.write("RECOMMENDATIONS\n")
        f.write("=" * 80 + "\n\n")
        
        critical_count = sum(len(urls) for urls in results["CRITICAL"].values())
        high_count = sum(len(urls) for urls in results["HIGH"].values())
        
        f.write("1. PRIORITIZE TESTING:\n")
        f.write(f"   - Start with {critical_count} CRITICAL patterns\n")
        f.write(f"   - Then test {high_count} HIGH risk patterns\n")
        f.write("   - These are most likely to have vulnerabilities\n\n")
        
        f.write("2. TESTING FOCUS AREAS:\n")
        f.write("   - Admin panels: Check for authentication bypass, default credentials\n")
        f.write("   - APIs: Test for authorization flaws, data exposure\n")
        f.write("   - Debug endpoints: Look for verbose error messages, info disclosure\n")
        f.write("   - File uploads: Test for arbitrary file upload, RCE\n")
        f.write("   - Parameters: Check for SQL injection, XSS, directory traversal\n\n")
        
        f.write("3. METHODOLOGY:\n")
        f.write("   - Use tools like Burp Suite, OWASP ZAP for detailed testing\n")
        f.write("   - Check if endpoints are still accessible (404 vs 200/403/401)\n")
        f.write("   - Test authentication and authorization on protected endpoints\n")
        f.write("   - Look for information disclosure in responses\n")
        f.write("   - Document all findings with proof-of-concept\n\n")


def generate_json_report(results, output_file="url_analysis_report.json"):
    """
    Generate JSON report for programmatic use
    """
    json_data = {
        "timestamp": datetime.now().isoformat(),
        "summary": {
            "CRITICAL": sum(len(urls) for urls in results["CRITICAL"].values()),
            "HIGH": sum(len(urls) for urls in results["HIGH"].values()),
            "MEDIUM": sum(len(urls) for urls in results["MEDIUM"].values()),
            "LOW": sum(len(urls) for urls in results["LOW"].values()),
        },
        "findings": results
    }
    
    # Convert defaultdicts to regular dicts for JSON serialization
    def convert_to_dict(obj):
        if isinstance(obj, defaultdict):
            return {k: convert_to_dict(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [convert_to_dict(item) for item in obj]
        else:
            return obj
    
    json_data["findings"] = convert_to_dict(json_data["findings"])
    
    with open(output_file, 'w') as f:
        json.dump(json_data, f, indent=2)
    
    print(f"[+] JSON report saved: {output_file}")


def main():
    print("[*] URL Pattern Analyzer - Bug Bounty Reconnaissance\n")
    
    # Analyze URLs from file
    results = analyze_url_file("test_urls.txt")
    
    # Check if we found anything
    total_findings = sum(
        len(urls) for risk_level in results.values() for urls in risk_level.values()
    )
    
    if total_findings == 0:
        print("[-] No patterns matched in any URLs")
        return
    
    print(f"[+] Found {total_findings} matching patterns\n")
    
    # Generate reports
    print("[*] Generating reports...")
    generate_report(results, "url_analysis_report.txt")
    generate_json_report(results, "url_analysis_report.json")
    
    print("[+] Report saved: url_analysis_report.txt")
    print("[+] JSON report saved: url_analysis_report.json")
    
    # Print summary to console
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    for risk_level in ["CRITICAL", "HIGH", "MEDIUM", "LOW"]:
        count = sum(len(urls) for urls in results[risk_level].values())
        if count > 0:
            print(f"{risk_level:10}: {count:5} findings")


if __name__ == "__main__":
    main()
