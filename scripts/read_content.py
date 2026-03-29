#!/usr/bin/env python3
"""
QueryMesh Content Reader

A utility script to read and display crawled content from S3 in human-readable format.

Usage:
    # Read raw content by S3 key
    python read_content.py raw raw/job-id/url-hash

    # Read parsed content by S3 key
    python read_content.py parsed parsed/job-id/url-hash

    # List recent raw content
    python read_content.py list-raw

    # List recent parsed content
    python read_content.py list-parsed

    # Read by URL (searches for it)
    python read_content.py url "https://example.com/page"

Environment:
    S3_ENDPOINT: MinIO/S3 endpoint (default: localhost:9000)
    S3_ACCESS_KEY: Access key (default: minioadmin)
    S3_SECRET_KEY: Secret key (default: minioadmin)
"""

import os
import sys
import json
import base64
import gzip
import hashlib
import argparse
from datetime import datetime
from typing import Optional
from textwrap import wrap, indent

try:
    from minio import Minio
    from minio.error import S3Error
except ImportError:
    print("Error: minio package not installed")
    print("Run: pip install minio")
    sys.exit(1)


# Terminal colors
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    END = '\033[0m'
    DIM = '\033[2m'


def get_client() -> Minio:
    """Create MinIO client from environment variables."""
    endpoint = os.getenv("S3_ENDPOINT", "localhost:9000")
    access_key = os.getenv("S3_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("S3_SECRET_KEY", "minioadmin")
    use_ssl = os.getenv("S3_USE_SSL", "false").lower() == "true"
    
    # Remove http:// or https:// prefix if present
    endpoint = endpoint.replace("http://", "").replace("https://", "")
    
    return Minio(
        endpoint,
        access_key=access_key,
        secret_key=secret_key,
        secure=use_ssl
    )


def print_header(text: str):
    """Print a styled header."""
    print(f"\n{Colors.BOLD}{Colors.CYAN}{'═' * 80}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}  {text}{Colors.END}")
    print(f"{Colors.BOLD}{Colors.CYAN}{'═' * 80}{Colors.END}\n")


def print_section(title: str):
    """Print a section title."""
    print(f"\n{Colors.BOLD}{Colors.YELLOW}▶ {title}{Colors.END}")
    print(f"{Colors.DIM}{'─' * 40}{Colors.END}")


def print_field(name: str, value: str, color: str = Colors.END):
    """Print a labeled field."""
    print(f"  {Colors.BOLD}{name}:{Colors.END} {color}{value}{Colors.END}")


def print_text_block(text: str, max_width: int = 76):
    """Print a text block with wrapping."""
    lines = text.split('\n')
    for line in lines[:50]:  # Limit to first 50 lines
        if len(line) > max_width:
            wrapped = wrap(line, width=max_width)
            for w in wrapped:
                print(f"  {w}")
        else:
            print(f"  {line}")
    if len(lines) > 50:
        print(f"\n  {Colors.DIM}... ({len(lines) - 50} more lines){Colors.END}")


def read_raw_content(client: Minio, key: str):
    """Read and display raw fetched content."""
    bucket = "crawler-raw"
    
    try:
        response = client.get_object(bucket, key)
        data = json.loads(response.read().decode('utf-8'))
        response.close()
        response.release_conn()
    except S3Error as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}")
        return
    
    print_header("RAW FETCHED CONTENT")
    
    # Basic info
    print_section("Request Info")
    print_field("URL", data.get("url", "N/A"), Colors.GREEN)
    print_field("Job ID", data.get("job_id", "N/A"))
    print_field("Status Code", str(data.get("status_code", "N/A")), 
                Colors.GREEN if data.get("status_code") == 200 else Colors.RED)
    print_field("Content Type", data.get("content_type", "N/A"))
    print_field("Attempt", str(data.get("attempt", 1)))
    print_field("Fetched At", data.get("fetch_timestamp_utc", "N/A"))
    print_field("Content Hash", data.get("content_hash_sha256", "N/A")[:32] + "...")
    
    # Headers
    headers = data.get("headers", {})
    if headers:
        print_section("Response Headers")
        for name, values in list(headers.items())[:15]:
            val = values[0] if isinstance(values, list) else values
            if len(val) > 60:
                val = val[:60] + "..."
            print_field(name, val, Colors.DIM)
        if len(headers) > 15:
            print(f"  {Colors.DIM}... ({len(headers) - 15} more headers){Colors.END}")
    
    # Body
    body_b64 = data.get("body_b64", "")
    if body_b64:
        try:
            body_raw = base64.b64decode(body_b64)
            
            # Check for gzip compression
            headers = data.get("headers", {})
            content_encoding = None
            for key, values in headers.items():
                if key.lower() == "content-encoding" and values:
                    content_encoding = values[0].lower().strip()
                    break
            
            # Decompress if needed
            if content_encoding == "gzip":
                try:
                    body = gzip.decompress(body_raw).decode('utf-8', errors='replace')
                    print_section(f"Body Content ({len(body):,} bytes, was gzip compressed)")
                except Exception as e:
                    body = body_raw.decode('utf-8', errors='replace')
                    print_section(f"Body Content ({len(body):,} bytes, gzip decompress failed: {e})")
            else:
                body = body_raw.decode('utf-8', errors='replace')
                print_section(f"Body Content ({len(body):,} bytes)")
            
            print_text_block(body)
        except Exception as e:
            print_section("Body Content")
            print(f"  {Colors.RED}Error decoding body: {e}{Colors.END}")
    
    print(f"\n{Colors.DIM}S3 Key: {bucket}/{key}{Colors.END}\n")


def read_parsed_content(client: Minio, key: str):
    """Read and display parsed content."""
    bucket = "crawler-parsed"
    
    try:
        response = client.get_object(bucket, key)
        data = json.loads(response.read().decode('utf-8'))
        response.close()
        response.release_conn()
    except S3Error as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}")
        return
    
    print_header("PARSED CONTENT")
    
    # Basic info
    print_section("Document Info")
    print_field("URL", data.get("url", "N/A"), Colors.GREEN)
    print_field("Job ID", data.get("job_id", "N/A"))
    print_field("Title", data.get("title", "N/A"), Colors.CYAN)
    print_field("Description", data.get("description", "N/A")[:100] + "..." if data.get("description", "") and len(data.get("description", "")) > 100 else data.get("description", "N/A"))
    print_field("Language", data.get("language", "N/A"))
    print_field("Parsed At", data.get("parse_timestamp_utc", "N/A"))
    print_field("Processing Time", f"{data.get('processing_time_ms', 0)}ms")
    
    # Stats
    print_section("Statistics")
    print_field("Text Length", f"{data.get('text_length', 0):,} chars")
    print_field("Links Found", str(data.get("links_count", 0)))
    print_field("New Links", str(data.get("new_links_count", 0)), Colors.GREEN)
    print_field("Skipped (Dedup)", str(data.get("skipped_count", 0)), Colors.YELLOW)
    
    # Extracted text
    text = data.get("text", "")
    if text:
        print_section(f"Extracted Text (first 2000 chars of {len(text):,})")
        print_text_block(text[:2000])
        if len(text) > 2000:
            print(f"\n  {Colors.DIM}... ({len(text) - 2000:,} more characters){Colors.END}")
    
    # Links
    links = data.get("links", [])
    if links:
        print_section(f"Extracted Links ({len(links)} total)")
        
        # Group by type
        internal = [l for l in links if l.get("type") == "internal"]
        external = [l for l in links if l.get("type") == "external"]
        new_links = [l for l in links if l.get("is_new")]
        
        print(f"\n  {Colors.BOLD}Internal:{Colors.END} {len(internal)}  "
              f"{Colors.BOLD}External:{Colors.END} {len(external)}  "
              f"{Colors.BOLD}New:{Colors.END} {len(new_links)}")
        
        print(f"\n  {Colors.BOLD}Sample Links:{Colors.END}")
        for link in links[:10]:
            status = f"{Colors.GREEN}NEW{Colors.END}" if link.get("is_new") else f"{Colors.DIM}seen{Colors.END}"
            link_type = link.get("type", "unknown")[:3]
            url = link.get("url", "")
            if len(url) > 60:
                url = url[:60] + "..."
            text = link.get("text", "")[:30]
            print(f"    [{status}] [{link_type}] {url}")
            if text:
                print(f"          {Colors.DIM}└─ \"{text}\"{Colors.END}")
        
        if len(links) > 10:
            print(f"\n  {Colors.DIM}... ({len(links) - 10} more links){Colors.END}")
    
    # Metadata
    metadata = data.get("metadata", {})
    if metadata:
        print_section("Metadata")
        for key, value in list(metadata.items())[:10]:
            if len(str(value)) > 50:
                value = str(value)[:50] + "..."
            print_field(key, str(value), Colors.DIM)
    
    print(f"\n{Colors.DIM}S3 Key: {bucket}/{key}{Colors.END}\n")


def list_objects(client: Minio, bucket: str, prefix: str = "", limit: int = 20):
    """List objects in a bucket."""
    try:
        objects = list(client.list_objects(bucket, prefix=prefix, recursive=True))
    except S3Error as e:
        print(f"{Colors.RED}Error: {e}{Colors.END}")
        return
    
    # Sort by last modified (most recent first)
    objects = sorted(objects, key=lambda x: x.last_modified or datetime.min, reverse=True)
    
    print_header(f"OBJECTS IN {bucket.upper()}")
    print(f"  Showing {min(limit, len(objects))} of {len(objects)} objects\n")
    
    for i, obj in enumerate(objects[:limit]):
        size = f"{obj.size:,} bytes" if obj.size else "N/A"
        modified = obj.last_modified.strftime("%Y-%m-%d %H:%M") if obj.last_modified else "N/A"
        print(f"  {Colors.BOLD}{i+1:2}.{Colors.END} {obj.object_name}")
        print(f"      {Colors.DIM}Size: {size}  Modified: {modified}{Colors.END}")
    
    if len(objects) > limit:
        print(f"\n  {Colors.DIM}... ({len(objects) - limit} more objects){Colors.END}")
    
    print()


def find_by_url(client: Minio, url: str, content_type: str = "both"):
    """Find content by URL."""
    # Compute URL hash
    url_hash = hashlib.sha256(url.encode()).hexdigest()
    
    print_header(f"SEARCHING FOR URL")
    print_field("URL", url, Colors.GREEN)
    print_field("URL Hash", url_hash[:32] + "...")
    
    found = False
    
    # Search in raw bucket
    if content_type in ["raw", "both"]:
        try:
            objects = list(client.list_objects("crawler-raw", recursive=True))
            for obj in objects:
                if url_hash in obj.object_name:
                    print(f"\n{Colors.GREEN}Found in crawler-raw:{Colors.END} {obj.object_name}")
                    read_raw_content(client, obj.object_name)
                    found = True
                    break
        except S3Error:
            pass
    
    # Search in parsed bucket
    if content_type in ["parsed", "both"]:
        try:
            objects = list(client.list_objects("crawler-parsed", recursive=True))
            for obj in objects:
                if url_hash in obj.object_name:
                    print(f"\n{Colors.GREEN}Found in crawler-parsed:{Colors.END} {obj.object_name}")
                    read_parsed_content(client, obj.object_name)
                    found = True
                    break
        except S3Error:
            pass
    
    if not found:
        print(f"\n{Colors.YELLOW}No content found for this URL{Colors.END}\n")


def main():
    parser = argparse.ArgumentParser(
        description="QueryMesh Content Reader - View crawled content in human-readable format",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s list-raw                    # List recent raw content
  %(prog)s list-parsed                 # List recent parsed content
  %(prog)s raw raw/job-id/hash         # Read specific raw content
  %(prog)s parsed parsed/job-id/hash   # Read specific parsed content
  %(prog)s url "https://example.com"   # Search by URL
        """
    )
    
    parser.add_argument("command", choices=["raw", "parsed", "list-raw", "list-parsed", "url"],
                        help="Command to execute")
    parser.add_argument("key", nargs="?", help="S3 key or URL to read")
    parser.add_argument("-n", "--limit", type=int, default=20,
                        help="Number of items to list (default: 20)")
    parser.add_argument("--no-color", action="store_true",
                        help="Disable colored output")
    
    args = parser.parse_args()
    
    # Disable colors if requested or not a TTY
    if args.no_color or not sys.stdout.isatty():
        for attr in dir(Colors):
            if not attr.startswith('_'):
                setattr(Colors, attr, '')
    
    client = get_client()
    
    if args.command == "list-raw":
        list_objects(client, "crawler-raw", limit=args.limit)
    elif args.command == "list-parsed":
        list_objects(client, "crawler-parsed", limit=args.limit)
    elif args.command == "raw":
        if not args.key:
            print(f"{Colors.RED}Error: S3 key required{Colors.END}")
            sys.exit(1)
        read_raw_content(client, args.key)
    elif args.command == "parsed":
        if not args.key:
            print(f"{Colors.RED}Error: S3 key required{Colors.END}")
            sys.exit(1)
        read_parsed_content(client, args.key)
    elif args.command == "url":
        if not args.key:
            print(f"{Colors.RED}Error: URL required{Colors.END}")
            sys.exit(1)
        find_by_url(client, args.key)


if __name__ == "__main__":
    main()
