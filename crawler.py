from urllib.request import urlopen
from urllib.robotparser import RobotFileParser
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode
import os
import json
import time
from frontier import CrawlFrontier
from http_status import classify_status

def allowed_by_robots(url):
    parser = RobotFileParser()

    parsed = urlparse(url)

    robots_url = (
        parsed.scheme
        + "://"
        + parsed.netloc
        + "/robots.txt"
    )

    parser.set_url(robots_url)

    try:
        parser.read()
        return parser.can_fetch("*", url)

    except Exception:
        return False

class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for name, value in attrs:
                if name == "href" and value:
                    self.links.append(value)

def normalize_url(url):
    parsed = urlparse(url)

    scheme = parsed.scheme.lower()
    netloc = parsed.netloc.lower()

    path = parsed.path

    tracking_parameters = {
        "utm_source",
        "utm_medium",
        "utm_campaign",
        "utm_term",
        "utm_content"
    }

    query = [
        (key, value)
        for key, value in parse_qsl(parsed.query)
        if key.lower() not in tracking_parameters
    ]

    clean_query = urlencode(query)

    if path != "/" and path.endswith("/"):
        path = path.rstrip("/")

    return parsed._replace(
        scheme=scheme,
        netloc=netloc,
        path=path,
        query=clean_query,
        fragment=""
    ).geturl()

def is_valid_url(url):
    parsed = urlparse(url)

    if parsed.scheme not in ("http", "https"):
        return False

    if not parsed.netloc:
        return False

    return True

def crawl_policy(url, start_domain):
    if not is_valid_url(url):
        return False

    return True

start_url = "https://example.com"
start_domain = urlparse(start_url).netloc
STATE_FILE = "crawl_state.json"
frontier = CrawlFrontier()

if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r", encoding="utf-8") as file:
        state = json.load(file)

    visited = set(state["visited"])

    frontier.domains = state["frontier"]["domains"]
    frontier.domain_order = state["frontier"]["domain_order"]
    frontier.next_domain_index = state["frontier"]["next_domain_index"]
else:
    frontier.add(start_url)
    visited = set()
page_urls = {}
crawl_errors = {}
MAX_PAGES = 5

while frontier.size() > 0 and len(visited) < MAX_PAGES:
    url = frontier.get_next()

    if url in visited:
        continue
    time.sleep(2)
    print("\nCrawling:", url)

    if not allowed_by_robots(url):
        print("Blocked by robots.txt")
        continue

    try:
        response = urlopen(url, timeout=10)
        status_type = classify_status(response.status)

        print("HTTP Status:", response.status)
        print("Status Type:", status_type)
        content_type = response.headers.get("Content-Type", "")

        print("Content-Type:", content_type)

        if "text/html" not in content_type.lower():
             print("Skipping non-HTML content")
             continue

        page = response.read()

        visited.add(url)
        frontier.mark_crawled(url)

        filename = "pages/page_" + str(len(visited)) + ".html"

        page_urls[filename] = {
            "url": url,
            "content_type": content_type,
            "status": response.status
        }

        with open(filename, "wb") as file:
            file.write(page)

        print("Status:", response.status)
        print("Bytes received:", len(page))
        print("Saved:", filename)

        html = page.decode("utf-8", errors="ignore")

        parser = LinkParser()
        parser.feed(html)

        print("Links found:", len(parser.links))

        for link in parser.links:
            full_url = urljoin(url, link)
            full_url = normalize_url(full_url)

            if not crawl_policy(full_url, start_domain):
                continue
            if full_url not in visited:
                frontier.add(full_url)
        with open(STATE_FILE, "w", encoding="utf-8") as file:
            json.dump(
                {
            "frontier": frontier.get_state(),
            "visited": list(visited)
        },
        file,
        ensure_ascii=False,
        indent=2
    )

    except Exception as error:
        print("Error:", error)
        crawl_errors[url] = str(error)
        frontier.mark_failed(url)

print("\nCrawl finished.")
print("Pages crawled:", len(visited))
with open("page_urls.json", "w", encoding="utf-8") as file:
    json.dump(page_urls, file, ensure_ascii=False, indent=2)

print("Saved: page_urls.json")
with open("crawl_errors.json", "w", encoding="utf-8") as file:
    json.dump(crawl_errors, file, ensure_ascii=False, indent=2)

print("Saved: crawl_errors.json")
