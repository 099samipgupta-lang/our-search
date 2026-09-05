from html.parser import HTMLParser
from urllib.parse import urljoin
from canonical import CanonicalExtractor
import json
import os


class TextExtractor(HTMLParser):
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url

        self.text = []
        self.title = ""
        self.headings = []
        self.paragraphs = []
        self.links = []

        self.in_title = False
        self.in_heading = False
        self.in_paragraph = False
        self.in_link = False

        self.current_heading = ""
        self.current_paragraph = ""
        self.current_link_text = ""
        self.current_link_url = ""

        self.ignore = False

    def handle_starttag(self, tag, attrs):
        if tag in ("style", "script"):
            self.ignore = True

        if tag == "title":
            self.in_title = True

        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            self.in_heading = True
            self.current_heading = ""

        if tag == "p":
            self.in_paragraph = True
            self.current_paragraph = ""

        if tag == "a":
            self.in_link = True
            self.current_link_text = ""
            self.current_link_url = ""

            for name, value in attrs:
                if name == "href" and value:
                    self.current_link_url = urljoin(
                        self.base_url, value
                    )

    def handle_endtag(self, tag):
        if tag in ("style", "script"):
            self.ignore = False

        if tag == "title":
            self.in_title = False

        if tag in ("h1", "h2", "h3", "h4", "h5", "h6"):
            if self.current_heading.strip():
                self.headings.append(
                    self.current_heading.strip()
                )

            self.in_heading = False
            self.current_heading = ""

        if tag == "p":
            if self.current_paragraph.strip():
                self.paragraphs.append(
                    self.current_paragraph.strip()
                )

            self.in_paragraph = False
            self.current_paragraph = ""

        if tag == "a":
            if (
                self.current_link_text.strip()
                and self.current_link_url
            ):
                self.links.append({
                    "text": self.current_link_text.strip(),
                    "url": self.current_link_url
                })

            self.in_link = False
            self.current_link_text = ""
            self.current_link_url = ""

    def handle_data(self, data):
        if self.in_title:
            self.title += data
            return

        if self.in_heading:
            self.current_heading += data
            return

        if self.in_paragraph:
            self.current_paragraph += data

        if self.in_link:
            self.current_link_text += data

        if not self.ignore:
            self.text.append(data)


with open(
    "page_urls.json",
    "r",
    encoding="utf-8"
) as file:
    page_urls = json.load(file)


os.makedirs("documents", exist_ok=True)


for filename in sorted(os.listdir("pages")):

    if not filename.endswith(".html"):
        continue

    html_path = os.path.join("pages", filename)

    print("\nProcessing:", filename)

    with open(
        html_path,
        "r",
        encoding="utf-8",
        errors="ignore"
    ) as file:
        html = file.read()

    extractor = TextExtractor(
        page_urls["pages/" + filename]["url"]
    )

    extractor.feed(html)
    canonical_extractor = CanonicalExtractor(
     page_urls["pages/" + filename]["url"]
    )

    canonical_extractor.feed(html)

    canonical_url = canonical_extractor.canonical_url
    clean_text = []

    for item in extractor.text:
        item = " ".join(item.split())

        if item:
            clean_text.append(item)

    page_number = filename.replace(".html", "")

    document_id = (
        "DOC-" +
        page_number.replace("page_", "").zfill(6)
    )

    document = {
        "id": document_id,
        "source_file": filename,
        "url": page_urls["pages/" + filename]["url"],
        "canonical_url": canonical_url,
        "title": extractor.title.strip(),
        "headings": extractor.headings,
        "paragraphs": extractor.paragraphs,
        "links": extractor.links,
        "text": clean_text
    }

    output_path = os.path.join(
        "documents",
        page_number + ".json"
    )

    with open(
        output_path,
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            document,
            file,
            ensure_ascii=False,
            indent=2
        )

    print("Saved:", output_path)


print("\nExtraction complete.")
