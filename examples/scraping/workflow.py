import csv
import os
from collections import defaultdict
from typing import Dict, List, Optional, Sequence, Tuple, TypeVar
from urllib.parse import urljoin, urlparse, urlunparse

import requests
from bs4 import BeautifulSoup

from redun import File, cond, task
from redun.functools import flat_map, flatten
from redun.tools import render_template

redun_namespace = "redun.examples.scraper"

S = TypeVar("S")
T = TypeVar("T")


def make_local_filename(out_path: str, url: str) -> str:
    """
    Choose a local filename for a given URL.
    """
    filename = os.path.join(out_path, url).replace(":", "")

    if filename.endswith("/") or os.path.isdir(filename):
        filename += "index.html"

    elif not filename.endswith(".html"):
        filename = filename + ".html"

    return filename


def get_base_url(url: str) -> str:
    """
    Strip the query and fragment from a URL.
    """
    parts = urlparse(url)
    return urlunparse((parts.scheme, parts.netloc, parts.path, "", "", ""))


def clean_url(url: str, base_url: str) -> str:
    """
    Clean a URL for use in crawling.
    """
    # Make url absolute.
    if not urlparse(url).netloc:
        url = urljoin(base_url, url)

    # Discard query params and fragment.
    return get_base_url(url)


def clean_word(word: str) -> str:
    """
    Clean a word for counting.
    """
    return word.strip(",.:()&-").lower()


@task(limits=["crawl"])
def scrape_page(url: str, out_path: str) -> Optional[File]:
    """
    Copy an HTML page to a local file.
    """
    # We use `limits` above to limit up to 5 downloads at once (see .redun/redun.ini)
    response = requests.head(url)
    if response.ok and "text/html" in response.headers["Content-Type"]:
        # If page is accessible and HTML, download it.
        return File(url).copy_to(File(make_local_filename(out_path, url)))
    else:
        return None


@task()
def get_links(file: File, base_url: str, url_prefix: str) -> List[str]:
    """
    Returns the links within an HTML file.
    """
    soup = BeautifulSoup(file.read(), "html.parser")
    base_url = get_base_url(base_url)

    urls = []
    for link in soup.find_all("a"):
        url = link.get("href")
        url = clean_url(url, base_url)
        if url.startswith(url_prefix):
            # Only keep links that are within the same site (i.e. shared the prefix).
            urls.append(url)
    return urls


@task()
def process_page(file: File, url: str, url_prefix: str, out_path: str, depth: int) -> List[File]:
    """
    Process a page by finding more links.
    """
    links = get_links(file, url, url_prefix)

    # Recursively crawl if desired.
    if depth > 1:
        subfiles = flat_map(
            crawl.partial(url_prefix=url_prefix, out_path=out_path, depth=depth - 1),
            links,
        )
        # We use flatten to concatenate our main file with the subfiles list.
        return flatten([[file], subfiles])
    else:
        return [file]


@task(check_valid="shallow")
def crawl(url: str, url_prefix: str, out_path: str, depth: int) -> List[File]:
    """
    Recursively crawl a website and save each HTML page to a local file.
    """
    # We use `check_valid="shallow"` to make resuming a past execution faster.
    # See task options documentation for more details.

    if depth <= 0:
        return []

    file = scrape_page(url, out_path)

    # If the page scraping is successful, process it.
    # We use cond (a lazy if-statement) since `file` is a lazy expression.
    return cond(file, process_page(file, url, url_prefix, out_path, depth), [])


@task()
def write_csv(out_path: str, columns: List[str], data: List[Sequence]) -> File:
    """
    Write a table to a CSV.
    """
    file = File(out_path)
    with file.open("w") as out:
        writer = csv.writer(out, delimiter="\t")
        writer.writerow(columns)
        for row in data:
            writer.writerow(row)
    return file


@task()
def count_words(files: List[File]) -> List[Tuple[str, int]]:
    """
    Count the word frequency in a list of files and write to the countds to a CSV.
    """
    # Count word frequency across all files.
    word_count = defaultdict(int)

    for file in files:
        soup = BeautifulSoup(file.read(), "html.parser")
        words = soup.text.split()
        for word in words:
            word = clean_word(word)
            if word:
                word_count[word] += 1

    counts = sorted(word_count.items(), key=lambda word_count: word_count[1], reverse=True)
    return counts


@task()
def make_report(
    report_path: str, url: str, files: List[File], word_counts: List[Tuple[str, int]]
) -> File:
    """
    Make an HTML report for the web scraping.
    """
    context = {
        "url": url,
        "files": files,
        "word_counts": word_counts,
    }
    # Since we pass the template as a File, our pipeline is reactive to changes
    # we might make in the template (e.g. changing the report structure).
    return render_template(report_path, File("templates/report.html"), context)


@task()
def main(
    url: str = "https://www.python.org/",
    url_prefix: str = "https://www.python.org",
    out_path: str = "./",
    depth: int = 2,
) -> Dict[str, File]:
    """
    Scrape a website, compute the word frequency, generate an HTML report.
    """
    files = crawl(url, url, os.path.join(out_path, "crawl"), depth)

    word_counts = count_words(files)
    word_counts_file = write_csv(
        os.path.join(out_path, "computed/word_counts.txt"),
        ["word", "count"],
        word_counts,
    )

    report_file = make_report(
        report_path=os.path.join(out_path, "reports/report.html"),
        url=url,
        files=files,
        word_counts=word_counts,
    )

    return {
        "word_counts": word_counts_file,
        "report_file": report_file,
    }
