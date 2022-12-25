import csv
from collections import defaultdict
from typing import List

from bs4 import BeautifulSoup

from redun import File, task

redun_namespace = "redun.examples.word_count"


@task()
def download_file(url: str, data_path: str) -> File:
    """
    Download one URL to a local file.
    """
    filename = data_path + "/" + url.replace(":", "") + "/index.html"
    return File(url).copy_to(File(filename))


@task()
def download(urls: List[str], data_path: str) -> List[File]:
    """
    Download (in parallel) a list of URLs to local files.
    """
    return [download_file(url, data_path) for url in urls]


@task()
def extract_text(html_file: File) -> File:
    """
    Extract text from an HTML file.
    """
    soup = BeautifulSoup(html_file.read(), "html.parser")
    text_file = File(html_file.path.replace(".html", ".txt"))
    text_file.write(soup.text)
    return text_file


@task()
def extract_texts(html_files: List[File]) -> List[File]:
    """
    Extract text (in parallel) from a list of HTML files.
    """
    return [extract_text(html_file) for html_file in html_files]


@task()
def count_words(text_files: List[File], data_path: str) -> File:
    """
    Count the word frequencies in a list of text files.
    """
    counts = defaultdict(int)

    for text_file in text_files:
        words = text_file.read().strip().split()
        for word in words:
            counts[word] += 1

    counts = sorted(counts.items(), key=lambda word_count: word_count[1], reverse=True)

    file = File(f"{data_path}/word_counts.tsv")
    with file.open("w") as out:
        writer = csv.writer(out, delimiter="\t")
        writer.writerow(["word", "count"])
        for row in counts:
            writer.writerow(row)
    return file


@task()
def main(urls_file: File = File("urls.txt"), data_path: str = "data") -> File:
    """
    Top-level workflow task.

    Download URLs from `urls_file`, extract words, and count word frequencies.
    """

    urls = urls_file.read().strip().split("\n")
    html_files = download(urls, data_path)
    text_files = extract_texts(html_files)

    # Alternatively, we can do:
    # text_files = map_(extract_text, html_files)

    counts_file = count_words(text_files, data_path)

    return counts_file
