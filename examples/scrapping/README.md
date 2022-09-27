# Let's go scrapping!

## Setup

This example requires a few additional libraries. You can install them using `pip`:

```sh
pip install -r requirements.txt
```

## Running the example

```sh
redun run workflow.py main
```

By default this will scrape web pages from https://www.python.org/ with a depth of 2 link traversals. All of the HTML files encountered will be stored in `crawl/`. Word frequency across all pages will be calculated and a CSV of the word counts will be stored in `computed/word_counts.txt`.

Lastly, an HTML report is generated in `reports/report.html` that summarizes the scrapping and analysis. The report is generated using a jinja2 template stored in `templates/report.html`.

## Exercises for the reader

Feel free to try other urls and depth of scrapping using the task arguments:

```sh
redun run workflow.py main --url URL --depth DEPTH
```

Also feel free to alter the report template `templates/report.html`. It is passed to the task `make_report()` as a `File` argument, so you should have automatic reactivity to changes in the template when rerunning the workflow.
