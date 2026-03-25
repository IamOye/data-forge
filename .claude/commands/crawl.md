Run a full crawl and extraction pipeline on this URL: $ARGUMENTS
- Crawl all sub-pages under the same domain recursively
- Extract all page titles, h1/h2/h3 headings, and resource titles
- Normalize and deduplicate the results
- Save the clean dataset to data/processed/titles.csv and data/processed/titles.db
- Log all activity to logs/crawl.log
- Report a summary of how many pages crawled and titles extracted
