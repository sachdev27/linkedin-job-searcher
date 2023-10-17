import logging
from linkedin_jobs_scraper import LinkedinScraper
from linkedin_jobs_scraper.events import Events, EventData, EventMetrics
from linkedin_jobs_scraper.query import Query, QueryOptions, QueryFilters
from linkedin_jobs_scraper.filters import RelevanceFilters, TimeFilters, TypeFilters, ExperienceLevelFilters, \
    OnSiteOrRemoteFilters
import ast,json,csv
from summarizer import Summarizer


# Change root logger level (default is WARN)
logging.basicConfig(level=logging.INFO)
# logging.basicConfig(level=logging.DEBUG)


# Fired once for each successfully processed job
def on_data(data: EventData):
    # after_summarize = model(data.description,min_length=100)
    after_summarize = model(data.description)
    full = ''.join(after_summarize)
    job = {
        'job_id': data.job_id,
        'title': data.title,
        'location': data.location,
        'company': data.company,
        'company_link' : data.company_link,
        'apply_link': data.apply_link,
        'date': data.date,
        'insights' : data.insights,
        'full': full
    }

    with open('data.json', 'a+') as json_file:
        json.dump(job, json_file, indent=4)
        json_file.write(',\n')


# Fired once for each page (25 jobs)
def on_metrics(metrics: EventMetrics):
    print('[ON_METRICS]', str(metrics))


def on_error(error):
    print('[ON_ERROR]', error)


def on_end():
    print('[ON_END]')


scraper = LinkedinScraper(
    chrome_executable_path='/Users/<USER>/<FOLDER TO >/chromedriver/chromedriver',  # Custom Chrome executable path (e.g. /foo/bar/bin/chromedriver)
    chrome_options=None,  # Custom Chrome options here
    headless=True,  # Overrides headless mode only if chrome_options is None
    max_workers=1,  # How many threads will be spawned to run queries concurrently (one Chrome driver for each thread)
    slow_mo=0.5,  # Slow down the scraper to avoid 'Too many requests 429' errors (in seconds)
    page_load_timeout=40  # Page load timeout (in seconds)
)

# Add event listeners
scraper.on(Events.DATA, on_data)
scraper.on(Events.ERROR, on_error)
scraper.on(Events.END, on_end)



def make_query(query):
    return Query(
            query=query.get('Job'),  # Query string
            options=QueryOptions(
                locations=ast.literal_eval(query.get('Locations')),  # List of locations
                apply_link=bool(query.get('apply_link')),  # Try to extract apply link (easy applies are skipped). If set to True, scraping is slower because an additional page mus be navigated. Default to False.
                skip_promoted_jobs=bool(query.get('skip_promoted_jobs')),  # Skip promoted jobs. Default to False.
                page_offset=int(query.get('page_offset')),  # How many pages to skip
                limit=int(query.get('limit')),  # Limit the number of jobs to scrape
                filters=QueryFilters(
                    company_jobs_url=query.get('company_id'),  # Filter by companies.
                    # relevance=RelevanceFilters.RECENT,
                    # time=TimeFilters.MONTH,
                    type=query.get('type'),  # Filter by job type.
                    # on_site_or_remote=[OnSiteOrRemoteFilters.REMOTE],
                    experience=ast.literal_eval(query.get('experience'))  # Filter by experience level,
                )
            )
        )


if __name__=="__main__":
    with open('list.csv', 'r') as f:
        dict = csv.DictReader(f)
        model = Summarizer()
        for row in dict:
            query = make_query(row)
            scraper.run(query)

