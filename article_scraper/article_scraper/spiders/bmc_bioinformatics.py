import scrapy, os, json, re
from inline_requests import inline_requests

# Check URL
check_url = lambda x: not any(['supplementary' in x.css('::text').extract_first().lower() if x.css('::text').extract_first() else False, '@' in x.css('::text').extract_first(), x.css('::attr("href")').extract_first() == None])

class JournalSpider(scrapy.Spider):

    # Setup
    name = "bmc_bioinformatics"
    start_urls = ['https://bmcbioinformatics.biomedcentral.com/articles']

    # Parse archive
    def parse(self, response):

        # Get minimum page
        from_page = 1

        # Loop through pages
        for page in list(range(1, 179)):

            # Parse year
            yield scrapy.Request('https://bmcbioinformatics.biomedcentral.com/articles?searchType=journalSearch&sort=PubDate&page='+str(page), callback=self.parse_page)

    # Parse page
    @inline_requests
    def parse_page(self, response):
        # Define articles
        articles = {'article_data': []}

        # Get page
        page = response.url.split('=')[-1]
        print("page:"+str(page))

        # Get base directory
        basedir = os.path.join(os.path.dirname(os.getcwd()), 'results/bmc_bioinformatics/')
        file = 'bmc-bioinformatics_page_'+str(page)+'.json'
        outfile = os.path.join(basedir, file)
  
        # Check if outfile exists
        if not os.path.exists(outfile):
     
            # Get articles
            article_links = ['https://bmcbioinformatics.biomedcentral.com'+x for x in response.css('ol[data-test="results-list"] li [data-test="title-link"]::attr(href)').extract()]

            # Loop through articles
            for i, article_link in enumerate(article_links):
            
                # Parse archive
                article = yield scrapy.Request(article_link)

                # Get data
                articles['article_data'].append({
                    'article_title': ''.join(article.css('.ArticleTitle::text, .ArticleTitle em::text').extract()),
                    'authors': [x.replace(u'\u00a0', ' ') for x in article.css('.u-listReset .AuthorName::text').extract()],
                    'doi': article.css('.ArticleDOI a::text').extract_first(),
                    'abstract': [[div.css('.Heading::text').extract_first().strip(), ''.join(div.css('.Para::text, .Para span::text').extract()).strip()] for div in article.css('.Abstract .AbstractSection')],
                    'date': article.css('[itemprop="datePublished"]::text').extract_first().replace(u'\u00a0', ' '),
                    'links': list(set([a.css('::attr("href")').extract_first() for a in article.css('.Abstract .AbstractSection a') if a.css('::text').extract_first() if check_url(a)]))
                })

            # Save data
            with open(outfile, 'w') as openfile:
                    openfile.write(json.dumps(articles, indent=4))


