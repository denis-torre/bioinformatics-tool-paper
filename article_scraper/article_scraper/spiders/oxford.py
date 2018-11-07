import scrapy, os, json
from inline_requests import inline_requests

# Check URL
check_url = lambda x: not any(['supplementary' in x.css('::text').extract_first().lower(), '@' in x.css('::text').extract_first(), x.css('::attr("href")').extract_first() == None])

class JournalSpider(scrapy.Spider):

    # Setup
    name = "oxford"
    journals = ['bioinformatics', 'nar', 'database']
    start_urls = ['https://academic.oup.com/'+x+'/issue-archive' for x in journals]

    # Parse archive
    def parse(self, response):

        # Get minimum year
        from_year = 2010

        # Loop through years
        for year_link in response.css('.widget-instance-OUP_Issues_Year_List div a::attr(href)').extract():

            # Minimum year
            if int(year_link.split('/')[-1]) >= from_year:
                
                # Parse year
                yield scrapy.Request('https://academic.oup.com'+year_link, callback=self.parse_year)

    # Parse year
    def parse_year(self, response):

        # Loop through archives
        for i, issue_link in enumerate(response.css('.widget-instance-OUP_Issues_List div a::attr(href)').extract()):

                
            # Parse archive
            yield scrapy.Request('https://academic.oup.com'+issue_link, callback=self.parse_issue)

    # Parse issue
    @inline_requests
    def parse_issue(self, response):

        # Define articles
        articles = {'article_data': []}

        # Split URL
        split_url = response.url.split('/')

        # Get journal name
        journal_name = split_url[3]

        # Get base directory
        basedir = os.path.join(os.path.dirname(os.getcwd()), 'results')
        # If database
        if journal_name == 'database':
            outfile = os.path.join(basedir, journal_name, '_'.join([journal_name, 'vol'+split_url[-1]])+'.json')
        else:
            outfile = os.path.join(basedir, journal_name, '_'.join([journal_name, 'vol'+split_url[-2], 'issue'+split_url[-1]])+'.json')

        # Check if outfile exists
        if not os.path.exists(outfile):

            # Loop through articles
            for i, article_link in enumerate(response.css('.viewArticleLink::attr(href)').extract()):


                # Parse archive
                article = yield scrapy.Request('https://academic.oup.com'+article_link)

                ## Get data ()
                article_title = ''.join(article.css('.wi-article-title::text, .wi-article-title em::text').extract()).strip()
                # Html differs for authors depensin gon the type of page, if they first doesn't work, try the second page type
                authors = article.css('.wi-authors .al-author-name .info-card-name::text').extract()
                if len (authors)<1:
                    authors = article.css('.wi-authors .al-author-name-more .info-card-name::text').extract()
                # check for hidden authors in page
                additional_authors = article.css('.wi-authors .remaining-authors .info-card-name::text').extract()
                if len(additional_authors)>0:
                    authors = list(authors)+list(additional_authors)
                doi = article.css('.ww-citation-primary a::text').extract_first()
                date = article.css('.citation-date::text').extract_first()
                links= list(set([a.css('::attr("href")').extract_first() for a in article.css('.abstract a') if check_url(a)]))
                # This may work for some page types where the abstract is broken down into sub paragraphs...
                # such as motivation, results, supplementatry info, etc.
                # Continue below if page type is different 
                abstract = [[p.css('.title::text').extract_first(), ''.join(p.css(':not(.title)::text').extract()).strip()] for p in article.css('.abstract section')]
                
                # If abstract field returns empty, try other page types depending on journal type
                if journal_name=='bioinformatics':
                    if len(abstract)<1:
                        abstract=[]
                        all_sections = [' '.join(p.css('::text').extract()) for p in article.css('.abstract p')]
                        for p in all_sections:
                            p = p.split(': ', 1)
                            title = p[0]
                            body = p[1]
                            abstract.append([title,body])
                if journal_name=='nar'or journal_name=='database':
                    if len(abstract)<1:
                        abstract= [' '.join(p.css('::text').extract()) for p in article.css('.abstract p')]
                        
     
                # Add data to article dict
                articles['article_data'].append({
                    'article_title': article_title,
                    'authors': authors,
                    'doi': doi,
                    'abstract': abstract,
                    'date': date,
                    'links': links
                })
                
            # # Save data
            with open(outfile, 'w') as openfile:
                openfile.write(json.dumps(articles, indent=4))


