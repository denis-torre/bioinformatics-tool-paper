#################################################################
#################################################################
############### Canned Analyses ################
#################################################################
#################################################################
##### Author: Denis Torre
##### Affiliation: Ma'ayan Laboratory,
##### Icahn School of Medicine at Mount Sinai

#############################################
########## 1. Load libraries
#############################################
##### 1. Python modules #####
from ruffus import *
import sys, os, glob, json
import pandas as pd
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sqlalchemy import create_engine, and_, or_
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, MetaData

##### 2. Custom modules #####
# Pipeline running
sys.path.append('article_scraper')
import Pipeline_Updater_Support as P

# #############################################
# ########## 2. General Setup
# #############################################
# ##### 1. Variables #####
# Spiders
spiders = ['oxford', 'bmc_bioinformatics']

##### 2. R Connection #####

#################################################################
#################################################################
############### 1. Computational Tools ##########################
#################################################################
#################################################################

#######################################################
#######################################################
########## S1. Spiders
#######################################################
#######################################################

#############################################
########## 1. Run Spiders
#############################################

def spiderJobs():
	for spider in spiders:
		yield [None,spider]
@files(spiderJobs)
def runSpiders(infile, outfile):

	# # Change directory
	# os.chdir('pipeline/scrapy')
	
	# # Run
	# os.system('scrapy crawl '+outfile)

	# # Move back
	# os.chdir('../..')
	print('skip spiders')
#############################################
########## 2. Extract Tools
#############################################

@follows(mkdir('02-tools'))
@follows(runSpiders)

@transform(glob.glob('results/*/*.json'),
		  regex(r"results/(.*)/(.*).json"),
		  r"02-tools/\2_tools.txt")

def getTools(infile, outfile):
	
	# Initialize dataframe
	tool_dataframe = pd.DataFrame()

	# Get dataframe
	with open(infile) as openfile:

		# Get dataframe
		tool_dataframe = pd.DataFrame(json.loads(openfile.read())['article_data'])[['article_title', 'links', 'doi']]
		
		# Drop no links
		
		tool_dataframe.drop([index for index, rowData in tool_dataframe.iterrows() if len(rowData['links']) == 0], inplace=True)
		
		# Add link column
		tool_dataframe['tool_homepage_url'] = [x[0] for x in tool_dataframe['links']]

		# Drop links columns
		tool_dataframe.drop('links', inplace=True, axis=1)
		
		# Add tool name column
		tool_dataframe['tool_name'] = [x.split(':')[0].replace('"', '') if ':' in x and len(x.split(':')[0]) < 50 else None for x in tool_dataframe['article_title']]

		# Drop rows with no names
		tool_dataframe.drop([index for index, rowData in tool_dataframe.iterrows() if not rowData['tool_name']], inplace=True)

		# Add tool description
		tool_dataframe['tool_description'] = [x.split(':', 1)[-1].strip() for x in tool_dataframe['article_title']]
		tool_dataframe['tool_description'] = [x[0].upper()+x[1:] for x in tool_dataframe['tool_description']]
		
		# Drop article title
		tool_dataframe.drop('article_title', inplace=True, axis=1)
		
	# Check if tool link works
	indices_to_drop = []

	# Loop through indicies
	for index, rowData in tool_dataframe.iterrows():

		# Try to connect
		try:
			# Check URL
			if 'http' in rowData['tool_homepage_url']: #urllib2.urlopen(rowData['tool_homepage_url']).getcode() in (200, 401)
				pass
			else:
				# Append
				indices_to_drop.append(index)
		except:
				# Append
				indices_to_drop.append(index)

	# Drop
	tool_dataframe.drop(indices_to_drop, inplace=True)

	# Write
	tool_dataframe.to_csv(outfile, sep='\t', index=False, encoding='utf-8')

# #############################################
# ########## 3. Extract Articles
# #############################################

@follows(mkdir('03-articles'))
@follows(getTools)

@transform(glob.glob('results/*/*.json'),
		  regex(r"results/(.*)/(.*).json"),
		  add_inputs(r"02-tools/\2_tools.txt"),
		  r"03-articles/\2_articles.txt")

def getArticles(infiles, outfile):

	# Split infiles
	jsonFile, toolFile = infiles

	# Get dataframe
	with open(jsonFile, 'r') as openfile:

		# Get dataframe
		article_dataframe = pd.DataFrame(json.loads(openfile.read())['article_data']).drop('links', axis=1)
		
		# Join authors
		article_dataframe['authors'] = ['; '.join(x) for x in article_dataframe['authors']]
		
		# Fix abstract
		article_dataframe['abstract'] = [json.dumps({'abstract': x}) for x in article_dataframe['abstract']]
		
	# Get tool DOIs
	toolDois = pd.read_table(toolFile)['doi'].tolist()

	# Intersect
	article_dataframe = article_dataframe.set_index('doi').loc[toolDois].reset_index()

	# Get Journal FK dict
	journal_fks = {'bioinformatics': 1, 'database': 2, 'nar': 3, 'bmc-bioinformatics': 4}

	# Add journal fk
	article_dataframe['journal_fk'] = journal_fks[os.path.basename(outfile).split('_')[0]]

	# Write
	article_dataframe.to_csv(outfile, sep='\t', index=False, encoding='utf-8')

#######################################################
#######################################################
########## S2. Prepare Tables
#######################################################
#######################################################

#############################################
########## 1. Tool Table
#############################################
@follows(mkdir('final_results'))
@follows(getTools)

@merge(getTools,
	   'final_results/tool.txt')

def prepareToolTable(infiles, outfile):

	# Get dataframe
	tool_dataframe = pd.concat([pd.read_table(x) for x in infiles])

	# Write to outfile
	tool_dataframe.to_csv(outfile, sep='\t', index=False)

#############################################
########## 2. Article Table
#############################################
@follows(getArticles)
@merge(getArticles,
	   'final_results/article.txt')

def prepareArticleTable(infiles, outfile):

	# Get dataframe
	article_dataframe = pd.concat([pd.read_table(x) for x in infiles])

	# Write to outfile
	article_dataframe.to_csv(outfile, sep='\t', index=False)

# #############################################
# ########## 3. Tool Similarity
# #############################################
@follows(prepareToolTable,prepareArticleTable)

@transform(prepareToolTable,
		   regex(r'final_results/(.*).txt'),
		   add_inputs(prepareArticleTable),
		   r'final_results/related_\1.txt')

def getRelatedTools(infiles, outfile):

	# Split infiles
	toolFile, articleFile = infiles

	# Initialize dataframe
	abstract_dataframe = pd.read_table(articleFile)[['abstract', 'doi']]

	# Fix abstract
	abstract_dataframe['abstract'] = [' '.join([abstract_tuple[1] for abstract_tuple in json.loads(x)['abstract'] if abstract_tuple[0] and abstract_tuple[0].lower() not in [u'contact:', u'availability and implementation', u'supplementary information']]) for x in abstract_dataframe['abstract']]

	# Process abstracts
	processed_abstracts = [P.process_text(x) for x in abstract_dataframe['abstract']]

	# Get similarity and keywords
	article_similarity_dataframe, article_keyword_dataframe = P.extract_text_similarity_and_keywords(processed_abstracts, labels=abstract_dataframe['doi'])

	# Get tool-doi correspondence
	tool_dois = pd.read_table(toolFile).set_index('doi')['tool_name'].to_dict()

	# Rename similarity
	tool_similarity_dataframe = article_similarity_dataframe.loc[tool_dois.keys(), tool_dois.keys()].rename(index=tool_dois, columns=tool_dois)

	# Fill diagonal
	np.fill_diagonal(tool_similarity_dataframe.values, np.nan)

	# Melt tool similarity
	melted_tool_similarity_dataframe = pd.melt(tool_similarity_dataframe.reset_index('doi').rename(columns={'doi': 'source_tool_name'}), id_vars='source_tool_name', var_name='target_tool_name', value_name='similarity').dropna()

	# Remove 0
	melted_tool_similarity_dataframe = melted_tool_similarity_dataframe.loc[[x > 0 for x in melted_tool_similarity_dataframe['similarity']]]
	
	# Get related tools
	related_tool_dataframe = melted_tool_similarity_dataframe.groupby(['source_tool_name'])['target_tool_name','similarity'].apply(lambda x: x.nlargest(5, columns=['similarity'])).reset_index().drop('level_1', axis=1)

	# Get tool keywords
	tool_keyword_dataframe = pd.DataFrame([{'tool_name': tool_dois.get(doi), 'keyword': keyword} for doi, keyword in article_keyword_dataframe.reset_index()[['doi', 'keyword']].as_matrix() ]).dropna()

	# Write
	related_tool_dataframe.to_csv(outfile, sep='\t', index=False)
	tool_keyword_dataframe.to_csv('final_results/tool_keyword.txt', sep='\t', index=False)

# #############################################
# ########## 5. Get article metrics
# #############################################
@follows(getRelatedTools)

@transform(prepareArticleTable,
		   suffix('.txt'),
		   '_metrics.txt')

def getArticleMetrics(infile, outfile):

	# Get DOIs
	dois = list(pd.read_table(infile)['doi'])

	# Initialize metrics list
	metrics = []

	# Get scores
	for i, doi in enumerate(dois):
		print (i+1)
		metrics.append(json.loads(os.popen('python get_article_metrics.py '+str(doi)).read()))

	# Convert to dataframe
	metric_score_dataframe = pd.DataFrame(metrics).set_index('doi')

	# Write
	metric_score_dataframe.to_csv(outfile, sep='\t', index=True)


@originate('test')

def test(outfile):
	print('This is a test')

##################################################
##################################################
########## Run pipeline
##################################################
##################################################
pipeline_run([sys.argv[-1]], multiprocess=2, verbose=1)
print('Done!')
