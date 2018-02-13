## Insight_dec version 1.2 ##
# Version Notes: This program will import a text file
# line by line and store it as a pandas dataframe. The
# program will also prune off uneccesary features, 
# preserving CMTE_ID, NAME, TRANSACTION_DT,
# TRANSACTION_AMT, OTHER_ID.
# 
# 1.3 - uses a for loop to try and create dataframes data
##

#### Libraries
# Standard libraries
import math
import sys

# Third-party libraries
import pandas as pd

# Pandas display settings
pd.set_option('display.height', 1000)
pd.set_option('display.max_rows', 500)
pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

### Global Declerations
# The file path to the input
global itcont_path
itcont_path = sys.argv[1]

# The file path to the output
global repeat_donors_path
repeat_donors_path = sys.argv[2]

# The file path to the percentage
global percentage_path
percentage_path = sys.argv[3]

# The Percentile to evaluate
global percentile
percentile = 100

# The column labels for the raw dataframe
cont_feature_string = "CMTE_ID,AMNDT_IND,RPT_TP,TRANSACTION_PGI,IMAGE_NUM,TRANSACTION_TP,ENTITY_TP,NAME,CITY,STATE,ZIP_CODE,EMPLOYER,OCCUPATION,TRANSACTION_DT,TRANSACTION_AMT,OTHER_ID,TRAN_ID,FILE_NUM,MEMO_CD,MEMO_TEXT,SUB_ID"
global cont_features 
cont_features = cont_feature_string.split(",")

pruned_cont_feature_string = "AMNDT_IND,RPT_TP,TRANSACTION_PGI,IMAGE_NUM,TRANSACTION_TP,ENTITY_TP,CITY,STATE,EMPLOYER,OCCUPATION,TRAN_ID,FILE_NUM,MEMO_CD,MEMO_TEXT,SUB_ID"
global pruned_cont_features
pruned_cont_features = pruned_cont_feature_string.split(",")

# Global dataframe holding valid contributions "cont_cache"
global cont_cache
cont_cache = pd.DataFrame()

# Global dataframe holding repeat donor metrics
global repeat_donor_cache
repeat_donor_cache = pd.DataFrame()

## open file function ##
# This function opens the itcont.txt file and
# processes the contribution data in a streaming fashion.
##

def open_file():

	global cont_cache, repeat_donor_cache, percentile

	# Extract and set percentile - default is 100
	with open(percentage_path) as f:
		for line in f: percentile = line.rstrip("\n")

	# begin streaming in contribution data
	with open(itcont_path) as f:
		for line in f:
			cont_list = txt_to_list(line)
			if len(cont_list) != 21:
				continue
			raw_cont_df = list_to_df(cont_list)
			cont_df = prune(raw_cont_df)
			cont_df['TRANSACTION_AMT'] = cont_df['TRANSACTION_AMT'].apply(pd.to_numeric)
			# Check input for malformed data
			if check(cont_df) == True:
				pass

			elif check(cont_df) == False:

				# Add a repeat flag and check for repeat contribution
				if check_repeat(cont_df.loc[0,'NAME']) == True:

					# Build unique repeat contribution ID
					repeat_CMTE = cont_df.loc[0,'CMTE_ID']
					repeat_DATE = cont_df.loc[0,'TRANSACTION_DT']
					repeat_YEAR = repeat_DATE[4:8]
					repeat_ZIP = cont_df.loc[0,'ZIP_CODE']
					repeat_donor_ID = repeat_CMTE+repeat_YEAR+repeat_ZIP
					repeat_ID_df = pd.DataFrame(data=[repeat_donor_ID],columns=['REPEAT_ID'])

					# Construct temporary repeat df to be joined to the internal database
					repeat_temp_df = cont_df.join(repeat_ID_df)
					repeat_temp_df = repeat_temp_df.drop(['NAME','OTHER_ID'],axis=1)
					
					repeat_donor_cache = repeat_donor_cache.append(repeat_temp_df,ignore_index=True)
					repeat_donor_cache.reset_index()

					# Construct temporary repeat df to be joined to the internal database
					repeat_df = pd.DataFrame(data=[True],columns=['REPEAT'])
					cont_df = cont_df.join(repeat_df)

					update_cache()

				elif check_repeat(cont_df.loc[0,'NAME']) == False:
					repeat_df = pd.DataFrame(data=[False],columns=['REPEAT'])
					cont_df = cont_df.join(repeat_df)

				# Join entry to internal databases
				cont_cache = cont_cache.append(cont_df,ignore_index=True)
				cont_cache.reset_index()

				# Update Repeat Donor Metrics
				

## txt_to_list function ##
# Converts open string from a "|" delimitted
# string to a python list.
##

def txt_to_list(txt_string):
	txt_list = txt_string.rstrip("\n").split("|")
	return(txt_list)


## txt_to_df function ##
# Converts open string from a txt file to
# a pandas dataframe
##

def list_to_df(cont_list):

	raw_import_df = pd.DataFrame(columns=cont_features)
	raw_import_df.loc[0] = cont_list

	return(raw_import_df)

## prune function ##
# This function removes unecessary features from
# the raw_cont_df.
# 
# Keeps only first 5 digits of zip code
##

def prune(raw_cont_df):
	
	cont_df = raw_cont_df.drop(pruned_cont_features, axis=1)
	zip_str = cont_df.loc[0,'ZIP_CODE']
	cont_df.loc[0,'ZIP_CODE'] = zip_str[0:5]
	
	return(cont_df)

## ##
# Now we need a function which can start checking the data before adding it
# to our internal database.
# 1. Check for broken entries.
# 	If TRANSACTION_DT is an invalid date (e.g., empty, malformed)
# 	If ZIP_CODE is an invalid zip code (i.e., empty, fewer than five digits)
# 	If the NAME is an invalid name (e.g., empty, malformed)
# 	If any lines in the input file contains empty cells in the CMTE_ID or TRANSACTION_AMT fields
## 

def check(cont_df):

	df_invalid = False

	TRANSACTION_DT = cont_df.loc[0,'TRANSACTION_DT']
	ZIP_CODE = cont_df.loc[0,'ZIP_CODE']
	NAME = cont_df.loc[0,'NAME']
	CMTE_ID = cont_df.loc[0,'CMTE_ID']
	TRANSACTION_AMT = cont_df.loc[0,'TRANSACTION_AMT']
	OTHER_ID = cont_df.loc[0,'OTHER_ID']

	# Check if TRANSACTION_DT is empty
	# Check if TRANSACTION_DT is malformed
	if (TRANSACTION_DT == '' or
	 TRANSACTION_DT.isdigit() == False or
	 len(TRANSACTION_DT) != 8 or
	 int(TRANSACTION_DT[0:2]) > 12 or
	 int(TRANSACTION_DT[2:4]) > 31 or
	 int(TRANSACTION_DT[4:8]) < 1800):
		df_invalid = True

	# Check if ZIP_CODE is empty
	elif cont_df.loc[0,'ZIP_CODE'] == '':
		df_invalid = True

	# Check if ZIP_CODE is > or < 5 characters
	elif len(cont_df.loc[0,'ZIP_CODE']) != 5:
		df_invalid = True

	# Check if NAME is empty
	# Check if NAME is malformed - is varchar2(200) names should not contain numbers
	elif (cont_df.loc[0,'NAME'] == '' or len(cont_df.loc[0,'NAME']) > 200 or 
	any(char.isdigit() for char in cont_df.loc[0,'NAME']) == True):
		df_invalid = True

	# Check if CMTE_ID is empty
	elif cont_df.loc[0,'CMTE_ID'] == '':
		df_invalid = True

	# Check if TRANSACTION_AMT is empty
	elif cont_df.loc[0,'TRANSACTION_AMT'] == '':
		df_invalid = True

	# Check if contribution is from an individual
	elif OTHER_ID != '':
		df_invalid = True

	return(df_invalid)

## update_cache Function ##
# This function scans through the "repeat_df" and
# updates the repeat_donor_df metrics by consolidating
# zip codes, years, and campaigns.
##

def update_cache():

	export_cache = pd.DataFrame()
	ID_LIST = []
	CONT_LIST = []
	
	temp_df = repeat_donor_cache.sort_values('REPEAT_ID')
	temp_df.reset_index(drop=True, inplace=True)

	for i in xrange(0,len(temp_df.index)):

		unique_ID = temp_df.loc[i,'REPEAT_ID']

		if unique_ID in ID_LIST:
			continue
		else:
			ID_LIST.append(unique_ID) #Keeps track of ID's we have checked
			CONT_LIST.append(temp_df.loc[temp_df['REPEAT_ID'] == unique_ID]) #pull out all contributions correspondng to the unique id
			
	#for i in CONT_LIST: print i

	# Build a new DF with export metrics
	for i in xrange(0,len(CONT_LIST)):
		temp_export_df = CONT_LIST[i] # store dataframe
		temp_export_df.reset_index(drop=True, inplace=True)
		temp_export_df.reset_index()
		export_features_df = pd.DataFrame(columns = ['YEAR','PERCENT','TOTAL','NUMBER'])
		temp_export_df = temp_export_df.join(export_features_df)
		DATE = temp_export_df.loc[0,'TRANSACTION_DT']

		for j in xrange(0,len(temp_export_df.index)):
			temp_NUMBER = j+1
			temp_TOTAL = temp_export_df.loc[0:j,'TRANSACTION_AMT'].sum()

			temp_export_df.set_value(j, 'TOTAL', temp_TOTAL)
			temp_export_df.set_value(j, 'NUMBER', temp_NUMBER)
			temp_export_df.set_value(j, 'YEAR', DATE[4:8])

		# drop date and add year
		temp_export_df = temp_export_df.drop(['TRANSACTION_DT','REPEAT_ID','TRANSACTION_AMT'],axis=1)
		rank_percentile = calculate_percentile(temp_export_df)
		temp_export_df['PERCENT'] = rank_percentile

		# Store modified output
		CONT_LIST[i] = temp_export_df

	# Stitch lists together for export
	if len(CONT_LIST) > 0:
		for i in xrange(0,len(CONT_LIST)):

			export_cache = export_cache.append(CONT_LIST[i],ignore_index=True)
			export_cache.reset_index()
		
	export(export_cache)

## check_repeat function ##
# This function checks if the donor is in the cache 
# before adding the user. If the donors name is found,
# the repeat flag is set to True.
##

def check_repeat(NAME):

	rep_donor = False

	if NAME in cont_cache.values:
		rep_donor = True

	return(rep_donor)

## export function ##
# Writes processed repeat metrics
# to a "|" delimitted txt file
##
def export(repeat_df):
	repeat_df.to_csv(repeat_donors_path, header=None, index=None, sep='|', mode='w')

## calculate_percentile function ##
# This function excepts a dataframe and
# callculates the rank percentile. It returns this 
# to be placed in the dataframe.
##

def calculate_percentile(repeat_df):
	repeat_df = repeat_df.sort_values('TOTAL')
	repeat_df.reset_index(drop=True, inplace=True)

	NUMBER = len(repeat_df.index)
	ORDINAL = int(math.ceil((float(percentile)/100)*NUMBER))
	rank_value = repeat_df.loc[ORDINAL-1,'TOTAL']

	return(rank_value)


## Execution Script
open_file()
#print repeat_donor_cache