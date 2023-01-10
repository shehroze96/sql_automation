import redshift_connector
import psycopg2
import csv
import os
from time import sleep
import pandas as pd


def create_conn_redshift(*args, **kwargs):
    config = kwargs

    try:
        conn = redshift_connector.connect(
        host = config['host'],
        port = config['port'],
        user = config['user'],
        password = config['password'],
        database = config['database']
        
    )
        return conn
    except Exception as err:
        print(err)
    
def create_conn_postgre(*args, **kwargs):
    config = kwargs

    try:
        conn = psycopg2.connect(
        host = config['host'],
        port = config['port'],
        user = config['user'],
        password = config['password'],
        database = config['database']
        
    )
        return conn
    except Exception as err:
        print(err)

# conn = create_conn(host='prod-tracking.cjk1id3qusgl.eu-west-1.redshift.amazonaws.com', port=5439, user='skhan', password='q%td!#t<KaspW}L82(', database='prod')

# azure
# jdbc:postgresql://barracuda-klipfolio-postgres.postgres.database.azure.com:5432/reporting_prod
# user: skhan@barracuda-klipfolio-postgres
# pw: r6EM!QvrfFR15BXYU-21

# conn = create_conn(host='barracuda-klipfolio-postgres.postgres.database.azure.com', port=5432, user='skhan@barracuda-klipfolio-postgres', password='r6EM!QvrfFR15BXYU-21', database='reporting_prod')
# connection = create_conn(host='prod-tracking.cjk1id3qusgl.eu-west-1.redshift.amazonaws.com', port=5439, user='skhan', password='q%td!#t<KaspW}L82(', database='prod')
# print("testing")
# connection.close()

#print(create_conn_redshift(host='prod-tracking.cjk1id3qusgl.eu-west-1.redshift.amazonaws.com', port=5439, user='skhan', password='q%td!#t<KaspW}L82(', database='prod'))


def schema_name_query(env: str, account_id: str, dog: str) -> str:
    return f"""SELECT nspname
               FROM pg_catalog.pg_namespace
               where nspname like {dog} '%{env}_{account_id}%';"""


def msft_reports_query(schema: str, start_date: str, end_date: str, **kwargs):

    query_without_advisor_id = f"""with basic_aggregations_part as (
    select           
           to_char(day, 'YYYY-MM-01')            as month,
           coalesce(sum(advisor_starts), 0)         as visit_count_sum,
           coalesce(sum(engaged_advisor_starts), 0) as engaged_visit_sum,
           case
               when sum(visits_counted_to_avg) > 0 then sum(avg_visit_time * visits_counted_to_avg) /
                                                        sum(visits_counted_to_avg)
               else 0 end                        as avg_visit_time,
          coalesce(sum(revenue_referred), 0)            as revenue_referred_sum,
          coalesce(sum(revenue_generated), 0)           as revenue_generated_sum,
           case
               when sum(engaged_visit_count) > 0 then sum(clicked_out_visit_count) / sum(engaged_visit_count)
               else 0 end                        as click_through_rate,
       case
           when sum(visit_count) > 0 then sum(engaged_visit_count) / sum(visit_count)
           else 0 end                                as engagement_rate
 
    from  {schema}.basic_aggregations()   -- replace the schema name per each account
    where day >= '{start_date}' and day <= '{end_date}'  
        
    group by month),
     conversion_completion_rate_part as (select (case
                                                     when sum(engaged_visits) > 0
                                                         then sum(completion_rate * engaged_visits) / sum(engaged_visits)
                                                     else 0 end)           as completion_rate,
 
                                                to_char(day, 'YYYY-MM-01') as month
                                         from {schema}.conversion_completion_rate() -- replace the schema name per each account
                                         where day >= '{start_date}' and day <= '{end_date}'
                                             
                                         group by month)
 
    select *
    from basic_aggregations_part
            join conversion_completion_rate_part using (month);"""
    query_with_advisor_id = f"""with basic_aggregations_part as (
    select           
           to_char(day, 'YYYY-MM-01')            as month,
           coalesce(sum(advisor_starts), 0)         as visit_count_sum,
           coalesce(sum(engaged_advisor_starts), 0) as engaged_visit_sum,
           case
               when sum(visits_counted_to_avg) > 0 then sum(avg_visit_time * visits_counted_to_avg) /
                                                        sum(visits_counted_to_avg)
               else 0 end                        as avg_visit_time,
          coalesce(sum(revenue_referred), 0)            as revenue_referred_sum,
          coalesce(sum(revenue_generated), 0)           as revenue_generated_sum,
           case
               when sum(engaged_visit_count) > 0 then sum(clicked_out_visit_count) / sum(engaged_visit_count)
               else 0 end                        as click_through_rate,
       case
           when sum(visit_count) > 0 then sum(engaged_visit_count) / sum(visit_count)
           else 0 end                                as engagement_rate
 
    from  {schema}.basic_aggregations()   -- replace the schema name per each account
    where day >= '{start_date}' and day <= '{end_date}'  and advisor_id = '{kwargs['advisor_id']}'  --uncomment the advisor id for Xcite and US-ST and replace with the appropriate id
        
    group by month),
     conversion_completion_rate_part as (select (case
                                                     when sum(engaged_visits) > 0
                                                         then sum(completion_rate * engaged_visits) / sum(engaged_visits)
                                                     else 0 end)           as completion_rate,
 
                                                to_char(day, 'YYYY-MM-01') as month
                                         from {schema}.conversion_completion_rate() -- replace the schema name per each account
                                         where day >= '{start_date}' and day <= '{end_date}' and advisor_id = '{kwargs['advisor_id']}' --uncomment the advisor id for Xcite and US-ST and replace with the appropriate id
                                             
                                         group by month)
 
    select *
    from basic_aggregations_part
            join conversion_completion_rate_part using (month);"""


    # if advisor_id is empty 
    # return query with advisor_id commented out so it will not be applied
    # in cases where it is provided so advisor_id is valid then return query with advisor uncommented

    # check if we have advisor_id in kwargs
    if 'advisor_id' in kwargs:
        advisor_id = kwargs['advisor_id']
        # check if advisor_id is empty, if it is return error about empty id
        if advisor_id == '':
            return query_without_advisor_id
        # if not empty return the query with the advisor_id inserted from the input
        else:
            return query_with_advisor_id
    # when advisor id has no value or the argument has not been put in
    else:
        return query_with_advisor_id
        


    

def us_st_msft_reports_query():
    return "this is a function to handle us-st queries"



# print(msft_reports_query(schema='SHALALALLALALALALALALLALALAH', start_date= '2021-08-01', end_date= '2021-08-31', advisor_id= '2234'))


# region = None # 0
# country = None # 1
# retailer = None # 2
# account_id = None # 3
# advisor_id = None # 4
# schema = None # 5

def form_queries(filepath, start, end):
    # dict to store filename: query
    queries = {}

    # DONE figure out how to take the values in the rows for (from the second row onwards as the first row is the name of the columns)
    with open(f"./{filepath}", 'r') as file:
        csvreader = csv.reader(file)
        line_count = 0
        

        for row in csvreader:
            if line_count == 0:
                print("column names =" ,row)
                line_count +=1 
            else:
                # print(row)
                retailer = row[2]
                country = row[1]
                file_name = f"{retailer} {country}"
                if 'us-st' in filepath:
                    query_for_account = msft_reports_query(schema='st_us_862_8187de6d26a718dc09217dde3491e4fb', start_date=start, end_date=end, advisor_id= row[4])
                else:
                    query_for_account = msft_reports_query(schema=row[5], start_date=start, end_date=end, advisor_id= row[4])
                if file_name == "Microsft US" and 'us-st' in filepath:
                    pass
                else:
                    print("The file name is", file_name)
                    # print(msft_reports_query(schema=row[5], start_date=start, end_date=end, advisor_id= row[4])) # create a dictionary file_name (key): msft_reports_query (value)
                    # CountryCodeDict["Spain"]= 34
                    queries[file_name] = query_for_account 
                    # queries.update( {file_name : query_for_account} )
                    line_count += 1
    return queries



# next steps
# Run the query
# create a new directory file
# write the results to a file
# return the file to nice place
# set a delay  n   
# TODO https://stackoverflow.com/questions/510348/how-do-i-make-a-time-delay 

# for loop to run as long the length of the dictionary
# how to create a folder https://www.geeksforgeeks.org/create-a-directory-in-python/

def five_second_delay():
    sleep(5)

def fifteen_second_delay():
     for x in range(16):
        print(f"Counting down until next query {x}")
        sleep(1)

def run_query(queries_dict, conn, month, directory):
    # Directory 
    directory = f"{directory}_{month}"

    try:
        os.mkdir(directory)
        os.chdir(directory)
    except OSError as error: 
        print(error)  
    
    # works no \n in the query
    for k, v in queries_dict.items():
        print("\n\nthis is a filename:",k)

        # Open a new CSV file and name it
        outputFile = open(f'{k}.csv', 'w', newline='')
        outputWriter = csv.writer(outputFile)

        # create cursor to execute query
        cursor = conn.cursor()

        # execute the query the value from v is what is needed
        cursor.execute(v)

        # get the first row and treat these at the headers for the document
        headers = [i[0] for i in cursor.description]

        # write these headers to the file
        outputWriter.writerow(headers)
        # write all the rows of the query to the file
        for row in cursor:
            outputWriter.writerow(row)

        outputFile.close()

        # pause the execution here to give us some time
        fifteen_second_delay()

        print("done")

    
    conn.close()


def fetch_credentials(filepath):

    credentials = {}
    with open(f"./{filepath}", 'r') as file:
        csvreader = csv.reader(file)
        line_count = 0
        
        for row in csvreader:
            if line_count == 0:
                #print("column names =" ,row)
                line_count +=1
            else:
                credentials['host'] = row[0].strip()
                credentials['port'] = int(row[1])
                credentials['user'] = row[2].strip()
                credentials['password'] = row[3].strip() 
                credentials['database'] = row[4].strip()
                line_count += 1
    return credentials


# def is_sheet_the_same("test_compare_original.csv", "test_compare_new.csv"):

def file_has_discrepancy(original, new):
    df1  = pd.read_csv(original)
    df2  = pd.read_csv(new)
    discrepancy = None

    # if this condition is true 
    if (~df1.apply(tuple,1).isin(df2.apply(tuple,1))):
        discrepancy = df1[~df1.apply(tuple,1).isin(df2.apply(tuple,1))]
        return discrepancy
    else:
        return 'No discrepancies'

# open the directory
# get a list of csv files in the directory
# read the first csv - use pandas
# check for discrep
# if file_has_discrep is true add the file name to an array
# print the array at the end

def compare_csv_files():
    return None


# conn_barracuda = create_conn_postgre(host='barracuda-klipfolio-postgres.postgres.database.azure.com', 
#                             port=5432, 
#                             user='skhan@barracuda-klipfolio-postgres', 
#                             password='r6EM!QvrfFR15BXYU-21', 
#                             database='reporting_prod')

# conn_tiger_st = create_conn_redshift(host='prod-tracking.cjk1id3qusgl.eu-west-1.redshift.amazonaws.com',
#                             port=5439,
#                             database='prod',
#                             user='skhan',
#                             password='q%td!#t<KaspW}L82(')

# queries_dict = form_queries(filepath='msft_us-st_accounts.csv',start='2022-08-01', end='2022-08-31')

# print(queries_dict)




# run_query(queries_dict, conn_tiger_st, month_year_string)

# creds = fetch_credentials('./credentials/azure_credentials.csv')
# creds_aurora = fetch_credentials('./credentials/aurora_credentials.csv')
# # for k,v in creds.items():
# #     print('Here is the the key:', k)
# #     print('Here is the value:', v)
# #     print('\n \n \n')


# conn_aurora= create_conn_postgre(host=creds_aurora['host'], 
#                             port=creds_aurora['port'], 
#                             user=creds_aurora['user'], 
#                             password=creds_aurora['password'], 
#                             database=creds_aurora['database'])



# queries_dict = form_queries(filepath='msft_us-st_accounts.csv',start='2022-08-01', end='2022-08-31')
# # for k,v in queries_dict.items():
# #     print('Here is the the key:', k)
# #     print('Here is the value:', v)
# #     print('\n \n \n')

# run_query(queries_dict, conn_aurora, "month", "directory")


# print("now we close the connection")
# conn_aurora.close()


# ask which DB to query
# ask for configuration path for credentials
# connect
# ask for month/ directory name


# print(is_sheet_the_same("test_compare_original.csv", "test_compare_new.csv"))
# df1  = pd.read_csv("test_compare_original.csv")
# df2  = pd.read_csv("test_compare_new.csv")
# print(~df1.apply(tuple,1).isin(df2.apply(tuple,1)))
# list_of_csv = os.listdir('./test_compare_files_1')
# list_of_csv2 = os.listdir('./test_compare_files_2')
# for file in list_of_csv:
#     print()