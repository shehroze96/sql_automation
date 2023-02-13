import helpers as db_helpers

def run():
    aurora_credentials = None
    azure_credentials = None
    db_name = input('Welcome to Zoovu Support\'s MSFT reports generator.\nWhich database would you like to query?\nFor Aurora type au then hit Enter\nFor Azure type az then hit Enter\n')

    if db_name.strip().lower() == 'au':
        print('You have chosen Aurora, fetching the credentials and connecting to DB...')
        aurora_credentials = db_helpers.fetch_credentials('./credentials/aurora_credentials.csv')
        conn_aurora= db_helpers.create_conn_postgre(host=aurora_credentials['host'], 
                            port=aurora_credentials['port'], 
                            user=aurora_credentials['user'], 
                            password=aurora_credentials['password'], 
                            database=aurora_credentials['database'])
        
        start_date = input('Enter the start date in the following format YYYY-MM-DD: \n')
        end_date = input('Enter the end date in the following format YYYY-MM-DD: \n')
        file_name = input('Enter the file name that contains schema and assistant details (for example "msft_tiger_accounts.csv or msft_us-st_accounts.csv"): \n')
        server_name  = input('Enter the server (ST, ORCA, TIGER, BARRACUDA ETC.) and the year: ')
        month_as_string = input('Enter the month you\'re querying as a string\n')


        db_helpers.five_second_delay()
        queries_dict = db_helpers.form_queries(filepath=file_name, start=start_date, end=end_date)
        db_helpers.run_query(queries_dict, conn_aurora, month_as_string, server_name)

        print('now we close the connection')
        conn_aurora.close()
 
    elif db_name.strip().lower() == 'az':
        print('You have chosen Azure, fetching the credentials and connecting to DB...')
        azure_credentials = db_helpers.fetch_credentials('./credentials/azure_credentials.csv')
        conn_azure= db_helpers.create_conn_postgre(host=azure_credentials['host'], 
                            port=azure_credentials['port'], 
                            user=azure_credentials['user'], 
                            password=azure_credentials['password'], 
                            database=azure_credentials['database'])
        db_helpers.five_second_delay()
        start_date = input('Enter the start date in the following format YYYY-MM-DD:\n')
        end_date = input('Enter the start date in the following format YYYY-MM-DD:\n')
        file_name = input('Enter the file name that contains schema and assistant details (for example "msft_barracuda_accounts.csv"):\n')
        server_name  = input('Enter the server (ST, ORCA, TIGER, BARRACUDA ETC.) and the year:\n')
        month_as_string = input('Enter the month you\'re querying as a string\n')

        queries_dict = db_helpers.form_queries(filepath=file_name, start=start_date, end=end_date)
        db_helpers.run_query(queries_dict, conn_azure, month_as_string, server_name)

        print('now we close the connection')
        conn_azure.close()
    else:
        print('You have entered an invalid db, please choose either Aurora or Azure')


print('Time to run the program')
run()
