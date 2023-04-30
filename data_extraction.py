class DataExtractor:

    import database_utils
    import pandas as pd
    import tabula
    import requests
    import boto3
    import tabula
    import psycopg2



    dbconinstance  = database_utils.DatabaseConnector

    database_utils.DatabaseConnector.db_creds

    engine = dbconinstance.init_db_engine(0) # database_utils.DatabaseConnector.init_db_engine

    apicred = {"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}
    number_of_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"

    s3_address = "s3://data-handling-public/products.csv"

    def list_db_tables():
        """Returns list of tables in database using engine """
        # 1) USING SQLALCHEMY##
        # from sqlalchemy import inspect
        # inspector = inspect(engine)
        # tablenames = inspector.get_table_names()

        # 2) USING pyscopg
        # import psycopg2
        tablenames = []
        with psycopg2.connect(host='data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com',
                            user='aicore_admin',password='AiCore2022', dbname='postgres',
                            port=5432) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT table_name FROM information_schema.tables
                                WHERE table_schema = 'public'""")
                for table in cur.fetchall():
                    tablenames.append(table[0])
        return  tablenames

    # engineinfo = init_db_engine(db_creds)

    # def read_data(engineinfo):
    #     """Read the data from the database"""
    #     import pandas as pd
    #     pd.read_sql("SELECT * from common",con=engineinfo)
    #     pd.read_sql_query

    tablenames = list_db_tables()

### original read_rds_table method ----------------------------------------------------disabled 24/04--

    # def read_rds_table(tablenames = tablenames, engine=engine):
    #     """Extract database table to DF"""
    #     import pandas as pd
    #     # dbconinstance.init_db_engine

    #     legacy_store_details_DF =  pd.read_sql_table(tablenames[0], con=engine)
    #     legacy_users_DF =  pd.read_sql_table(tablenames[1], con=engine, index_col="index")
    #     orders_DF =  pd.read_sql_table(tablenames[2], con=engine)

    #     return legacy_users_DF #,legacy_store_details_DF, orders_DF

## try again and make the program more flexible

    def read_rds_table(tablename, engine=engine):
        """Extract database table to DF"""
        # import pandas as pd
        # dbconinstance.init_db_engine
        table_as_df =  pd.read_sql_table(tablename, con=engine, index_col="index")
        return table_as_df

    def  retrieve_pdf_data(link)-> pd.DataFrame:
        """Gets pdf data from url and produces a DataFrame"""
        # import tabula
        # link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
        list_of_df = tabula.read_pdf(link,pages="all", output_format="dataframe")
        df = list_of_df[0]
        return df

    def list_number_of_stores(endpoint = number_of_stores_endpoint , apicred : dict = apicred.copy()):
        """Retrives number of stores from API call"""
        # import requests

        number_of_stores_endpoint_response = requests.get(endpoint, headers=apicred, timeout=10)

        number_of_stores_json = number_of_stores_endpoint_response.json()

        number_of_stores =  number_of_stores_json["number_stores"]

        return number_of_stores

    # def retrieve_stores_data(endpoint = stores_endpoint, apicred = apicred):

    #     """Retrives information about a particular store number"""

    #     import pandas as pd
    #     import requests

    #     stores_endpoint_response = requests.get(endpoint, headers=apicred)

    #     stores_json = stores_endpoint_response.json()

    #     storedf = pd.DataFrame(stores_json, index=[0])

    #     return storedf

    number_of_stores = list_number_of_stores()

    def retrieve_stores_data(number_of_stores = number_of_stores , apicred : dict = apicred.copy):
        """"Retrieves store data from all listed stores"""
        # import pandas as pd  
        # import requests

        stores_list = []

        for store_number in range(number_of_stores):

            endpoint = f"https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"

            stores_endpoint_response = requests.get(endpoint, headers=apicred, timeout=10)

            stores_json = stores_endpoint_response.json()

            stores_list.append(stores_json)

        storedf = pd.DataFrame(stores_list)

        return storedf

    def extract_from_s3(s3_address = s3_address):
        """Extract date from s3 container"""
        # import pandas as pd
        # import boto3

        s3_addr_list = s3_address.split("/")

        service = s3_addr_list[0][:s3_address.find(":")] # S3 part

        bucket = s3_addr_list[2] # bucket

        filename = s3_addr_list[3] # filename

        downloaded_filename = "awsfile.csv"

        s3 = boto3.client(service)

        s3.download_file(bucket, filename, downloaded_filename)

        awsfile_df = pd.read_csv(downloaded_filename)

        return awsfile_df

    def extract_events(endpoint = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json" ):
        # import pandas as pd
        # import requests

        events_endpoint_response = requests.get(endpoint, timeout=10)

        with open("date_details.json","wb") as f:
            f.write(events_endpoint_response.content)

        storedf = pd.read_json("date_details.json")

        return storedf
