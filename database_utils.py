class DatabaseConnector:
    """Connects to Database"""
    import pandas as pd
    from sqlalchemy import create_engine

    def __init__(self) -> None:
        pass

    def read_db_creds():
        """read credentials and return in dict"""
        import yaml
        with open("db_creds.yaml", "r") as stream:
            data_loaded = yaml.safe_load(stream)
        return  data_loaded
        
    db_creds = read_db_creds()

    def init_db_engine(self,db_creds=db_creds):

        """Initalising database engine using credentials"""
        from sqlalchemy import create_engine
        # import pandas as pd

        DATABASE_TYPE ="postgresql"
        DBAPI = "psycopg2"
        ENDPOINT= db_creds["RDS_HOST"]
        USER = db_creds["RDS_USER"]
        PASSWORD = db_creds["RDS_PASSWORD"]
        PORT = db_creds["RDS_PORT"]
        DATABASE =db_creds["RDS_DATABASE"]

        # get the yaml reading to work


        # DATABASE_TYPE ="postgresql"
        # DBAPI = "psycopg2"
        # ENDPOINT= "aicoredbproject.cozhvbt5fgei.us-east-1.rds.amazonaws.com"
        # USER = "aicore_admin"
        # PASSWORD = "AiCore2022"
        # PORT = 5432
        # DATABASE ="postgres"

        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{ENDPOINT}:{PORT}/{DATABASE}")
        engine.connect()
        return engine

    engineinfo = init_db_engine(0)

    def upload_to_db(dataframe:pd.DataFrame, tablename:str):
        """Upload Pandas DF to local database 'sales_data' """

        from sqlalchemy import create_engine
        DATABASE_TYPE = 'postgresql'
        DBAPI = 'psycopg2'
        HOST = 'localhost'
        USER = 'postgres'
        PASSWORD = 'newPassword'
        DATABASE = 'Sales_Data'
        PORT = 5432
        engine = create_engine(f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}")

        return engine.connect(), dataframe.to_sql(name=tablename,con=engine, if_exists="replace")