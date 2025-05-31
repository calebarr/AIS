class Database:
    def __init__(self, user,password, host, port, database):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.database = database

    def save_to_postgres(self, data,table_name):
        from sqlalchemy import create_engine

        engine = create_engine(
            f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        )
        data.to_sql(
                name=table_name,
                con=engine,
                if_exists='replace',
                index=False
            )
        engine.dispose()

    def fetch_from_postgres(self, table_name):
        from sqlalchemy import create_engine
        import pandas as pd

        engine = create_engine(
            f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}'
        )
        data = pd.read_sql_table(table_name, con=engine)
        engine.dispose()
        return data
        
