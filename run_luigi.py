import luigi
import pandas as pd

from datetime import datetime
from sqlalchemy import create_engine


class mydb(luigi.Config):
    username = luigi.Parameter()
    password = luigi.Parameter()
    host = luigi.Parameter()
    port = luigi.Parameter()
    dbname = luigi.Parameter()

    @property
    def engine(self):
        return create_engine(
            f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}")


class LoadToDB(luigi.Task):
    task_namespace = "run_luigi"

    task_date = luigi.DateParameter(default=datetime.now())
    csv_file = luigi.Parameter()
    table_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"/home/gerald/temp/load_to_db_{datetime.now().strftime('%Y%m%d')}.txt")

    def run(self):
        print('\n\n--------------------LoadToDB------------')
        print('somethin here')
        print(self.output().path)

        print(self.task_date)
        print(self.csv_file)
        print(self.table_name)

        df = pd.read_csv(self.csv_file, index_col=0)

        print(df.head())

        print(mydb().username)
        print(mydb().password)
        print(mydb().host)
        print(mydb().port)
        print(mydb().dbname)

        # Create Table in taxi
        df.head(0).to_sql(name=self.table_name,
                          con=mydb().engine, if_exists='replace')

        # Load data into trip table
        df.to_sql(name=self.table_name, con=mydb().engine,
                  if_exists='append', chunksize=100000)

        with self.output().open('w') as f:
            f.write('Done')

        print('--------------------LoadToDB------------\n\n')


class QueryDBToCSV(luigi.Task):
    task_namespace = "run_luigi"

    def requires(self):
        return LoadToDB(csv_file="data/trip.csv", table_name="trip")

    def output(self):
        return luigi.LocalTarget("./output.csv")

    def run(self):
        # TODO: Query DB
        print('\n\n\n---------HAHA')


class EmailResult(luigi.Task):
    task_namespace = "run_luigi"

    def run(self):
        # TODO: Send email
        pass


if __name__ == "__main__":
    luigi.build([LoadToDB(csv_file="data/trip.csv", table_name="trip")],
                local_scheduler=True)
    # luigi.build([QueryDBToCSV()], local_scheduler=True)
