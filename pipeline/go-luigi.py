import luigi
import pandas as pd

from datetime import datetime
from sqlalchemy import create_engine


class mydb(lugi.Config):
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
    task_namespace = "go-luigi"

    task_date = luigi.DateParameter(default.datetime.now())
    csv_file = luigi.Parameter()
    table_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"/home/gerald/temp/load_to_db_{datetime.now().strftime('%Y%m%d')}.txt")

    def run(self):
        print('\n\n-----LoadToDB-----')

        # Read CSV file
        df = pd.read_csv(self.csv_file, index_col=0)

        # Create Table in taxi
        df.head(0).to_sql(name=self.table_name,
                          con=mydb().engine, if_exists='replace')

        # Load data into trip tabe
        df.to_sql(name=self.table_name, con=my_db().engine,
                  if_exists='append', chunksize=10000)

        # Write something to self.output()
        with self.output().open('w') as f:
            f.write('Done')

        print('-----LoadToDB-----\n\n')


class QueryDBToCSV(luigi.Task):
    task_namespace = "go-luigi"

    def requires(self):
        # Require 2 table to de load in DB
        # ../data/trip.csv
        # ../data/zone.csv
        pass

    def output(self):
        return luigi.LocalTarget('../output.csv')

    def run(self):
        print('\n\n-----QueryDBToCSV-----')

        # TODO: Query DB

        print('-----QueryDBToCSV-----\n\n')


class EmailResult(luigi.Task):
    task_namespace = "go-luigi"

    def run(self):
        print('\n\n-----EmailResult-----')

        # TODO: Send email

        print('-----EmailResult-----\n\n')


if __name__ == '__main__':
    luigi.build([LoadToDB(csv_file='../data/trip.csv',
                table_name='trip')], local_scheduler=True)
