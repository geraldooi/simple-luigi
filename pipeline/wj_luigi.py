import smtplib
import luigi
import pandas as pd
import statistics

from email.message import EmailMessage
from textwrap import dedent
from datetime import datetime, timedelta
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
            f"mysql+pymysql://{self.username}:{self.password}@{self.host}:{self.port}/{self.dbname}"
        )

class email(luigi.Config):
    username = luigi.Parameter()
    password = luigi.Parameter()
    server = luigi.Parameter()
    out_port = luigi.Parameter()

class receivers(luigi.Config):
    email = luigi.Parameter()

class LoadToDB(luigi.Task):
    task_namespace = "wj_luigi"

    csv_file = luigi.Parameter()
    table_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"/home/weijie/luigi/simple-luigi/data/{self.table_name}_load_to_db_{datetime.now().strftime('%Y%m%d')}_{self.table_name}.txt")

    def run(self):
        print('\n\n------------------------------Load Data To Database----------------------')
        print(f'\n\n------------------------------ {self.csv_file}------------------------- ')

        df = pd.read_csv(self.csv_file, index_col=0)

        print(df.head())

        # Create Table in taxi
        df.head(0).to_sql(name=self.table_name,
                          con=mydb().engine, if_exists='replace')

        # Load data into trip table
        df.to_sql(name=self.table_name, con=mydb().engine,
                  if_exists='append', chunksize=100000)

        with self.output().open('w') as f:
            f.write('Done')

        print('-----------------------------Data Insert Into Database---------------------\n\n')


class QueryDBToCSV(luigi.Task):
    task_namespace = "wj_luigi"

    def requires(self):
        return LoadToDB(csv_file="data/trip.csv", table_name="trip"), LoadToDB(csv_file="data/zone.csv", table_name="zone")

    def output(self):
        return luigi.LocalTarget("./data/output.csv")

    def run(self):
        print('\n\n----------------------------Query To Database--------------------------------\n') 
        print('\n\n--------------Upper East Side South -> Upper East Side North-----------------\n') 

        query = '''
                SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, total_amount, trip_distance, TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration
                FROM (
                        SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, total_amount, trip_distance, a.Zone AS PULocationZone, b.Zone AS DOLocationZone
                        FROM `trip`
                        JOIN `zone` a ON trip.PULocationID = a.LocationID
                        JOIN `zone` b ON trip.DOLocationID = b.LocationID
                    ) a
                WHERE PULocationZone = "Upper East Side South" AND DOLocationZone = "Upper East Side North";
                '''

        output_df = pd.read_sql(query, con=mydb().engine)
        print(output_df)
        print('\n\n -------------------- Query Data Done----------------------------')

        with self.output().open('w') as f:
            f.write(output_df.to_csv(index=None))

        return output_df


class EmailResult(luigi.Task):
    task_namespace = "wj_luigi"
    csv_file = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"/home/weijie/luigi/simple-luigi/data/email_result_{datetime.now().strftime('%Y%m%d')}.txt")

    def run(self):
        print('----------------------Send Email---------------------------')        

        df = pd.read_csv(self.csv_file, index_col=0)

        # Email content   
        subject = 'Exercise'
        message = dedent(f'''
        Dear All,

        I found something,

        From "Upper East Side South" -> "Upper East Side North": \n
        Number of rows selected: {df.shape[0]} \n
        Average passenger count per trip: {statistics.mean(df['passenger_count'])} \n
        Average total amount per trip: {statistics.mean(df['total_amount'])} \n
        Average distance per trip: {statistics.mean(df['trip_distance'])} \n
        Average duration per trip (in minutes): {statistics.mean(df['trip_duration'])} \n

        -------------------------------------------
        Summary (First 5 rows order by pickup time)
        -------------------------------------------
        {'|'.join(df.columns.to_list())}
        ''')
        for i in range(5):
            message += '|'.join(map(str, df.values.tolist()[:5][i])) + '\n'

        msg = EmailMessage()
        msg["Date"] = datetime.now() - timedelta(hours=8)
        msg['Subject'] = subject
        msg['From'] = email().username
        msg['To'] = receivers().email
        msg.set_content(dedent(message))

        # attach files
        files = ['data/output.csv']
        for file in files:
            with open(file, 'rb') as f:
                file_data = f.read()
                file_name = f.name
            msg.add_attachment(file_data, maintype='application', subtype='octet-stream', filename=file_name)

        try:
            s = smtplib.SMTP_SSL(email().server, email().out_port)
            s.login(email().username, email().password)
            s.send_message(msg) # send the EmailMessage you have created
        except Exception as e:
            print(f'Exception: {e}')
        finally:
            print(f'Email sent to {receivers().email}')
            s.quit() # close the smtp connection

if __name__ == "__main__":
    luigi.build([LoadToDB(csv_file="data/trip.csv", table_name="trip")],
                local_scheduler=True)
    luigi.build([LoadToDB(csv_file="data/zone.csv", table_name="zone")],
                local_scheduler=True)

    luigi.build([QueryDBToCSV()], local_scheduler=True)

    luigi.build([EmailResult(csv_file="data/output.csv")],local_scheduler=True)
