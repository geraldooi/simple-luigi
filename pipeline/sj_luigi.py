import os
import smtplib
from datetime import datetime
from email.message import EmailMessage
from textwrap import dedent

import luigi
import pandas as pd
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
    email_from = luigi.Parameter()
    email_password = luigi.Parameter()
    email_server = luigi.Parameter()
    email_out_port = luigi.Parameter()


class LoadToDB(luigi.Task):
    task_namespace = "sj_luigi"

    csv_file = luigi.Parameter()
    table_name = luigi.Parameter()

    def output(self):
        return luigi.LocalTarget(f"/home/seejie/simple-luigi/pipeline/{self.table_name}_load_to_db_{datetime.now().strftime('%Y%m%d')}.txt")

    def run(self):
        print(f"\n\n------------------------------- Load {self.csv_file} to DB -------------------------------")

        df = pd.read_csv(self.csv_file, index_col=0)

        print(df.head())

        # create table in db
        df.head(0).to_sql(name=self.table_name, con=mydb().engine, if_exists="replace")

        # load data into db table
        df.to_sql(name=self.table_name, con=mydb().engine, if_exists="append", chunksize=100000)

        with self.output().open('w') as f:
            f.write('Done')

        print("\n\n------------------------------- Done -------------------------------")


class QueryDBToCSV(luigi.Task):
    task_namespace = "sj_luigi"

    query = luigi.Parameter()

    def requires(self):
        return LoadToDB(csv_file="data/trip.csv", table_name="trip"), LoadToDB(csv_file="data/zone.csv", table_name="zone")

    def output(self):
        return luigi.LocalTarget("./pipeline/output.csv")

    def run(self):
        print("\n\n------------------------------- From Upper East Side North -> Upper East Side South -------------------------------")
        
        output_df = pd.read_sql(self.query, con=mydb().engine)
        print(output_df)

        with self.output().open('w') as f:
            f.write(output_df.to_csv(index=None))

        return output_df


class EmailResult(luigi.Task):
    task_namespace = "sj_luigi"

    subject = luigi.Parameter()
    message = luigi.Parameter()
    receipients = luigi.Parameter()
    attach_files = luigi.Parameter()

    def requires(self):
        return QueryDBToCSV(query='''
                                    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, total_amount, trip_distance, TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration
                                    FROM (
                                        SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, total_amount, trip_distance, a.Zone AS PULocationZone, b.Zone AS DOLocationZone
                                        FROM `trip`
                                        JOIN `zone` a ON trip.PULocationID = a.LocationID
                                        JOIN `zone` b ON trip.DOLocationID = b.LocationID
                                        ) a
                                    WHERE PULocationZone = 'Upper East Side North' AND DOLocationZone = 'Upper East Side South';
                                '''
                            )
    
    def output(self):
        return luigi.LocalTarget(f"/home/seejie/simple-luigi/pipeline/load_to_db_{datetime.now().strftime('%Y%m%d')}_2.txt")

    def run(self):
        print("------------------------------- Sending Email -------------------------------")

        # with self.output().open('w') as f:
        #     f.write('Done')
        
        # creates EmailMessage object
        msg = EmailMessage()
        msg["Subject"] = self.subject
        msg["From"] = email().email_from
        msg["To"] = ", ".join(self.receipients)
        msg.set_content(dedent(self.message))

        # attach files
        files = self.attach_files
        for file in files:
            with open(file, "rb") as f:
                file_data = f.read()
                file_name = os.path.basename("pipeline/output.csv")

            msg.add_attachment(file_data, maintype="application", subtype="octet-stream", filename=file_name)

        # send email
        try:
            s = smtplib.SMTP_SSL(email().email_server, email().email_out_port)
            s.login(email().email_from, email().email_password)
            s.send_message(msg)
        except Exception as e:
            print(f"Exception: {e}")
        finally:
            print(f"Email sent to {self.receipients}")
            s.quit()


if __name__ == "__main__":
    luigi.build([LoadToDB(csv_file="data/trip.csv", table_name="trip")], 
                local_scheduler=True)

    luigi.build([LoadToDB(csv_file="data/zone.csv", table_name="zone")], 
                local_scheduler=True)

    query = '''
                SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, total_amount, trip_distance, TIMESTAMPDIFF(MINUTE, tpep_pickup_datetime, tpep_dropoff_datetime) AS trip_duration
                FROM (
                    SELECT tpep_pickup_datetime, tpep_dropoff_datetime, passenger_count, total_amount, trip_distance, a.Zone AS PULocationZone, b.Zone AS DOLocationZone
                    FROM `trip`
                    JOIN `zone` a ON trip.PULocationID = a.LocationID
                    JOIN `zone` b ON trip.DOLocationID = b.LocationID
                    ) a
                WHERE PULocationZone = 'Upper East Side North' AND DOLocationZone = 'Upper East Side South';
            '''

    luigi.build([QueryDBToCSV(query=query)],
                local_scheduler=True)
    
    output_df = QueryDBToCSV(query).run()

    newline = "\n"
    message = f'''
Dear All,

I found something,

From Upper East Side North -> Upper East Side South:
Number of rows selected: {output_df.shape[0]}
Average passenger count per trip: {round(sum(output_df.passenger_count) / len(output_df.passenger_count), 2)}
Average total amount per trip: {round(sum(output_df.total_amount) / len(output_df.total_amount), 2)}
Average distance per trip: {round(sum(output_df.trip_distance) / len(output_df.trip_distance), 2)}
Average duration per trip (in minutes): {round(sum(output_df.trip_duration) / len(output_df.trip_duration), 2)}

-------------------------------------------
Summary (First 5 rows order by pickup time)
-------------------------------------------
{'|'.join(output_df.columns.to_list())}
{newline.join(
    list('|'.join(map(
        str, output_df.values.tolist()[:5][i])) 
        for i in range(5)))}
'''

    luigi.build([EmailResult(subject="Luigi Exercise", 
                            message=message,
                            receipients=["seejie@postpay.asia"],
                            attach_files=["pipeline/output.csv"]
                            )],
                local_scheduler=True)

    os.remove("pipeline/trip_load_to_db_20220627.txt")
    os.remove("pipeline/zone_load_to_db_20220627.txt")
    os.remove("pipeline/output.csv")


