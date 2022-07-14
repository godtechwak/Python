import boto3
import math
import sys
sys.path.append('./config')
import config
from datetime import datetime, timedelta
import time
import warnings
warnings.filterwarnings('ignore')

client_rds=boto3.client('rds', config.CON_INFO['region'])
client_log=boto3.client('logs', config.CON_INFO['region'])

def get_failover_date():
        global date_time

        # Check the events before and after 2 minutes
        events=client_rds.describe_events(StartTime=datetime.now()+timedelta(minutes=-2), EndTime=datetime.now()+timedelta(minutes=2))

        # It contains the time before and after 2 minutes of the failover action.
        date_time=[(categories['Date']+timedelta(minutes=-2), categories['Date']+timedelta(minutes=2)) for categories in events['Events'] for backup in categories['EventCategories'] if backup=='failover']

        return date_time

def get_db_instance_n_log_event(befter_datetime):
        global db_instance_writer_id

        a_datetime=befter_datetime[0][0]
        b_datetime=befter_datetime[1][1]

        db_cluster=client_rds.describe_db_clusters()

        # Contains a write instance
        db_instance_writer_id=[db_writer['DBInstanceIdentifier'] for db_member in db_cluster['DBClusters'] for db_writer in db_member['DBClusterMembers'] if db_writer['IsClusterWriter'] == 1 and config.CON_INFO['db_instance_id'] in db_writer['DBInstanceIdentifier']]

        response=client_log.get_log_events(
            logGroupName=config.CON_INFO['loggroup_name'],
            logStreamName=db_instance_writer_id[0],
            startTime=(math.trunc(time.mktime(a_datetime.timetuple()))*1000),
            endTime=(math.trunc(time.mktime(b_datetime.timetuple()))*1000)
        )

        with open("failover_{}.txt".format(datetime.strftime(datetime.now(),'%Y-%m-%d-%H-%M-%S')), "w") as f:
                [f.write(events['message']+'\n') for events in response['events']]


if __name__ == "__main__":
        date_time=datetime.now()
        db_instance_writer_id=[]

        get_failover_date()

        try:
            get_db_instance_n_log_event(date_time)
        except IndexError:
            print('Not Exists Failover Event')
