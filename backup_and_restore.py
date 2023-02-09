import boto3
import os
import datetime
import logging

client = boto3.client('rds')
os.environ['AWS_DEFAULT_REGION'] = 'eu-central-1'
db_identifier='vdf-prod-01-rds-handset'
subnet_group_name='vdf-prod-01-subgrp-rds' #must be identefied which subnet group will be placed in
waiter = client.get_waiter('db_snapshot_available') 


#create snapshot from exsiting uipath db

def create_snapshot(db_identifier):
    timestamp = '{:%Y%m%d-%H%M%S}'.format(datetime.datetime.now())
    global snapshot_id
    snapshot_id=f"{db_identifier}-snapshot-{timestamp}"
    response = client.create_db_snapshot(
        DBInstanceIdentifier=db_identifier,
        DBSnapshotIdentifier=snapshot_id,
    )
    return (response,snapshot_id)
create_snapshot(db_identifier)    


def check_snapshot_status_and_restore(snapshot_id):
    response = client.describe_db_snapshots(DBSnapshotIdentifier=snapshot_id,)
    db_snapshots = response['DBSnapshots'][0]
    status = db_snapshots['Status']
    #restore the snapshot in new instance
    if status != 'available':
        logging.info(f"{snapshot_id} is not available yet , waiting")
        try:
          waiter.wait( DBSnapshotIdentifier=snapshot_id, WaiterConfig={'Delay': 200, 'MaxAttempts': 5}
          )
          logging.info(f"status now is :{status}")
          client.restore_db_instance_from_db_snapshot( DBInstanceIdentifier=f"{db_identifier}-restored", DBSnapshotIdentifier=snapshot_id, DBSubnetGroupName=subnet_group_name)
        except Exception as e:
          logging.warning(e)
    else:
      logging.info(f"db restored from snapshot {snapshot_id} ")
        
    return (response,snapshot_id)
check_snapshot_status_and_restore(snapshot_id)    
