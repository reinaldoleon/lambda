import boto3
import logging

#setup simple logging for INFO
logger = logging.getLogger()
logger.setLevel(logging.INFO)

#define the connection
ec2 = boto3.resource('ec2')

def lambda_handler(event, context):
    # all stop EC2 instances.
    filters = [{
            'Name': 'tag:Start',
            'Values': ['On']
        },
        {
            'Name': 'instance-state-name', 
            'Values': ['stopped']
        }
    ]
 
    #filter the instances
    instances = ec2.instances.filter(Filters=filters)

    #locate all stopped instances
    StopInstances = [instance.id for instance in instances]
    
    #make sure there are actually instances to shut down. 
    if len(StopInstances) > 0:
        #perform the shutdown and save details
        runningInstances = ec2.instances.filter(InstanceIds=StopInstances).start()
        print "MESSAGE: The instance was running. GENERAL DETAILS: " + str(runningInstances) 
    else:
       print "MESSAGE: No instances to start"