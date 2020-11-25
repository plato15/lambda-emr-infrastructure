from __future__ import print_function
import boto3
import os
import logging as log
from config import emr_properties
from datetime import datetime
import tarfile
import logging

client = boto3.client('emr')



def run_emr_creation(task_node_market): 
    def setup_logging(default_level=logging.WARNING):
        """
        Setup logging configuration
        """
        logging.basicConfig(level=default_level)
        return logging.getLogger('DeployPySparkScriptOnAws')

    class DeployPySparkScriptOnAws(object):
        """
        Programmatically deploy a local PySpark script on an AWS cluster
        """
        def __init__(self):

            self.app_name = os.environ['emr_app_name'] # Application name
            self.mastersg_id = os.environ['mastersg_id']
            self.slavesg_id = os.environ['slavesg_id']
            self.servicesg_id = os.environ['servicesg_id']
            self.subnet_id = os.environ['subnet_id']
            self.emr_releaselabel=os.environ['emr_version']
            self.tag_application = os.environ['tag_application']
            self.jobflow_role= os.environ['jobflow_role']
            self.service_role= os.environ['service_role']
            #self.ec2_key_name = os.environ['emr_keypair']                   # Key name to use for cluster
            self.job_flow_id = None                             # Returned by AWS in start_spark_cluster()
            self.job_name = None                                # Filled by generate_job_name()
            self.s3_bucket_logs = os.environ['emr_log_bucket']   # S3 Bucket to store AWS EMR logs
            #def read_emr_property_files(file):
            #    f = open(configuration_path + file,"r")
            #    return (f.read())
            self.emr_instancegroups=emr_properties.emr_instancegroups()
            self.emr_applications=emr_properties.emr_applications()
            self.emr_configurations=emr_properties.emr_configurations()
            self.emr_bootstrapactions=emr_properties.emr_bootstrapactions()
            self.emr_steps=emr_properties.emr_steps()
            self.emr_tags=emr_properties.emr_tags()
            self.job_name = os.environ["job_name"] #"{}.{}.{}".format(self.app_name, self.user, datetime.now().strftime("%Y%m%d.%H%M%S.%f"))

        def run(self):
            session = boto3.Session(region_name="us-east-1") #(profile_name='thom')        # Select AWS IAM profile
            s3 = session.resource('s3')
            #self.tar_python_script()
            #self.upload_temp_files(s3)
            #print(self.emr_instancegroups)
            self.start_cluster_submit_steps()

        def start_cluster_submit_steps(self):

            #for record in event['Records']:
            #bucket = "s3://%s" % record['s3']['bucket']['name']
            #key = record['s3']['object']['key']
            #file_name="sample" #key.split('/')[1]
            #file_uploaded_to_s3_timestamp=record['eventTime']
            #database = os.environ['database_name'] + "." + os.environ['table_name']
            #log_uri = "%s/emr-logs/" % bucket

            #if "ToProcess/" in key and ".csv" in key:
            print("Starting EMR cluster with Processing Steps")
            sparkEmrSteps = self.emr_steps
            # Submit and execute EMR Step
            response = client.run_job_flow(
                Name= self.tag_application,
                LogUri="s3://{}/elasticmapreduce/".format(self.s3_bucket_logs),
                ReleaseLabel=self.emr_releaselabel, #'emr-5.30.0',
                Instances={
                    'InstanceGroups': self.emr_instancegroups,
                    'EmrManagedMasterSecurityGroup': self.mastersg_id,
                    'EmrManagedSlaveSecurityGroup': self.slavesg_id,
                    'ServiceAccessSecurityGroup': self.servicesg_id,
                    'Ec2SubnetId': self.subnet_id,
                    'KeepJobFlowAliveWhenNoSteps': False,
                    'TerminationProtected': False
                    #'Ec2KeyName': self.ec2_key_name

                },
                Applications= self.emr_applications,
                Configurations=self.emr_configurations,
                BootstrapActions= self.emr_bootstrapactions,
                Steps=self.emr_steps,
                Tags=self.emr_tags,
                VisibleToAllUsers=True,
                JobFlowRole=self.jobflow_role,
                ServiceRole=self.service_role,
                AutoScalingRole='EMR_AutoScaling_DefaultRole'
            )
            log.info("EMR Launch response: %s" % response)
    logger = setup_logging()
    DeployPySparkScriptOnAws().run()
