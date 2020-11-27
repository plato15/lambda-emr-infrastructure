
import os
def emr_applications():
    val=[
                        {
                            'Name': 'Hue'
                        },
                        {
                            'Name': 'JupyterHub'
                        },
                        {
                            'Name': 'Hive'
                        },
                        {
                            'Name': 'Pig'
                        },
                        {
                            'Name': 'Spark'
                        }
    ]
    return val

def emr_bootstrapactions():
    val= [
                {
                    'Name': 'setup',
                    'ScriptBootstrapAction': {
                        'Path': os.environ['emr_bootstrap_key1'],
                        'Args': [
                            os.environ['emr_bootstrap_arg_key1']
                        ]
                    }
                }
                #{
               #     'Name': 'idle timeout',
               #     'ScriptBootstrapAction': {
               #         'Path': os.environ['emr_bootstrap_key2'],
               #         #'Path':'s3n://{}/{}/terminate_idle_cluster.sh'.format(os.environ['script_bucket'], os.environ['job_name']),
               #         'Args': ['3600', '300']
               #     }
               # },
            ]
    return val

def emr_instancegroups(task_node_market):
        val= [
                        {
                            'Name': 'Master - 1',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'MASTER',
                            #'CustomAmiId'='ami-01d025118d8e760db',
                            'InstanceType': os.environ['instance_size'],
                            'InstanceCount': 1
                        },
                        {
                            'Name': 'Core - 1',
                            'Market': 'ON_DEMAND',
                            'InstanceRole': 'CORE',
                            #'CustomAmiId'='ami-01d025118d8e760db',
                            'InstanceType': os.environ['instance_size'],
                            'InstanceCount': 1,
                            'EbsConfiguration': {
                                'EbsBlockDeviceConfigs': [
                                    {
                                        'VolumeSpecification': {
                                            'VolumeType': 'standard',
                                            'SizeInGB': 200
                                        },
                                        'VolumesPerInstance': 1
                                    }
                                ],
                                'EbsOptimized': True
                            }
                            
                        },
                        {
                            'Name': 'Task - 2',
                            'Market': task_node_market,
                            'InstanceRole': 'TASK',
                            #'CustomAmiId'='ami-01d025118d8e760db',
                            'InstanceType': os.environ['instance_size'],
                            'InstanceCount': int(os.environ['instance_count']),
                            "AutoScalingPolicy":
                               {
                                "Constraints":
                                 {
                                  "MinCapacity": 15,
                                  "MaxCapacity": 45
                                 },
                                "Rules":
                                [
                                 {
                                  "Name": "Default-scale-out",
                                  "Description": "Scales out when YARNMemoryAvailablePercentage is below 15%",
                                  "Action":{
                                   "SimpleScalingPolicyConfiguration":{
                                     "AdjustmentType": "CHANGE_IN_CAPACITY",
                                     "ScalingAdjustment": 1,
                                     "CoolDown": 300
                                   }
                                  },
                                  "Trigger":{
                                   "CloudWatchAlarmDefinition":{
                                     "ComparisonOperator": "LESS_THAN",
                                     "EvaluationPeriods": 1,
                                     "MetricName": "YARNMemoryAvailablePercentage", 
                                     "Namespace": "AWS/ElasticMapReduce",
                                     "Period": 300,
                                     "Threshold": 15,
                                     "Statistic": "AVERAGE",
                                     "Unit": "PERCENT",
                                     "Dimensions":[
                                        {
                                          "Key" : "JobFlowId",
                                          "Value" : "${emr.clusterId}"
                                        }
                                     ]
                                   }
                                  }
                                 },
                                 {
                                  "Name": "Default-scale-in",
                                  "Description": "Scales in when YARNMemoryAvailablePercentage is beyond 75%",
                                  "Action":{
                                   "SimpleScalingPolicyConfiguration":{
                                     "AdjustmentType": "CHANGE_IN_CAPACITY",
                                     "ScalingAdjustment": -1,
                                     "CoolDown": 300
                                   }
                                  },
                                  "Trigger":{
                                   "CloudWatchAlarmDefinition":{
                                     "ComparisonOperator": "GREATER_THAN",
                                     "EvaluationPeriods": 1,
                                     "MetricName": "YARNMemoryAvailablePercentage", 
                                     "Namespace": "AWS/ElasticMapReduce",
                                     "Period": 300,
                                     "Threshold": 75,
                                     "Statistic": "AVERAGE",
                                     "Unit": "PERCENT",
                                     "Dimensions":[
                                        {
                                          "Key" : "JobFlowId",
                                          "Value" : "${emr.clusterId}"
                                        }
                                     ]
                                   }
                                  }
                                 }
                                ]
                              },
                            'EbsConfiguration': {
                                'EbsBlockDeviceConfigs': [
                                    {
                                        'VolumeSpecification': {
                                            'VolumeType': 'standard',
                                            'SizeInGB': 200
                                        },
                                        'VolumesPerInstance': 1
                                    }
                                ],
                                'EbsOptimized': True
                            }
                            
                        }
                    ]

    return val

def emr_steps():
    val=[
               # {
            #        'Name': 'Spark Application',
             #       'ActionOnFailure': 'CONTINUE',
              #      'HadoopJarStep': {
              #          'Jar': 'command-runner.jar',
              #          'Args': [
              #              "spark-submit",
              #              "/home/hadoop/curated-mdm.py"
              #          ]
              #      }
              #  },
                {
                    'Name': 'Spark Application',
                    'ActionOnFailure': 'CONTINUE',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': [
                            "spark-submit",
                            "/home/hadoop/curated_mdm_pivot.py"
                        ]
                    }
                },
            ]
    return val

def emr_configurations():
    val=[
                    {
                        "Classification": "spark",
                        "Properties": {
                            "maximizeResourceAllocation": "true"
                        }
                    },
                    {
                        'Classification': 'spark-hive-site',
                        'Properties': {
                            "hive.metastore.client.factory.class": "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory"
                        }
                    },
                    {
                        'Classification': 'spark-defaults',
                        'Properties': {
                                        "spark.executor.cores": "20"
                        }
                    },
                    {
                        'Classification': 'hdfs-site',
                        'Properties': {
                            "dfs.client.block.write.replace-datanode-on-failure.policy": "ALWAYS",
                            "dfs.client.block.write.replace-datanode-on-failure.best-effort": "true"
                        }
                    },
                    {
                        'Classification': 'yarn-site',
                        'Properties': {
                            "yarn.log-aggregation-enable": "true",
                            "yarn.log-aggregation.retain-seconds": "60",
                        }
                    }
                ]
    return val

def emr_tags():
    val=[
                    {
                        "Key": "application",
                        "Value": os.environ['tag_name']
                    }

    ]

    return val
