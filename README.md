Goal: Deploy scalable EMR into the new datalakeli account to run spark jobs originally run by Glue. 

How? 

The EMR will be created with lambda based on a cloudwatch cron schedule 

The lambda function lives in a parent folder named DataLake-lambda-create-emr-prod and this folder is composed of 3 elements listed below 

	#Subfolder named config 

		This subfolder contains a file named emr_properties.py file 

	#File1 named emr_create.py 

	#File2 named EMRLambda.py 

 

So we have a directory similar to the structure below 

	+--- Datalake-lambda-create-emr-prod 

	| 

	| 

	\---  config 

	| 

	| 

	\emr_properties.py 

 

	\---emr_create.py 

	\--- EMRLambda.py	 

 

 

emr_properties.py: This contains all configurations of the EMR cluster. The functions in this file will be called in emr_create.py file 

emr_create.py: This is function that contains all codes used to create EMR including the steps 

EMRLambda.py: This is the main lambda function that lambda triggers. This function will call emr_create.py 

 

How is lambda deployed into the datalake account: 

Below is the tree structure of the project directory named Datalake-Infrastructure-EMR-PROD(Will be uploaded to the GitHub) 

Developer should clone this repo to their local directory 
	C:. | output.doc | readme.md |
	+---CF_Scripts | lambda-for-emr.yml |
	---s3_uploads | create_zip_file.py |
	+---emr_scripts | curated-mdm.py | curated_mdm_pivot.py | terminate_idle_cluster.sh |
	+---emr_scripts_uploads | script.tar.gz | setup.sh | terminate_idle_cluster.sh |
	---lambda_codes_uploads | lambda-spin-emr.zip |
	---lambda-spin-emr | EMRLambda.py | emr_create.py |
	---config emr_properties.py
 

The directory structure is explained below 

#  CF Script: This contains the Cloudformation script to create the lambda function named DataLake-lambda-create-emr-prod. This script has been uploaded in s3(https://gabe-datalakeprodli-datalake-emr-codes-prod.s3.amazonaws.com/emr cf templates/lambda-for-emr.yml 

# S3 uploads: This directory contains the contents below 

# Create_zip_file.py: This is a python script that performs 2 major functions 

# Zip the contents of the emr scripts uploaded the zip file to emr scripts uploads 

# Zip the content of lambda-spin-emr inside lambda-spin-emr folder and upload the zipped file into lambda code uploads 

# emr_scripts: Contains the script that is run as a spark step. Actual jobs 

# emr_script_uploads: The content of this folder is the result of the zipping emr_scripts. It also contains setup.sh script that involves all emr prerun activities. The content in this will be uploaded to the s3 directory. https://gabe-datalakeprodli-datalake-emr-codes-prod.s3.amazonaws.com/emr-codes 

# lambda_codes_uploads: This contains lambda-spin-emr.zip which is the zip file from all 	 

Contents in the folder lambda-spin-emr in the same directory. 

Only the below files are uploaded to lambda 

Upload the below in https://gabe-datelakeprodli-datalake-emr-codes-prod.s3.amazonaws.com/emr-codes/ 

script.tar.gz 

setup.sh 

terminate_idle_cluster.sh 

Upload the below to https://gabe-datalakeprodli-datalake-emr-codes-prod.s3.amazonaws.com/lambda-codes/ 

lambda-spin-emr.zip 

 

Upload the below to https://gabe-datalakeprodli-datalake-emr-codes-prod.s3.amazonsaws.com/emr cf templates/  

lamba-for-enr.yml 

 

Once the steps are done, the Cloudformation will deploy lambda to create EMR in datalakeprodli. Additionally the stack will create the Lambda execution role and necessary EMR security groups and instance role(Ref CHG0000131102) 