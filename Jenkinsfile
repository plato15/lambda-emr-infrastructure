pipeline {
    agent any
    stages {
        stage('Submit Stack') {
            steps {
            sh "aws s3 cp s3_uploads/lambda_codes_uploads/lambda-spin-emr.zip s3://saidatech-datalake-coderepo-test/lambda-codes/lambda-spin-emr.zip/lambda-spin-emr.zip"
            sh "aws s3 cp file://s3_uploads/emr_scripts_uploads/* s3://saidatech-datalake-coderepo-test/bootstrap-scripts/"
            sh "aws cloudformation create-stack --stack-name s3bucket --template-body file://CF_Scripts/lambda-for-emr.yml --region 'us-east-1' --capabilities CAPABILITY_NAMED_IAM"
              }
             }
            }
            }

            