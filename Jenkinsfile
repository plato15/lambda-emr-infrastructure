pipeline {
    agent any
    stages {
        stage('Submit Stack') {
            steps {
            sh "aws s3 cp s3_uploads/all-emr-codes-s3 s3://saidatech-datalake-coderepo-test/ --recursive"
            sh "aws cloudformation delete-stack --stack-name lambda-emr --region 'us-east-1'"
            sh "aws cloudformation create-stack --stack-name lambda-emr --template-body file://CF_Scripts/lambda-for-emr.yml --region 'us-east-1' --capabilities CAPABILITY_NAMED_IAM"
              }
             }
            }
            }

            
