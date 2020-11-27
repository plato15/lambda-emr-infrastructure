pipeline {
    agent any
    stages {
        stage('Submit Stack') {
            steps {
            sh "aws s3 cp s3_uploads/all-emr-codes-s3/ s3://saidatech-datalake-coderepo-test/"
            sh "aws cloudformation create-stack --stack-name s3bucket --template-body file://CF_Scripts/lambda-for-emr.yml --region 'us-east-1' --capabilities CAPABILITY_NAMED_IAM"
              }
             }
            }
            }

            
