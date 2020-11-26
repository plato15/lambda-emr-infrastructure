pipeline {
    agent any
    stages {
        stage('Submit Stack') {
            steps {
            sh "aws cloudformation create-stack --stack-name s3bucket --template-body file://CF_Scripts/lambda-for-emr.yml --region 'us-east-1'"
              }
             }
            }
            }