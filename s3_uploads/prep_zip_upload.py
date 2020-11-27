import tarfile
import os
def prepare_zip_upload(file_source, file_dest):
    #file_source= r'emr_scripts/'
    t_file = tarfile.open(file_dest, "w:gz")
    files = os.listdir(file_source)
    print(files)
    for f in files:
        t_file.add(file_source + f, arcname=f)
    t_file.close()

prepare_zip_upload(r'all-emr-codes-local/emr-jobs/', "all-emr-codes-s3/emr-jobs/emr-jobs.tar.gz")
prepare_zip_upload(r'all-emr-codes-local/lambda-codes/lambda-spin-emr/', "all-emr-codes-s3/lambda-codes/lambda-spin-emr.zip")

#create_zip(r'lambda_code/',"emr_scripts_uploads/lambda-spin-emr.zip")

def create_lambdazip():
    #print("Hey")
    #if os.path.exists("lambda_codes_uploads/lambda-spin-emr.zip"):
    #s.remove("lambda_codes_uploads/lambda-spin-emr.zip")
    import shutil
    shutil.make_archive(r'lambda_codes_uploads/lambda-spin-emr', 'zip', "lambda_codes_uploads/lambda-spin-emr")

#create_lambdazip()