import tarfile
import os
def create_zip():
    file_source= r'emr_scripts/'
    t_file = tarfile.open("emr_scripts_uploads/script.tar.gz", "w:gz")
    files = os.listdir(file_source)
    print(files)
    for f in files:
        t_file.add(file_source + f, arcname=f)
    t_file.close()

create_zip()

#create_zip(r'lambda_code/',"emr_scripts_uploads/lambda-spin-emr.zip")

def create_lambdazip():
    #print("Hey")
    #if os.path.exists("lambda_codes_uploads/lambda-spin-emr.zip"):
    #s.remove("lambda_codes_uploads/lambda-spin-emr.zip")
    import shutil
    shutil.make_archive(r'lambda_codes_uploads/lambda-spin-emr', 'zip', "lambda_codes_uploads/lambda-spin-emr")

create_lambdazip()