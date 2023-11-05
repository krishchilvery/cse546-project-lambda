import boto3
import json
import os
import time

region = "us-east-1"
os.environ["AWS_DEFAULT_REGION"] = region

s3 = boto3.client("s3")
iam = boto3.client("iam")
dynamodb = boto3.resource("dynamodb")
ecr = boto3.client("ecr")
lambda_client = boto3.client("lambda")

student_data_path = "student_data.json"
table_name = "546StudentData"

input_bucket = "546proj2-oneszeros"
output_bucket = "546proj2output-oneszeros"

lambda_image_name = "cse546projectlambda:latest"
iam_function_name = "cse546-project-lambda"
repository_name = "cse546projectlambda"
iam_role_name = "cse546-project-lambda-admin"


def create_dynamodb():
    try:
        table = dynamodb.Table(table_name)
        print("Table already exists")
    except dynamodb.meta.client.exceptions.ResourceNotFoundException:
        table = dynamodb.create_table(
            TableName = table_name,
            KeySchema = [
                {
                    "AttributeName": "id",
                    "KeyType": "HASH"
                }
            ],
            AttributeDefinitions = [
                {
                    "AttributeName": "id",
                    "AttributeType": "N"
                }
            ],
            ProvisionedThroughput = {
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 5
            }
        )
        table.meta.client.get_waiter("table_exists").wait(TableName = table_name)
        print("Successfully created table")

    with open(student_data_path) as f:
        student_data = json.load(f)
        for student in student_data:
            table.put_item(Item = student)

def create_input_s3_bucket():
    s3.create_bucket(
        Bucket = input_bucket,
        # CreateBucketConfiguration = {
        #     "LocationConstraint": region
        # }
    )
    print("Successfully created input bucket")
    # public_upload_policy = {
    #     "Version": "2012-10-17",
    #     "Statement": [
    #         {
    #             "Sid": "PublicUploadPolicy",
    #             "Effect": "Allow",
    #             "Principal": "*",
    #             "Action": "s3:PutObject",
    #             "Resource": f"arn:aws:s3:::{input_bucket}/*"
    #         }
    #     ]
    # }
    # public_upload_policy_str = json.dumps(public_upload_policy)
    # s3.put_bucket_policy(Bucket = input_bucket, Policy = public_upload_policy_str)

def create_output_s3_bucket():
    s3.create_bucket(
        Bucket = output_bucket,
        # CreateBucketConfiguration = {
        #     "LocationConstraint": region
        # }
    )
    print("Successfully created output bucket")
    # public_read_policy = {
    #     "Version": "2012-10-17",
    #     "Statement": [
    #         {
    #             "Sid": "PublicReadPolicy",
    #             "Effect": "Allow",
    #             "Principal": "*",
    #             "Action": "s3:GetObject",
    #             "Resource": f"arn:aws:s3:::{output_bucket}/*"
    #         }
    #     ]
    # }
    # public_read_policy_str = json.dumps(public_read_policy)
    # s3.put_bucket_policy(Bucket = output_bucket, Policy = public_read_policy_str)

def create_ecr_repository():
    try:
        resp = ecr.create_repository(repositoryName = repository_name)
        repository_uri = resp["repository"]["repositoryUri"]
    except ecr.exceptions.RepositoryAlreadyExistsException:
        resp = ecr.describe_repositories(
            repositoryNames=[repository_name]
        )
        repository_uri = resp['repositories'][0]['repositoryUri']
    return repository_uri

def upload_lambda_image_to_ecr():
    repository_uri = create_ecr_repository()
    print(repository_uri)
    resp = os.system(f"docker build -t {lambda_image_name} .")
    if resp != 0:
        raise Exception("Failed to build docker image")
    login_resp = ecr.get_authorization_token()
    token = login_resp["authorizationData"][0]["authorizationToken"]
    login_cmd = f"aws ecr get-login-password --region {region} | docker login --username AWS --password-stdin {repository_uri.split('/')[0]}"
    resp = os.system(login_cmd)
    if resp != 0:
        raise Exception("Failed to login to ECR")
    resp = os.system(f"docker tag {lambda_image_name} {repository_uri}:latest")
    if resp != 0:
        raise Exception("Failed to tag docker image")
    resp = os.system(f"docker push {repository_uri}:latest")
    if resp != 0:
        raise Exception("Failed to push docker image")
    print("Successfully uploaded docker image to ECR")

def create_iam_role():
    try:
        resp = iam.get_role(RoleName = iam_role_name)
        arn = resp["Role"]["Arn"]
    except iam.exceptions.NoSuchEntityException:
        resp = iam.create_role(
            RoleName = iam_role_name,
            AssumeRolePolicyDocument = json.dumps({
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Sid": "",
                        "Effect": "Allow",
                        "Principal": {
                            "Service": "lambda.amazonaws.com"
                        },
                        "Action": "sts:AssumeRole"
                    }
                ]
            })
        )
        arn = resp["Role"]["Arn"]
    return arn

def create_lambda_function():
    repository_uri = create_ecr_repository()
    try:
        resp = lambda_client.create_function(
            FunctionName = iam_function_name,
            Role = create_iam_role(),
            Code = {
                "ImageUri": f"{repository_uri}:latest"
            },
            PackageType = "Image",
            Timeout = 300,
            MemorySize = 1024,
            Architectures = ["arm64"]
        )
        print("Waiting 30s for the function to be created")
        time.sleep(30)
    except lambda_client.exceptions.ResourceConflictException:
        resp = lambda_client.update_function_code(
            FunctionName = iam_function_name,
            ImageUri = f"{repository_uri}:latest"
        )
        print("Waiting 10s for the function to be upadted")
        time.sleep(10)
    function_arn = resp["FunctionArn"]
    print(function_arn)
    try:
        lambda_client.add_permission(
            FunctionName=iam_function_name,
            StatementId='s3-trigger',
            Action='lambda:InvokeFunction',
            Principal='s3.amazonaws.com',
            SourceArn=f'arn:aws:s3:::{input_bucket}'
        )
    except lambda_client.exceptions.ResourceConflictException:
        pass

    resp = s3.put_bucket_notification_configuration(
        Bucket=input_bucket,
        NotificationConfiguration={
            'LambdaFunctionConfigurations': [
                {
                    'LambdaFunctionArn': function_arn,
                    'Events': ['s3:ObjectCreated:*']
                }
            ]
        }
    )


if __name__ == "__main__":
    upload_lambda_image_to_ecr()
    create_dynamodb()
    create_input_s3_bucket()
    create_output_s3_bucket()
    create_lambda_function()