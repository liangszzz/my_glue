import boto3

glue = boto3.client(service_name="glue")


myJob = glue.create_job(
    Name="sample",
    Role="Glue_DefaultRole",
    Command={"Name": "glueetl", "ScriptLocation": "s3://my_script_bucket/scripts/my_etl_script.py"},
)
