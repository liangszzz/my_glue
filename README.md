# my_glue

## 1. Overview

My glue Project!

## 2. Usage

### 2.1 init project

```bash
python3 -m pip install --upgrade pip
pip install -U poetry

poetry install -v
```

### 2.2 usage

TODO

## 3. Develop

create local develop environment in windows

https://github.com/aws-samples/aws-glue-local-development
https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-libraries.html#develop-local-python
https://github.com/awslabs/aws-glue-libs
https://github.com/steveloughran/winutils/tree/master/hadoop-3.0.0/bin
https://stackoverflow.com/questions/41851066/exception-in-thread-main-java-lang-unsatisfiedlinkerror-org-apache-hadoop-io

1. install python3.10 or later
2. install java8 or later
4. download https://aws-glue-etl-artifacts.s3.amazonaws.com/glue-4.0/spark-3.3.0-amzn-1-bin-3.3.3-amzn-0.tgz
5. unzip D:\glue_local\spark
6. system environment variable SPARK_HOME,HADOOP_HOME same path like D:\glue_local\spark
7. download https://github.com/awslabs/aws-glue-libs/archive/refs/tags/v4.0.zip
8. zip and mvn package get PyGlue.zip in linux or download from aws glue4.0 docker container(amazon/aws-glue-libs:
   glue_libs_4.0.0_image_01) /home/glue_user/aws-glue-libs/PyGlue.zip
9. download hadoop-3.0.0/bin/winutils.exe and hadoop-3.0.0/bin/hadoop.dll
10. copy hadoop-3.0.0/bin/winutils.exe and hadoop-3.0.0/bin/hadoop.dll to D:\glue_local\spark\bin


1. python PYTHONPATH add

```
   D:\glue_local\spark\python
   D:\glue_local\spark\python\lib\py4j-0.10.9.5-src.zip
   D:\glue_local\aws-glue-libs\PyGlue.zip
```

2. run test or job
3. by error create dir like D:\tmp\spark-events


1. install localstack
2. install aws cli
