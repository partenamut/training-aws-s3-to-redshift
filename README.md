# Ingest Superstore ; from S3 to Redshift

## Prerequisite

Run on mac
Need:
- AWS Cli installed
- zip
- Bash

## Mandatory

AWS Lambda run on linux. The library should be build with linux. The package 'psycopg2' has been copid on this project and it's needed to request Redshift

## Command

### Pack the project

```bash
$ ./build.sh __REGION__ __AWS_PROFILE__ __LAMBDA_FUNCTION_NAME__
```

Example:

`./build.sh eu-west-1 aws_profile myLambdaFunction`

## Good inspiration
Github : [christianhxc/aws-lambda-redshift-copy](https://github.com/christianhxc/aws-lambda-redshift-copy)
