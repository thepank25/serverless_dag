provider "aws" {
    region = "us-east-1"
    profile = "mwaa"
}

resource "aws_iam_role" "lambda_role" {
    name = "serverless_dag_role"

    assume_role_policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
            {
                Action = "sts:AssumeRole"
                Effect = "Allow"
                Principal = {
                    Service = "lambda.amazonaws.com"
                }
            }
        ]
    })
}

resource "aws_iam_role_policy_attachment" "lambda_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_policy" "s3_admin_policy" {
    name        = "s3_admin_policy"
    description = "Policy to grant admin rights to S3 bucket credence-core-raw"

    policy = jsonencode({
        Version = "2012-10-17"
        Statement = [
            {
                Action = [
                    "s3:*"
                ]
                Effect   = "Allow"
                Resource = [
                    "arn:aws:s3:::${var.account_name}-core-raw",
                    "arn:aws:s3:::${var.account_name}-core-raw/*"
                ]
            }
        ]
    })
}

resource "aws_iam_role_policy_attachment" "s3_admin_policy_attachment" {
    role       = aws_iam_role.lambda_role.name
    policy_arn = aws_iam_policy.s3_admin_policy.arn
}

locals {
    lambda_function_names = {
        invoke_small_memory_lambda  = 128
        invoke_medium_memory_lambda = 256
        invoke_large_memory_lambda  = 1024
    }
}

resource "aws_lambda_function" "branching_lambdas" {
    for_each = local.lambda_function_names
    function_name = each.key
    role          = aws_iam_role.lambda_role.arn
    handler       = "lambda_function_payload.lambda_handler"
    runtime       = "python3.10"
    filename      = "lambda_function_payload.zip"
    memory_size   = each.value
    timeout       = 30

    source_code_hash = filebase64sha256("lambda_function_payload.zip")
}

