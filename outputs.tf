# outputs.tf

output "lambda_function_name" {
  description = "The name of the deployed Lambda function"
  value       = aws_lambda_function.reddit_analyzer_lambda.function_name
}

output "lambda_function_arn" {
  description = "The ARN of the deployed Lambda function"
  value       = aws_lambda_function.reddit_analyzer_lambda.arn
}

output "lambda_iam_role_arn" {
  description = "The ARN of the IAM role created for the Lambda function"
  value       = aws_iam_role.lambda_exec_role.arn
}

output "lambda_layer_arn" {
  description = "The ARN of the deployed Lambda layer version"
  value       = aws_lambda_layer_version.lambda_deps_layer.arn
}

output "s3_data_bucket_name" {
  description = "The name of the S3 bucket for storing raw Reddit data"
  value       = aws_s3_bucket.reddit_data_new.id # Corrected: Points to reddit_data_new
}

output "s3_data_bucket_arn" {
  description = "The ARN of the S3 bucket for storing raw Reddit data"
  value       = aws_s3_bucket.reddit_data_new.arn # Corrected: Points to reddit_data_new
}

output "dynamodb_table_name" {
  description = "The name of the DynamoDB table for processed posts"
  value       = aws_dynamodb_table.processed_posts_table.name
}

output "dynamodb_table_arn" {
  description = "The ARN of the DynamoDB table for processed posts"
  value       = aws_dynamodb_table.processed_posts_table.arn
}

# --- Outputs for EventBridge, Glue, and Athena ---

output "eventbridge_lambda_scheduler_names" {
  description = "A map of EventBridge rule names scheduling the Lambda function, keyed by schedule identifier"
  value       = { for k, rule in aws_cloudwatch_event_rule.lambda_weekly_scheduler : k => rule.name }
}

output "eventbridge_lambda_scheduler_arns" {
  description = "A map of EventBridge rule ARNs scheduling the Lambda function, keyed by schedule identifier"
  value       = { for k, rule in aws_cloudwatch_event_rule.lambda_weekly_scheduler : k => rule.arn }
}

output "glue_database_name" {
  description = "The name of the Glue Data Catalog database created"
  value       = aws_glue_catalog_database.reddit_data_db.name
}

output "glue_crawler_name" {
  description = "The name of the Glue Crawler created"
  value       = aws_glue_crawler.reddit_data_crawler.name
}

output "glue_crawler_iam_role_arn" {
  description = "The ARN of the IAM role for the Glue Crawler"
  value       = aws_iam_role.glue_crawler_role.arn
}

output "s3_athena_results_bucket_name" {
  description = "The name of the S3 bucket for storing Athena query results"
  value       = aws_s3_bucket.athena_query_results.id
}

output "s3_athena_results_bucket_arn" {
  description = "The ARN of the S3 bucket for storing Athena query results"
  value       = aws_s3_bucket.athena_query_results.arn
}

output "athena_workgroup_name" {
  description = "The name of the Athena workgroup created"
  value       = aws_athena_workgroup.reddit_analyzer_workgroup.name
}