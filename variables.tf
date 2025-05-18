variable "aws_region" {
  description = "AWS region for deployment (e.g., us-east-1, eu-central-1)."
  type        = string
  default     = "eu-central-1" # Or choose a common default like "us-east-1"
}

variable "project_name" {
  description = "Base name for resources, used as a prefix."
  type        = string
  default     = "reddit-aws-analyzer"
}

# --- Lambda Specific Variables ---

variable "lambda_function_name" {
  description = "Name for the Lambda function."
  type        = string
  default     = "redditAwsPostAnalyzer"
}

variable "lambda_code_path" {
  description = "Path to the directory containing Lambda function code."
  type        = string
  default     = "./lambda_code/"
}

variable "lambda_handler" {
  description = "Lambda handler (filename.function_name)."
  type        = string
  default     = "redditAwsPostAnalyzer.lambda_handler"
}

variable "lambda_runtime" {
  description = "Lambda runtime environment."
  type        = string
  default     = "python3.9"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds."
  type        = number
  default     = 180
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB."
  type        = number
  default     = 512
}

variable "lambda_layer_name" {
  description = "Name for the Lambda layer."
  type        = string
  default     = "reddit-aws-analyzer-deps"
}

variable "lambda_layer_path" {
  description = "Path to the directory containing layer content."
  type        = string
  default     = "./lambda_layer/"
}

# --- S3 Specific Variables ---

variable "s3_bucket_name" {
  description = "REQUIRED: Name for the S3 bucket storing raw data (must be globally unique). Example: 'my-reddit-data-unique123'."
  type        = string
  default     = "" # User MUST provide this value.
}

variable "s3_key_prefix" {
  description = "Prefix within the S3 data bucket for storing parquet files."
  type        = string
  default     = "reddit-analysis"
}

# --- DynamoDB Specific Variables ---

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table for tracking processed posts."
  type        = string
  default     = "RedditAwsAnalysis"
}

# --- Secrets Manager & Bedrock Variables ---

variable "secrets_manager_secret_arn" {
  description = "REQUIRED: ARN of the Secrets Manager secret containing Reddit API credentials. Example: 'arn:aws:secretsmanager:us-east-1:123456789012:secret:MyRedditSecret-aBcDeF'."
  type        = string
  default     = "" # User MUST provide this value.
}

variable "bedrock_model_id" {
  description = "Bedrock Model ID to use for summarization (e.g., anthropic.claude-3-haiku-20240307-v1:0)."
  type        = string
  default     = "anthropic.claude-3-haiku-20240307-v1:0" # This is a general model ID
}

variable "bedrock_model_arn" {
  description = "Optional: Full ARN of the Bedrock model if the auto-constructed one is not sufficient. If empty, it's constructed from aws_region and bedrock_model_id."
  type        = string
  default     = ""
}

# --- Lambda Environment Variables (passed as strings) ---

variable "post_limit" {
  description = "Max Reddit posts to fetch per Lambda run."
  type        = string
  default     = "10"
}

variable "comment_limit" {
  description = "Max comments to fetch per Reddit post."
  type        = string
  default     = "5"
}

# --- EventBridge Schedule Variables ---

variable "lambda_eventbridge_schedule_name" {
  description = "Optional: Name for the EventBridge rule scheduling the Lambda. If empty, derived from project_name."
  type        = string
  default     = ""
}

variable "lambda_eventbridge_schedule_expression" {
  description = "Cron expression for the Lambda EventBridge schedule (e.g., 'cron(0 12 ? * FRI *)' for noon UTC on Fridays)."
  type        = string
  default     = "cron(0 12 ? * FRI *)"
}

# --- Glue Specific Variables ---

variable "glue_database_name" {
  description = "Optional: Name for the Glue Data Catalog database. If empty, derived from project_name."
  type        = string
  default     = ""
}

variable "glue_crawler_name" {
  description = "Optional: Name for the Glue Crawler. If empty, derived from project_name."
  type        = string
  default     = ""
}

variable "glue_crawler_role_name" {
  description = "Optional: Name for the IAM role used by the Glue Crawler. If empty, derived from project_name."
  type        = string
  default     = ""
}

variable "glue_crawler_schedule_expression" {
  description = "Cron expression for the Glue Crawler schedule (e.g., 'cron(0 2 ? * SAT *)' for 2 AM UTC on Saturdays)."
  type        = string
  default     = "cron(0 2 ? * SAT *)"
}

variable "glue_crawler_s3_target_path_suffix" {
  description = "Optional: Suffix for the Glue crawler S3 target path. Defaults to var.s3_key_prefix if empty."
  type        = string
  default     = ""
}

# --- Athena Specific Variables ---

variable "athena_workgroup_name" {
  description = "Optional: Name for the Athena workgroup. If empty, derived from project_name."
  type        = string
  default     = ""
}

variable "athena_results_s3_bucket_name" {
  description = "Optional: Name for the S3 bucket to store Athena query results. If empty, a unique name will be generated based on project_name, account ID, and region."
  type        = string
  default     = ""
}

variable "athena_results_s3_key_prefix" {
  description = "Optional key prefix within the Athena results S3 bucket."
  type        = string
  default     = "query_results/"
}