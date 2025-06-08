# variables.tf

variable "aws_region" {
  description = "AWS region for deployment (e.g., us-east-1, eu-central-1)."
  type        = string
  default     = "eu-central-1" # Or your preferred region
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
  default     = "redditAwsPostAnalyzer" # Your original default
}

variable "lambda_code_path" {
  description = "Path to the directory containing Lambda function code."
  type        = string
  default     = "./lambda_code/"
}

variable "lambda_handler" {
  description = "Lambda handler (filename.function_name)."
  type        = string
  default     = "redditAwsPostAnalyzer.lambda_handler" # Your original default
}

variable "lambda_runtime" {
  description = "Lambda runtime environment."
  type        = string
  default     = "python3.9"
}

variable "lambda_timeout" {
  description = "Lambda timeout in seconds."
  type        = number # Changed from string to number
  default     = 180
}

variable "lambda_memory_size" {
  description = "Lambda memory size in MB."
  type        = number # Changed from string to number
  default     = 512
}

variable "lambda_layer_name" {
  description = "Name for the Lambda layer."
  type        = string
  default     = "reddit-aws-analyzer-deps" # Your original default
}

variable "lambda_layer_path" {
  description = "Path to the directory containing layer content."
  type        = string
  default     = "./lambda_layer/"
}

# --- S3 Specific Variables ---

variable "new_s3_bucket_name_for_data" { # Renamed from s3_bucket_name to match new main.tf
  description = "REQUIRED: A globally unique name for the NEW S3 bucket to store Reddit data. Example: 'yourname-reddit-data-unique123'."
  type        = string
  # No default - User MUST provide this value to ensure uniqueness.
}

variable "s3_key_prefix" {
  description = "Prefix within the S3 data bucket for storing parquet files (e.g., reddit-analysis)."
  type        = string
  default     = "reddit-analysis"
}

# --- DynamoDB Specific Variables ---

variable "dynamodb_table_name" {
  description = "Name of the DynamoDB table for tracking processed posts."
  type        = string
  default     = "RedditAwsAnalysis" # Your original default
}

# --- Secrets Manager & Bedrock Variables ---

variable "secrets_manager_secret_arn" {
  description = "REQUIRED: ARN of the Secrets Manager secret containing Reddit API credentials. Example: 'arn:aws:secretsmanager:us-east-1:123456789012:secret:MyRedditSecret-aBcDeF'."
  type        = string
  # No default - User MUST provide this value.
}

variable "bedrock_model_id" {
  description = "Bedrock Model ID to use for summarization (e.g., anthropic.claude-3-haiku-20240307-v1:0)."
  type        = string
  default     = "anthropic.claude-3-haiku-20240307-v1:0" # Your original default
}

variable "bedrock_model_arn" {
  description = "Optional: Full ARN of the Bedrock model if the auto-constructed one is not sufficient. If empty, it's constructed from aws_region and bedrock_model_id."
  type        = string
  default     = ""
}

# --- Lambda Environment Variables (Terraform converts numbers to strings for env vars) ---

variable "post_limit" {
  description = "Max Reddit posts to fetch per Lambda run."
  type        = number # Changed from string to number
  default     = 5
}

variable "comment_limit" {
  description = "Max comments to fetch per Reddit post."
  type        = number # Changed from string to number
  default     = 5
}

variable "new_comments_to_process" {
  description = "Minimum number of new comments on a post for it to be re-processed."
  type        = number # Changed from string to number
  default     = 2
}

variable "new_post_check_limit" {
  description = "Number of recent posts to check for updates."
  type        = number # Changed from string to number
  default     = 50
}

# --- EventBridge Schedule Variables ---

variable "lambda_eventbridge_schedule_expressions" {
  description = "List of cron expressions for the Lambda EventBridge schedules (e.g., noon UTC on Tuesdays and Fridays)."
  type        = list(string)
  default     = ["cron(0 12 ? * TUE *)", "cron(0 12 ? * FRI *)"] # Your original default
}

# --- Glue Specific Variables ---

variable "glue_database_name_suffix" { # Aligned with new main.tf
  description = "Suffix for the Glue database name (appended to project_name)."
  type        = string
  default     = "_reddit_data_db"
}

variable "glue_crawler_name_suffix" { # Aligned with new main.tf
  description = "Suffix for the Glue crawler name (appended to project_name)."
  type        = string
  default     = "-reddit-data-crawler"
}

variable "glue_crawler_schedule_expression" { # Kept from your file, main.tf uses this
  description = "Cron expression for the Glue Crawler schedule (e.g., 'cron(0 2 ? * SAT *)' for 2 AM UTC on Saturdays)."
  type        = string
  default     = "cron(0 2 ? * SAT *)" # Your original default
}

# --- Athena Specific Variables ---

variable "athena_workgroup_name_suffix" { # Aligned with new main.tf
  description = "Suffix for the Athena workgroup name (appended to project_name)."
  type        = string
  default     = "-workgroup"
}

variable "athena_results_bucket_suffix" { # Aligned with new main.tf
  description = "Suffix for the Athena results bucket name (used in constructing a unique name)."
  type        = string
  default     = "-athena-results"
}