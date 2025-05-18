# --- IAM Role and Policy for Lambda ---

resource "aws_iam_role" "lambda_exec_role" {
  name = "${var.project_name}-lambda-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Action = "sts:AssumeRole"
      Effect = "Allow"
      Principal = {
        Service = "lambda.amazonaws.com"
      }
    }]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_policy" "lambda_policy" {
  name        = "${var.project_name}-lambda-policy"
  description = "IAM policy for Reddit AWS Analyzer Lambda"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_function_name}:*"
      },
      {
        Effect   = "Allow",
        Action   = "secretsmanager:GetSecretValue",
        Resource = var.secrets_manager_secret_arn
      },
      {
        Effect = "Allow",
        Action = [
          "dynamodb:GetItem",
          "dynamodb:PutItem"
        ],
        Resource = "arn:aws:dynamodb:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${var.dynamodb_table_name}"
      },
      {
        Effect = "Allow",
        Action = "s3:PutObject",
        Resource = "${aws_s3_bucket.reddit_data.arn}/${var.s3_key_prefix}/*"
      },
      {
        Effect   = "Allow",
        Action   = "bedrock:InvokeModel",
        Resource = var.bedrock_model_arn != "" ? var.bedrock_model_arn : "arn:aws:bedrock:${var.aws_region}::foundation-model/${var.bedrock_model_id}"
      }
    ]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_role_policy_attachment" "lambda_policy_attach" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# --- S3 Bucket ---

resource "aws_s3_bucket" "reddit_data" {
  bucket = var.s3_bucket_name

  tags = {
    Project = var.project_name
    Purpose = "Stores analyzed Reddit post data in Parquet format"
  }
}

resource "aws_s3_bucket_public_access_block" "reddit_data_public_access" {
  bucket = aws_s3_bucket.reddit_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "reddit_data_versioning" {
  bucket = aws_s3_bucket.reddit_data.id
  versioning_configuration {
    status = "Enabled"
  }
}

# --- Lambda Layer ---

# Package the layer content from the source_dir
data "archive_file" "lambda_layer_zip" { # Consistent name
  type        = "zip"
  source_dir  = var.lambda_layer_path # Should point to "./lambda-layer/"
  output_path = "./archived_lambda_layer_${var.lambda_layer_name}.zip" # Temporary local zip file
}

resource "aws_lambda_layer_version" "lambda_deps_layer" {
  layer_name          = var.lambda_layer_name
  filename            = data.archive_file.lambda_layer_zip.output_path # Correctly references the data block
  source_code_hash    = data.archive_file.lambda_layer_zip.output_base64sha256 # Correctly references
  compatible_runtimes = [var.lambda_runtime]
  description         = "Dependencies for ${var.project_name} Lambda"
}

# --- Lambda Function ---

# Package the function code from the source_dir
data "archive_file" "lambda_code_zip" { # Consistent name
  type        = "zip"
  source_dir  = var.lambda_code_path # Should point to "./lambda-code/"
  output_path = "./archived_lambda_code_${var.lambda_function_name}.zip" # Temporary local zip file
}

# Optional: Manage CloudWatch Log Group explicitly for retention etc.
resource "aws_cloudwatch_log_group" "lambda_log_group" {
  name              = "/aws/lambda/${var.lambda_function_name}"
  retention_in_days = 14

  tags = {
    Project = var.project_name
  }
}

resource "aws_lambda_function" "reddit_analyzer_lambda" {
  function_name    = var.lambda_function_name
  role             = aws_iam_role.lambda_exec_role.arn
  handler          = var.lambda_handler
  runtime          = var.lambda_runtime
  timeout          = var.lambda_timeout
  memory_size      = var.lambda_memory_size
  filename         = data.archive_file.lambda_code_zip.output_path 
  source_code_hash = data.archive_file.lambda_code_zip.output_base64sha256 

  layers = [
    aws_lambda_layer_version.lambda_deps_layer.arn
  ]

  environment {
    variables = {
      MY_AWS_REGION        = var.aws_region
      POST_LIMIT           = var.post_limit
      SUBREDDIT_NAME       = "aws"
      COMMENT_LIMIT        = var.comment_limit
      DYNAMODB_TABLE_NAME  = var.dynamodb_table_name
      S3_BUCKET_NAME       = aws_s3_bucket.reddit_data.id
      BEDROCK_MODEL_ID     = var.bedrock_model_id
      S3_KEY_PREFIX        = var.s3_key_prefix
      SECRET_NAME          = split(":", var.secrets_manager_secret_arn)[6]
    }
  }

  tags = {
    Project = var.project_name
  }

  depends_on = [aws_cloudwatch_log_group.lambda_log_group]
}

# --- DynamoDB Table for Processed Post IDs ---

resource "aws_dynamodb_table" "processed_posts_table" {
  name         = var.dynamodb_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "PostID"

  attribute {
    name = "PostID"
    type = "S"
  }

  point_in_time_recovery {
    enabled = true
  }

  tags = {
    Project = var.project_name
    Purpose = "Stores IDs of processed Reddit posts to prevent duplicates"
  }
}


# --- EventBridge Schedule for Lambda Function ---

resource "aws_cloudwatch_event_rule" "lambda_weekly_scheduler" {
  name                = "${var.project_name}-lambda-weekly-trigger"
  description         = "Triggers the Reddit Analyzer Lambda weekly every Friday"
  # Runs at 12:00 PM UTC every Friday
  schedule_expression = "cron(0 15 ? * FRI *)"

  tags = {
    Project = var.project_name
  }
}

resource "aws_cloudwatch_event_target" "lambda_weekly_target" {
  rule      = aws_cloudwatch_event_rule.lambda_weekly_scheduler.name
  arn       = aws_lambda_function.reddit_analyzer_lambda.arn
  target_id = "${var.lambda_function_name}-target"
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_lambda" {
  statement_id  = "AllowExecutionFromCloudWatch"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reddit_analyzer_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = aws_cloudwatch_event_rule.lambda_weekly_scheduler.arn
}

# --- Glue Data Catalog Database ---

resource "aws_glue_catalog_database" "reddit_data_db" {
  name        = "${var.project_name}_reddit_data_db" 
  description = "Database for Reddit posts data crawled from S3"

  tags = {
    Project = var.project_name
  }
}

# --- IAM Role and Policy for Glue Crawler ---

resource "aws_iam_role" "glue_crawler_role" {
  name = "${var.project_name}-glue-crawler-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = {
        Service = "glue.amazonaws.com"
      }
    }]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_policy" "glue_crawler_policy" {
  name        = "${var.project_name}-glue-crawler-policy"
  description = "IAM policy for Glue crawler to access S3 and Glue Data Catalog resources"

  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect = "Allow",
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ],
        Resource = [
          aws_s3_bucket.reddit_data.arn, # For ListBucket
          "${aws_s3_bucket.reddit_data.arn}/${var.s3_key_prefix}/*" # For GetObject
        ]
      },
      {
        Effect = "Allow",
        Action = "s3:GetBucketLocation",
        Resource = aws_s3_bucket.reddit_data.arn
      },
      {
        Effect = "Allow",
        Action = [
          "glue:GetDatabase",
          "glue:GetDatabases",
          "glue:CreateTable",
          "glue:UpdateTable",
          "glue:DeleteTable",
          "glue:GetTable",
          "glue:GetTables",
          "glue:GetPartition",
          "glue:GetPartitions",
          "glue:CreatePartition",
          "glue:UpdatePartition",
          "glue:DeletePartition",
          "glue:BatchCreatePartition",
          "glue:BatchUpdatePartition",
          "glue:BatchDeletePartition",
          "glue:BatchGetPartition"
        ],
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.reddit_data_db.name}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.reddit_data_db.name}/*"
        ]
      },
      {
        # Required for Glue Crawlers to write logs
        Effect = "Allow",
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ],
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/crawlers:*"
      }
      # If using customer managed KMS keys for S3, add kms:Decrypt for Glue role
    ]
  })

  tags = {
    Project = var.project_name
  }
}

resource "aws_iam_role_policy_attachment" "glue_crawler_policy_attach" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = aws_iam_policy.glue_crawler_policy.arn
}

# --- Glue Crawler ---

resource "aws_glue_crawler" "reddit_data_crawler" {
  name          = "${var.project_name}-reddit-data-crawler" # Consider making this a variable
  database_name = aws_glue_catalog_database.reddit_data_db.name
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.reddit_data.id}/${var.s3_key_prefix}/"
  }

  # Example: Run weekly on Saturday at 2 AM UTC after Friday's Lambda run
  schedule = "cron(0 2 ? * SAT *)"

  configuration = jsonencode({
    Version = 1.0,
    CrawlerOutput = {
      Partitions      = { AddOrUpdateBehavior = "InheritFromTable" }, # Important for partitioned data
      Tables          = { AddOrUpdateBehavior = "MergeNewColumns" }   # Good for schema evolution
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE" # Or "LOG"
    delete_behavior = "LOG"                # Or "DEPRECATE_IN_DATABASE", "DELETE_FROM_DATABASE"
  }

  # Optional: specify table prefix if crawler creates multiple tables
  # table_prefix = "reddit_"

  tags = {
    Project = var.project_name
  }

  depends_on = [
    aws_iam_role.glue_crawler_role,
    aws_glue_catalog_database.reddit_data_db
  ]
}

# --- S3 Bucket for Athena Query Results ---

resource "aws_s3_bucket" "athena_query_results" {
  bucket = "${lower(var.project_name)}-athena-results-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}" # Consider making this a variable

  tags = {
    Project = var.project_name
    Purpose = "Stores Athena query results for ${var.project_name}"
  }
}

resource "aws_s3_bucket_public_access_block" "athena_query_results_public_access" {
  bucket = aws_s3_bucket.athena_query_results.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "athena_query_results_versioning" {
  bucket = aws_s3_bucket.athena_query_results.id
  versioning_configuration {
    status = "Enabled"
  }
}

# --- Athena Workgroup ---

resource "aws_athena_workgroup" "reddit_analyzer_workgroup" {
  name = "${var.project_name}-workgroup" # Consider making this a variable
  description = "Athena workgroup for querying Reddit analysis data"
  state       = "ENABLED"

  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_query_results.id}/"
    }
    # Optional: enforce workgroup configuration to prevent users from overriding output location
    enforce_workgroup_configuration = true
    # Optional: publish CloudWatch metrics for Athena queries
    publish_cloudwatch_metrics_enabled = true
    # Optional: control query costs
    # bytes_scanned_cutoff_per_query = 1000000000 # 1GB, example
  }

  tags = {
    Project = var.project_name
  }

  depends_on = [aws_s3_bucket.athena_query_results]
}