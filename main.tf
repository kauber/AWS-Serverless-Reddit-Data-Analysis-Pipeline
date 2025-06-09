# --- IAM Role and Policy for Lambda ---
resource "aws_iam_role" "lambda_exec_role" {
  name = "${var.project_name}-lambda-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = { Service = "lambda.amazonaws.com" }
    }]
  })
  tags = { Project = var.project_name }
}

resource "aws_iam_policy" "lambda_policy" {
  name        = "${var.project_name}-lambda-policy"
  description = "IAM policy for Lambda"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws/lambda/${var.lambda_function_name}:*"
      },
      {
        Effect   = "Allow",
        Action   = "secretsmanager:GetSecretValue",
        Resource = var.secrets_manager_secret_arn
      },
      {
        Effect   = "Allow",
        Action   = ["dynamodb:GetItem", "dynamodb:PutItem"],
        Resource = aws_dynamodb_table.processed_posts_table.arn
      },
      {
        Effect   = "Allow",
        Action   = "s3:PutObject",
        Resource = "${aws_s3_bucket.reddit_data_new.arn}/${var.s3_key_prefix}/*" # Points to NEW bucket
      },
      {
        Effect   = "Allow",
        Action   = "bedrock:InvokeModel",
        Resource = var.bedrock_model_arn != "" ? var.bedrock_model_arn : "arn:aws:bedrock:${var.aws_region}::foundation-model/${var.bedrock_model_id}"
      }
    ]
  })
  tags = { Project = var.project_name }
}

resource "aws_iam_role_policy_attachment" "lambda_policy_attach" {
  role       = aws_iam_role.lambda_exec_role.name
  policy_arn = aws_iam_policy.lambda_policy.arn
}

# --- NEW S3 Bucket for Reddit Data ---
resource "aws_s3_bucket" "reddit_data_new" { # Renamed to avoid conflict if old state exists
  bucket = var.new_s3_bucket_name_for_data # Use the new variable

  tags = {
    Project = var.project_name
    Purpose = "Stores analyzed Reddit post data in Parquet format"
  }
}

resource "aws_s3_bucket_public_access_block" "reddit_data_new_public_access" {
  bucket                  = aws_s3_bucket.reddit_data_new.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "reddit_data_new_versioning" {
  bucket = aws_s3_bucket.reddit_data_new.id
  versioning_configuration {
    status = "Enabled"
  }
}

# --- Lambda Layer ---
data "archive_file" "lambda_layer_zip" {
  type        = "zip"
  source_dir  = var.lambda_layer_path
  output_path = "./archived_lambda_layer_${var.lambda_layer_name}.zip"
}

resource "aws_lambda_layer_version" "lambda_deps_layer" {
  layer_name          = var.lambda_layer_name
  filename            = data.archive_file.lambda_layer_zip.output_path
  source_code_hash    = data.archive_file.lambda_layer_zip.output_base64sha256
  compatible_runtimes = [var.lambda_runtime]
}

# --- Lambda Function ---
data "archive_file" "lambda_code_zip" {
  type        = "zip"
  source_dir  = var.lambda_code_path
  output_path = "./archived_lambda_code_${var.lambda_function_name}.zip"
}

resource "aws_cloudwatch_log_group" "lambda_log_group" {
  name              = "/aws/lambda/${var.lambda_function_name}"
  retention_in_days = 14
  tags              = { Project = var.project_name }
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
  layers           = [aws_lambda_layer_version.lambda_deps_layer.arn]

  environment {
    variables = {
      MY_AWS_REGION           = var.aws_region
      POST_LIMIT              = var.post_limit
      SUBREDDIT_NAME          = "aws" # Example, can be a variable
      COMMENT_LIMIT           = var.comment_limit
      MIN_COMMENTS_TO_PROCESS = var.new_comments_to_process
      NEW_POST_CHECK_LIMIT    = var.new_post_check_limit
      DYNAMODB_TABLE_NAME     = aws_dynamodb_table.processed_posts_table.name # Use resource attribute
      S3_BUCKET_NAME          = aws_s3_bucket.reddit_data_new.id              # Points to NEW bucket
      BEDROCK_MODEL_ID        = var.bedrock_model_id
      S3_KEY_PREFIX           = var.s3_key_prefix
      SECRET_NAME             = split(":", var.secrets_manager_secret_arn)[6] # Assumes ARN format
    }
  }
  tags       = { Project = var.project_name }
  depends_on = [aws_cloudwatch_log_group.lambda_log_group, aws_s3_bucket.reddit_data_new]
}

# --- DynamoDB Table ---
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
  tags = { Project = var.project_name }
}

# --- EventBridge Schedule ---
resource "aws_cloudwatch_event_rule" "lambda_weekly_scheduler" {
  for_each            = { for idx, expr in var.lambda_eventbridge_schedule_expressions : idx => expr }
  name                = "${var.project_name}-lambda-trigger-${each.key}"
  schedule_expression = each.value
  tags                = { Project = var.project_name }
}

resource "aws_cloudwatch_event_target" "lambda_target" {
  for_each  = aws_cloudwatch_event_rule.lambda_weekly_scheduler
  rule      = each.value.name
  arn       = aws_lambda_function.reddit_analyzer_lambda.arn
  target_id = "${var.lambda_function_name}-target-${each.key}"
}

resource "aws_lambda_permission" "allow_cloudwatch_to_call_lambda" {
  for_each      = aws_cloudwatch_event_rule.lambda_weekly_scheduler
  statement_id  = "AllowExecutionFromCloudWatch${each.key}"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.reddit_analyzer_lambda.function_name
  principal     = "events.amazonaws.com"
  source_arn    = each.value.arn
}

# --- Glue Data Catalog Database ---
resource "aws_glue_catalog_database" "reddit_data_db" {
  name = "${var.project_name}${var.glue_database_name_suffix}"
  tags = { Project = var.project_name }
}

# --- IAM Role and Policy for Glue Crawler ---
resource "aws_iam_role" "glue_crawler_role" {
  name = "${var.project_name}-glue-crawler-role"
  assume_role_policy = jsonencode({
    Version = "2012-10-17",
    Statement = [{
      Action = "sts:AssumeRole",
      Effect = "Allow",
      Principal = { Service = "glue.amazonaws.com" }
    }]
  })
  tags = { Project = var.project_name }
}

resource "aws_iam_policy" "glue_crawler_policy" {
  name        = "${var.project_name}-glue-crawler-policy"
  description = "IAM policy for Glue crawler"
  policy = jsonencode({
    Version = "2012-10-17",
    Statement = [
      {
        Effect   = "Allow",
        Action   = ["s3:GetObject", "s3:ListBucket"],
        Resource = [
          aws_s3_bucket.reddit_data_new.arn, # For ListBucket, points to NEW bucket
          "${aws_s3_bucket.reddit_data_new.arn}/${var.s3_key_prefix}/*" # For GetObject, points to NEW bucket
        ]
      },
      {
        Effect   = "Allow",
        Action   = "s3:GetBucketLocation",
        Resource = aws_s3_bucket.reddit_data_new.arn # Points to NEW bucket
      },
      {
        Effect = "Allow",
        Action = [
          "glue:GetDatabase", "glue:GetDatabases", "glue:CreateTable", "glue:UpdateTable",
          "glue:DeleteTable", "glue:GetTable", "glue:GetTables", "glue:GetPartition",
          "glue:GetPartitions", "glue:CreatePartition", "glue:UpdatePartition",
          "glue:DeletePartition", "glue:BatchCreatePartition", "glue:BatchUpdatePartition",
          "glue:BatchDeletePartition", "glue:BatchGetPartition"
        ],
        Resource = [
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:catalog",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:database/${aws_glue_catalog_database.reddit_data_db.name}",
          "arn:aws:glue:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:table/${aws_glue_catalog_database.reddit_data_db.name}/*"
        ]
      },
      {
        Effect   = "Allow",
        Action   = ["logs:CreateLogGroup", "logs:CreateLogStream", "logs:PutLogEvents"],
        Resource = "arn:aws:logs:${data.aws_region.current.name}:${data.aws_caller_identity.current.account_id}:log-group:/aws-glue/crawlers:*"
      }
    ]
  })
  tags = { Project = var.project_name }
}

resource "aws_iam_role_policy_attachment" "glue_crawler_policy_attach" {
  role       = aws_iam_role.glue_crawler_role.name
  policy_arn = aws_iam_policy.glue_crawler_policy.arn
}

# --- Glue Crawler ---
resource "aws_glue_crawler" "reddit_data_crawler" {
  name          = "${var.project_name}${var.glue_crawler_name_suffix}"
  database_name = aws_glue_catalog_database.reddit_data_db.name
  role          = aws_iam_role.glue_crawler_role.arn

  s3_target {
    path = "s3://${aws_s3_bucket.reddit_data_new.id}/${var.s3_key_prefix}/" # Points to NEW bucket
  }

  schedule = "cron(0 2 ? * SAT *)" # Example: Weekly Saturday 2 AM UTC

  configuration = jsonencode({
    Version = 1.0,
    CrawlerOutput = {
      Partitions = { AddOrUpdateBehavior = "InheritFromTable" }, # This is fine. Once table is created with partitions, it will inherit.
                                                                # Alternative: "Detect" might be slightly more aggressive for initial creation if it struggles.
      Tables     = { AddOrUpdateBehavior = "MergeNewColumns" }
    },
    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas" # Helps ensure one table is created if schemas are similar
    }
  })

  schema_change_policy {
    update_behavior = "UPDATE_IN_DATABASE"
    delete_behavior = "LOG" # Or "DEPRECATE_IN_DATABASE"
  }

  tags = { Project = var.project_name }
  depends_on = [
    aws_iam_role.glue_crawler_role,
    aws_glue_catalog_database.reddit_data_db,
    aws_s3_bucket.reddit_data_new # Ensure bucket exists before crawler
  ]
}

# --- S3 Bucket for Athena Query Results ---
resource "aws_s3_bucket" "athena_query_results" {
  bucket = "${lower(var.project_name)}${var.athena_results_bucket_suffix}-${data.aws_caller_identity.current.account_id}-${data.aws_region.current.name}"
  tags   = { Project = var.project_name }
}

resource "aws_s3_bucket_public_access_block" "athena_query_results_public_access" {
  bucket                  = aws_s3_bucket.athena_query_results.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_versioning" "athena_query_results_versioning" {
  bucket = aws_s3_bucket.athena_query_results.id
  versioning_configuration { status = "Enabled" }
}

# --- Athena Workgroup ---
resource "aws_athena_workgroup" "reddit_analyzer_workgroup" {
  name        = "${var.project_name}${var.athena_workgroup_name_suffix}"
  description = "Athena workgroup for Reddit analysis"
  state       = "ENABLED"
  configuration {
    result_configuration {
      output_location = "s3://${aws_s3_bucket.athena_query_results.id}/"
    }
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
  }
  tags       = { Project = var.project_name }
  depends_on = [aws_s3_bucket.athena_query_results]
}