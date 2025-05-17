# AWS Serverless Reddit Data Analysis Pipeline

This project implements a serverless data pipeline on AWS that pulls posts from a specified Reddit subreddit, summarizes them using Amazon Bedrock, stores the data in a data lake, and makes it queryable with Amazon Athena. This project serves as a learning experience and portfolio piece demonstrating various AWS services and best practices.

## Architecture

The pipeline consists of the following components, orchestrated using Terraform:

[**Insert Architecture Diagram Here - You can create this using a tool like draw.io or Lucidchart and link to it**]

1.  **EventBridge Schedule:** An EventBridge rule triggers the data ingestion process on a regular schedule (default: weekly on Fridays at 3 PM UTC).

2.  **AWS Lambda Function (redditAwsPostAnalyzer):**
    *   This Python-based Lambda function is the core of the data ingestion process.
    *   It uses the Reddit API to retrieve recent posts from a specified subreddit (default: "aws").
    *   For each post:
        *   It calls Amazon Bedrock to generate a summary of the post.
        *   It stores the post data and summary in an Amazon S3 bucket in Parquet format.
        *   To prevent duplicate processing, it stores the Reddit post ID in an Amazon DynamoDB table.

3.  **Amazon DynamoDB Table (RedditAwsAnalysis):**
    *   This table stores the IDs of processed Reddit posts.
    *   Before processing a post, the Lambda function checks if the ID exists in this table. If it does, the post is skipped.
    *   This ensures idempotency and prevents reprocessing of the same Reddit content.

4.  **Amazon S3 Bucket (reddit-data-aws):**
    *   This S3 bucket acts as the data lake, storing the raw Reddit post data and summaries in Parquet format.
    *   Data is partitioned by date (year, month, day) to optimize query performance.
    *   Example S3 path: `s3://reddit-data-aws/reddit-analysis/year=2024/month=05/day=18/data.parquet`

5.  **AWS Glue Data Catalog:**
    *   The AWS Glue Data Catalog provides metadata management and schema discovery for the data in S3.
    *   It defines a database (`reddit-aws-analyzer_reddit_data_db`) and a table (`reddit_analysis`) that describe the structure of the Parquet files in S3.

6.  **AWS Glue Crawler (reddit-aws-analyzer-reddit-data-crawler):**
    *   The Glue Crawler automatically crawls the S3 bucket, infers the schema of the Parquet files, and creates/updates the table definition in the Glue Data Catalog.
    *   It also detects and registers new partitions (based on the date-based folder structure) in the Glue Data Catalog.
    *   The crawler is scheduled to run regularly (default: weekly on Saturdays at 2 AM UTC), after the Lambda function has had a chance to ingest new data.

7.  **Amazon Athena:**
    *   Amazon Athena is a serverless query service that allows you to analyze the data in S3 using standard SQL.
    *   Athena uses the Glue Data Catalog to understand the structure of the data.
    *   Query results are stored in a separate S3 bucket (described below).

8.  **Amazon S3 Bucket for Athena Query Results (reddit-aws-analyzer-athena-results-\<account-id>-\<region>):**
    *   This S3 bucket stores the results of Athena queries.
    *   It is separate from the main data bucket to allow for different lifecycle policies and access controls.

9.  **Amazon Bedrock:**
    *   Amazon Bedrock is used to generate summaries of the Reddit posts.
    *   The Lambda function invokes a specified Bedrock model (default: Anthropic Claude 3 Haiku) to summarize the post text.

## Prerequisites

Before deploying this project, you will need:

*   An AWS account.
*   The AWS CLI installed and configured with appropriate credentials.
*   Terraform installed (version >= 1.0).
*   Python 3.9 or later installed.
*   A Reddit API client ID and secret (see instructions below).

## 1. Create Reddit API Credentials

This project requires access to the Reddit API. To obtain credentials:

1.  Create a Reddit account (if you don't already have one) and log in.
2.  Go to [https://www.reddit.com/prefs/apps](https://www.reddit.com/prefs/apps)
3.  Click the "Create App" button.
4.  Fill out the form with the following information:
    *   **Name:** A descriptive name for your app (e.g., "AWS Reddit Analyzer").
    *   **App type:** `script`
    *   **description:** A description for the app
    *   **about url:** The url of your project
    *   **redirect uri:** `http://localhost:8080` (This is a placeholder and does not need to be a real URL).
5.  Click "Create App".
6.  You will see:
    *   A **client ID** (below "personal use script") - this is the `client_id`
    *   A **secret** (next to "secret") - this is the `client_secret`

## 2. Store Reddit API Credentials in AWS Secrets Manager

1.  Go to the AWS Secrets Manager console.
2.  Click "Store a new secret".
3.  Choose "Other type of secret".
4.  Enter the following key-value pairs:
    *   `client_id`: Your Reddit API client ID.
    *   `client_secret`: Your Reddit API client secret.
    *   `user_agent`: A descriptive user agent string (e.g., "RedditAwsAnalyzer/1.0 (by /u/\<your-reddit-username>)").
5.  Choose a secret name (e.g., `reddit_api`).
6.  Configure encryption and replication as needed (the defaults are usually fine).
7.  Review and store the secret.
8.  **Note the ARN of the secret.** You will need this for the Terraform configuration.

## Terraform Deployment

1.  **Clone the repository:**
    ```bash
    git clone <your-github-repo-url>
    cd <project-directory>
    ```

2.  **Create a `terraform.tfvars` file:**
    Create a file named `terraform.tfvars` in the root of the project directory. This file will contain the values for the required variables. The file will be ignored by Git due to the `.gitignore` configuration.

    Populate the `terraform.tfvars` file with the following, replacing the placeholders with your actual values:

    ```terraform
    # terraform.tfvars

    aws_region                 = "your-desired-aws-region"       # e.g., "us-east-1", "eu-central-1"
    s3_bucket_name             = "your-globally-unique-s3-bucket-name-for-data"  # Must be globally unique.
    secrets_manager_secret_arn = "arn:aws:secretsmanager:YOUR_REGION:YOUR_ACCOUNT_ID:secret:YOUR_SECRET_NAME-XXXXXX" # Replace with the ARN of your Secrets Manager secret for Reddit API
    ```

3.  **Initialize Terraform:**
    ```bash
    terraform init
    ```

4.  **Plan the deployment:**
    ```bash
    terraform plan
    ```

5.  **Apply the deployment:**
    ```bash
    terraform apply
    ```

    Confirm the changes by typing `yes` when prompted.

6.  **Note the outputs:**
    After the deployment is complete, Terraform will output the names and ARNs of the created resources.

## Accessing and Analyzing Data

1.  **Access the Athena Console:**
    *   Go to the Amazon Athena console in the AWS Management Console.

2.  **Select the Workgroup:**
    *   Ensure that you select the workgroup `reddit-aws-analyzer-workgroup`

3.  **Select the Database:**
    *   Ensure that the database `reddit-aws-analyzer_reddit_data_db` is selected.

4.  **Query the Data:**
    *   Write and run SQL queries to analyze the Reddit data and summaries.
    *   Example:
        ```sql
        SELECT *
        FROM "reddit_analysis"
        LIMIT 10;
        ```

## Destroying the Infrastructure

To destroy all the resources created by this Terraform configuration:

```bash
terraform destroy