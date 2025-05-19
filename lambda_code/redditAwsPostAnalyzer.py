import boto3
import json
import praw
import os
import time
import io
import re
from datetime import datetime, timedelta, timezone # Keep for general timestamping
from decimal import Decimal
import pyarrow as pa
import pyarrow.parquet as pq # Corrected import name
from botocore.exceptions import ClientError
from praw.exceptions import PRAWException
from praw.models import MoreComments
import traceback

# --- Configuration ---
SECRET_NAME = "reddit_api"
REGION_NAME = os.environ.get("AWS_REGION", "eu-central-1")

# --- Environment Variables ---
try:
    POST_LIMIT = int(os.environ.get("POST_LIMIT", "5")) # How many eligible posts to process
    if POST_LIMIT <= 0: raise ValueError("POST_LIMIT must be positive")
except ValueError:
    print(f"Invalid POST_LIMIT env var. Using default 5.")
    POST_LIMIT = 5

SUBREDDIT_NAME = os.environ.get("SUBREDDIT_NAME", "aws")
try:
    COMMENT_LIMIT = int(os.environ.get("COMMENT_LIMIT", "10")) # For Bedrock analysis
    if COMMENT_LIMIT < 0: raise ValueError("COMMENT_LIMIT cannot be negative")
except ValueError:
     print(f"Invalid COMMENT_LIMIT env var. Using default 10.")
     COMMENT_LIMIT = 10

DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID")
S3_KEY_PREFIX = os.environ.get("S3_KEY_PREFIX", "reddit-analysis")
MAX_COMMENTS_FOR_PROMPT = 3

# NEW: Minimum number of comments a post must have to be processed
try:
    MIN_COMMENTS_TO_PROCESS = int(os.environ.get("MIN_COMMENTS_TO_PROCESS", "2"))
    if MIN_COMMENTS_TO_PROCESS < 0:
        raise ValueError("MIN_COMMENTS_TO_PROCESS cannot be negative.")
except ValueError as e:
    print(f"Invalid MIN_COMMENTS_TO_PROCESS env var: {e}. Using default 2 (posts with 0 or 1 comment will be skipped).")
    MIN_COMMENTS_TO_PROCESS = 2

# How many of the newest posts to check to find POST_LIMIT eligible ones
try:
    # This should be larger than POST_LIMIT to account for skipped posts
    NEW_POST_CHECK_LIMIT = int(os.environ.get("NEW_POST_CHECK_LIMIT", "50"))
    if NEW_POST_CHECK_LIMIT <= 0:
        raise ValueError("NEW_POST_CHECK_LIMIT must be positive.")
    if NEW_POST_CHECK_LIMIT < POST_LIMIT and MIN_COMMENTS_TO_PROCESS > 0:
        print(f"Warning: NEW_POST_CHECK_LIMIT ({NEW_POST_CHECK_LIMIT}) is less than POST_LIMIT ({POST_LIMIT}) "
              f"and MIN_COMMENTS_TO_PROCESS is > 0. May not find enough eligible posts.")
except ValueError as e:
    print(f"Invalid NEW_POST_CHECK_LIMIT env var: {e}. Using default 50.")
    NEW_POST_CHECK_LIMIT = 50


# --- Initialize AWS Clients ---
session = boto3.session.Session()
secrets_client = session.client(service_name='secretsmanager', region_name=REGION_NAME)
dynamodb_resource = session.resource('dynamodb', region_name=REGION_NAME)
s3_client = session.client('s3', region_name=REGION_NAME)
bedrock_runtime = session.client(service_name='bedrock-runtime', region_name=REGION_NAME)

# --- Helper Function: Get Secrets ---
def get_secret():
    # ... (Same as before)
    """Retrieves the Reddit API credentials from AWS Secrets Manager."""
    try:
        print(f"Attempting to retrieve secret: {SECRET_NAME}")
        get_secret_value_response = secrets_client.get_secret_value(SecretId=SECRET_NAME)
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            print("Secret JSON parsed successfully.")
            if 'REDDIT_CLIENT_ID' not in secret or 'REDDIT_CLIENT_SECRET' not in secret:
                 raise ValueError("Retrieved secret is missing REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET keys.")
            return secret
        else:
            raise ValueError("SecretString not found in Secrets Manager response")
    except ClientError as e:
        print(f"Secrets Manager ClientError retrieving secret: {e}")
        raise e
    except (json.JSONDecodeError, ValueError) as e:
        print(f"Error parsing secret JSON or validating content: {e}")
        raise ValueError(f"Secret validation failed: {e}") from e

# --- Helper Function: Build Bedrock Prompt ---
def build_bedrock_prompt(title, selftext, top_comments_list):
    # ... (Same as before)
    comments_section = f"Top Comments Provided (up to {MAX_COMMENTS_FOR_PROMPT}):\n"
    if top_comments_list:
        for i, comment_body in enumerate(top_comments_list):
            truncated_comment = (comment_body[:1000] + '...') if len(comment_body) > 1000 else comment_body
            escaped_comment = json.dumps(truncated_comment)[1:-1]
            comments_section += f"Comment {i+1}:\n{escaped_comment}\n---\n"
    else:
        comments_section += "No relevant comments provided or fetched.\n" # Should not happen if MIN_COMMENTS_TO_PROCESS > 0
    combined_text = f"Original Post Title: {title}\n\nOriginal Post Body:\n{selftext}\n\n{comments_section}"
    prompt = f"""Human: Analyze the following Reddit thread (original post and top comments) about AWS, focusing on explaining the concepts for learning purposes.

1.  **Problem Identification & Summary:** Identify the core technical problem or question from the original post. Provide a concise summary (1-2 sentences) as 'problem_summary'.
2.  **Problem Explanation:** Explain the summarized problem ('problem_summary') in simpler terms, assuming the reader may not be familiar with all concepts. Define key technical jargon or AWS services mentioned in the *original post*. Briefly explain *why* this problem might occur or why it's significant. Aim for clarity and conciseness (e.g., 2-4 sentences). Store this as 'problem_explanation'.
3.  **Solution Identification & Summary:** Analyze the provided top comments. Concisely summarize the main potential solutions, suggestions, or key advice offered as 'solution_summary'. If no relevant solutions are found, state "No specific solutions offered in provided comments."
4.  **Solution Explanation:** Explain the summarized solutions ('solution_summary') in simpler terms. Define key technical terms or AWS services mentioned *in the comments*. Briefly explain the *reasoning* behind the suggestions if possible. Aim for clarity and conciseness (e.g., 2-4 sentences per major solution point). Store this as 'solution_explanation'.
5.  **Categorization:** Identify the **top 3 most relevant** AWS service categories or technical concepts involved in the entire thread (problem and solutions). Choose from the example list or add other highly relevant technical terms (like 'Idempotency', 'Networking Concepts', etc.). Store this as 'suggested_categories'.

**Output Format:** Format the output strictly as a single, valid JSON object starting with `{{` and ending with `}}`.
*   Use only the keys "problem_summary", "problem_explanation", "solution_summary", "solution_explanation", and "suggested_categories".
*   **Crucially: ALL string values associated with these keys MUST be enclosed in double quotes (`"`).** For example: `"problem_summary": "This is the summary text."`.
*   The value for "suggested_categories" MUST be a JSON list of strings, like `["EC2", "S3", "Lambda"]`, containing a maximum of 3 strings.
*   Ensure proper JSON syntax (commas between key-value pairs, etc.). Do not include any text before the opening `{{` or after the closing `}}`.

**Example Categories:** EC2, S3, Lambda, Networking, Security, Cost Optimization, Migration, Serverless, Databases, IAM, Containers, CI/CD, IaC, Monitoring, API Gateway, Route 53, CloudFront, Other.

Reddit Thread Text:
<thread>
{combined_text}
</thread>

Assistant:"""
    return prompt

# --- Helper Function: Invoke Bedrock ---
def invoke_bedrock(prompt):
    # ... (Same as before)
    if not BEDROCK_MODEL_ID:
        raise ValueError("BEDROCK_MODEL_ID environment variable not set.")
    analysis_text, sanitized_json_str, analysis_json = None, None, None
    try:
        messages = [{"role": "user", "content": prompt.replace("Human:","").replace("Assistant:","").strip()}]
        body_payload = {"messages": messages, "max_tokens": 1000, "temperature": 0.2, "top_p": 0.9, "anthropic_version": "bedrock-2023-05-31"}
        body = json.dumps(body_payload)
        print(f"Invoking Bedrock model: {BEDROCK_MODEL_ID}")
        response = bedrock_runtime.invoke_model(body=body, modelId=BEDROCK_MODEL_ID, accept='application/json', contentType='application/json')
        response_body = json.loads(response['body'].read().decode('utf-8'))
        print("Bedrock response received.")
        if 'content' in response_body and len(response_body['content']) > 0: analysis_text = response_body['content'][0].get('text')
        else: analysis_text = None
        if not analysis_text:
             print(f"Bedrock response body for debugging: {response_body}")
             raise ValueError("Bedrock response did not contain expected text field.")
        match = re.search(r'\{.*\}', analysis_text, re.DOTALL)
        if match:
            analysis_json_str = match.group(0)
            sanitized_json_str = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', analysis_json_str)
            print("Extracted potential JSON block using regex.")
        else:
            print(f"Could not find JSON block boundaries using regex in Bedrock text: {analysis_text}")
            return {"problem_summary": "No JSON object found via regex", "problem_explanation": "N/A", "solution_summary": "No JSON object found", "solution_explanation": "N/A", "suggested_categories": ["Error", "No JSON Found"]}
        print(f"Attempting to parse sanitized JSON string (strict=False):\n{repr(sanitized_json_str)}\n")
        try:
            analysis_json = json.loads(sanitized_json_str, strict=False)
            print("Parsed JSON from Bedrock response (strict=False).")
        except json.JSONDecodeError as e:
            print(f"CRITICAL: Error decoding JSON even with strict=False: {e}")
            context_str = f"...{sanitized_json_str[max(0,e.pos-40):e.pos]}>>>ERROR (char {e.pos}) '{sanitized_json_str[e.pos] if e.pos < len(sanitized_json_str) else ''}'<<<{sanitized_json_str[e.pos+1:min(len(sanitized_json_str),e.pos+40)]}..."
            print(f"Context around error position {e.pos}:\n{context_str}")
            return {"problem_summary": f"Fatal JSON parsing error: {e}", "problem_explanation": "N/A", "solution_summary": f"Fatal JSON parsing error: {e}", "solution_explanation": "N/A", "suggested_categories": ["Error", "Fatal Parsing Failed"]}
        required_keys = ["problem_summary", "problem_explanation", "solution_summary", "solution_explanation", "suggested_categories"]
        missing_keys = [key for key in required_keys if key not in analysis_json]
        if missing_keys:
            print(f"Warning: Bedrock response JSON missing keys: {missing_keys}. Got: {analysis_json.keys()}. Filling defaults.")
            for key_to_default in required_keys: 
                if key_to_default not in analysis_json:
                    analysis_json[key_to_default] = "N/A" if key_to_default != "suggested_categories" else []
        if not isinstance(analysis_json.get("suggested_categories"), list):
            print("Warning: 'suggested_categories' is not a list. Wrapping it.")
            analysis_json["suggested_categories"] = [str(analysis_json.get("suggested_categories", "Error"))]
        return analysis_json
    except ClientError as e: print(f"Bedrock API ClientError: {e}"); raise e
    except Exception as e: print(f"Unexpected error during Bedrock: {type(e).__name__} - {e}"); raise e

# --- Main Lambda Handler ---
def lambda_handler(event, context):
    start_time = time.time()
    current_execution_time_utc = datetime.now(timezone.utc) # For logging and analysis timestamp
    print(f"Lambda execution started. 'now_utc': {current_execution_time_utc.isoformat()}")

    if not DYNAMODB_TABLE_NAME or not S3_BUCKET_NAME or not BEDROCK_MODEL_ID:
        error_msg = "Missing required env variables: DYNAMODB_TABLE_NAME, S3_BUCKET_NAME, BEDROCK_MODEL_ID must be set."
        print(f"CRITICAL ERROR: {error_msg}")
        return {'statusCode': 500, 'body': json.dumps({'error': error_msg})}

    processed_count = 0
    skipped_due_to_low_comments = 0
    skipped_duplicates = 0 # Renamed for clarity
    failed_posts = []
    eligible_posts_to_process = []
    # dynamodb_table initialized after PRAW

    try:
        reddit_credentials = get_secret()
        client_id = reddit_credentials['REDDIT_CLIENT_ID']
        client_secret = reddit_credentials['REDDIT_CLIENT_SECRET']
        print("Reddit credentials obtained securely.")

        user_agent = f'AwsAnalysisBot/0.9.5 by YourRedditUsername' # User agent version
        reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
        dynamodb_table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
        print(f"PRAW initialized. Targetting r/{SUBREDDIT_NAME}")

        # --- MODIFIED: Fetch Newest Posts and Filter by Comment Count ---
        subreddit = reddit.subreddit(SUBREDDIT_NAME)

        print(f"\n--- Post Fetching Logic ---")
        print(f"MIN_COMMENTS_TO_PROCESS: {MIN_COMMENTS_TO_PROCESS}")
        print(f"NEW_POST_CHECK_LIMIT: {NEW_POST_CHECK_LIMIT}")
        print(f"POST_LIMIT (target eligible posts): {POST_LIMIT}")

        posts_checked_count = 0
        print(f"Fetching up to {NEW_POST_CHECK_LIMIT} newest posts from r/{SUBREDDIT_NAME} to filter by comment count...")
        
        try:
            for post in subreddit.new(limit=NEW_POST_CHECK_LIMIT):
                posts_checked_count += 1
                post_created_dt = datetime.fromtimestamp(post.created_utc, timezone.utc)

                if post.num_comments >= MIN_COMMENTS_TO_PROCESS:
                    eligible_posts_to_process.append(post)
                    print(f"  Eligible (enough comments): Post ID {post.id}, Comments: {post.num_comments}, Created: {post_created_dt.isoformat()}")
                    if len(eligible_posts_to_process) >= POST_LIMIT:
                        print(f"Collected {POST_LIMIT} eligible posts with enough comments. Stopping fetch.")
                        break 
                else:
                    # print(f"  Skipped (low comments): Post ID {post.id}, Comments: {post.num_comments}, Created: {post_created_dt.isoformat()}")
                    skipped_due_to_low_comments += 1
                
                if posts_checked_count % 20 == 0:
                    print(f"  ...checked {posts_checked_count} posts, found {len(eligible_posts_to_process)} eligible, skipped {skipped_due_to_low_comments} for low comments.")

            print(f"Finished scanning {posts_checked_count} newest posts. Found {len(eligible_posts_to_process)} eligible. Skipped {skipped_due_to_low_comments} for low comments.")

        except PRAWException as e:
            print(f"PRAWException during subreddit.new() fetch: {e}")
            if not eligible_posts_to_process:
                 raise ValueError(f"Reddit fetch from .new() failed before finding any posts: {e}") from e
            print(f"Continuing with {len(eligible_posts_to_process)} posts found before PRAWException.")
        except Exception as e:
            print(f"Unexpected error during subreddit.new() fetch: {type(e).__name__} - {e}")
            traceback.print_exc()
            if not eligible_posts_to_process:
                raise
            print(f"Continuing with {len(eligible_posts_to_process)} posts found before unexpected error.")

        if not eligible_posts_to_process:
            summary_message = (f"No posts found with at least {MIN_COMMENTS_TO_PROCESS} comments "
                               f"after checking the {posts_checked_count} newest posts from r/{SUBREDDIT_NAME}.")
            print(summary_message)
            return {'statusCode': 200, 'body': json.dumps({'message': summary_message, 
                                                            'processed_count': 0, 
                                                            'skipped_duplicates': 0, 
                                                            'skipped_low_comments': skipped_due_to_low_comments,
                                                            'failed_posts_count': 0})}
        # --- END MODIFIED Fetch Logic ---

        print(f"\nProceeding to process {len(eligible_posts_to_process)} posts.")
        for post in eligible_posts_to_process:
            post_id = post.id
            post_created_dt_utc = datetime.fromtimestamp(post.created_utc, timezone.utc)
            print(f"\n--- Checking Post: {post_id} (Comments: {post.num_comments}, Created: {post_created_dt_utc.isoformat()}) | Title: {post.title[:60]}... ---")

            try:
                response = dynamodb_table.get_item(Key={'PostID': post_id}, ConsistentRead=True)
                if 'Item' in response:
                    print(f"Post {post_id} already processed (found in DynamoDB). Skipping.")
                    skipped_duplicates += 1
                    continue
                else:
                    print(f"Post {post_id} is new. Proceeding with processing...")
            except ClientError as e:
                print(f"ERROR checking DynamoDB for post {post_id}: {e.response['Error']['Code']} - {e.response['Error']['Message']}. Skipping post.")
                failed_posts.append({'id': post_id, 'title': post.title[:50] if hasattr(post, 'title') else 'N/A', 'reason': f'DynamoDB check failed: {e.response["Error"]["Code"]}'})
                continue

            try:
                print(f"Fetching up to {COMMENT_LIMIT} comments for {post_id} for Bedrock prompt...")
                top_comments_for_prompt = []
                all_fetched_comments_text_list = [] # Still useful for Parquet context
                comments_fetched_count = 0
                post.comment_sort = 'top' # Ensure we get top comments
                post.comments.replace_more(limit=0) # Resolve MoreComments objects
                
                # Iterate through comments to build prompt and full list
                for top_level_comment in post.comments.list():
                    if isinstance(top_level_comment, MoreComments):
                        continue # Should be resolved by replace_more, but good practice

                    comment_body = top_level_comment.body if top_level_comment.body else ""
                    
                    # Add to the list for Bedrock prompt (up to MAX_COMMENTS_FOR_PROMPT)
                    if comments_fetched_count < MAX_COMMENTS_FOR_PROMPT:
                        top_comments_for_prompt.append(comment_body)
                    
                    # Add to the list for Parquet storage (up to COMMENT_LIMIT)
                    if comments_fetched_count < COMMENT_LIMIT:
                         all_fetched_comments_text_list.append(f"- {comment_body}")
                    
                    comments_fetched_count += 1
                    if comments_fetched_count >= COMMENT_LIMIT and comments_fetched_count >= MAX_COMMENTS_FOR_PROMPT:
                        break # Stop if we have enough for both

                comments_text_concatenated = "\n".join(all_fetched_comments_text_list)
                print(f"Fetched {comments_fetched_count} comments. Using top {len(top_comments_for_prompt)} for Bedrock prompt.")

                print(f"Preparing prompt and invoking Bedrock...")
                prompt = build_bedrock_prompt(post.title, post.selftext, top_comments_for_prompt)
                analysis_result = invoke_bedrock(prompt)

                if "Fatal Parsing Failed" in analysis_result.get("suggested_categories", []) or \
                   "No JSON Found" in analysis_result.get("suggested_categories", []):
                     raise ValueError(f"Bedrock analysis or parsing failed for post {post_id}: {analysis_result.get('problem_summary', 'Unknown parsing error')}")

                current_analysis_time_obj = datetime.now(timezone.utc)
                data_to_store = {
                    'PostID': post.id, 'Subreddit': post.subreddit.display_name,
                    'Title': post.title, 'Selftext': post.selftext if post.selftext else '',
                    'URL': post.url, 'Author': post.author.name if post.author else '[deleted]',
                    'Score': int(post.score), 
                    'OriginalCommentCount': int(post.num_comments), # Reddit's count at time of fetch
                    'FetchedCommentCountForAnalysis': comments_fetched_count, # How many we processed for Parquet/Bedrock
                    'PostTimestampUTC': post_created_dt_utc.isoformat(),
                    'FetchedCommentsText': comments_text_concatenated,
                    'ProblemSummary': analysis_result.get("problem_summary", "N/A"),
                    'SolutionSummary': analysis_result.get("solution_summary", "N/A"),
                    'ProblemExplanation': analysis_result.get("problem_explanation", "N/A"),
                    'SolutionExplanation': analysis_result.get("solution_explanation", "N/A"),
                    'SuggestedCategories': analysis_result.get("suggested_categories", []),
                    'AnalysisTimestampUTC': current_analysis_time_obj.isoformat(),
                    'BedrockModelID': BEDROCK_MODEL_ID
                }

                print(f"Converting data to Parquet for post {post_id}...")
                table = pa.Table.from_pylist([data_to_store])
                buffer = io.BytesIO()
                pq.write_table(table, buffer, compression='snappy')
                buffer.seek(0)
                s3_key = f"{S3_KEY_PREFIX}/year={current_analysis_time_obj.year:04d}/month={current_analysis_time_obj.month:02d}/day={current_analysis_time_obj.day:02d}/{post.id}.parquet"
                print(f"Writing Parquet file to s3://{S3_BUCKET_NAME}/{s3_key}")
                s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=buffer.getvalue())
                print("Successfully wrote Parquet file to S3.")

                print(f"Marking post {post_id} as processed in DynamoDB...")
                dynamodb_table.put_item(
                    Item={'PostID': post.id,'ProcessedTimestampUTC': current_analysis_time_obj.isoformat()}
                )
                print("Successfully marked post in DynamoDB.")
                processed_count += 1

            except (ClientError, PRAWException, ValueError, TypeError, pa.ArrowException, Exception) as e:
                 error_type = type(e).__name__
                 error_message = str(e)
                 print(f"ERROR processing post {post_id}: {error_type} - {error_message}")
                 traceback.print_exc()
                 failed_posts.append({'id': post_id, 'title': post.title[:50] if hasattr(post, 'title') else 'N/A', 'reason': f'{error_type}: {error_message[:200]}'})
                 continue
        # --- End of Post Processing Loop ---

        end_time = time.time()
        duration = end_time - start_time
        summary_message = (
            f"Lambda execution finished. Duration: {duration:.2f}s. "
            f"Checked {posts_checked_count} newest posts. "
            f"Found {len(eligible_posts_to_process)} posts with >= {MIN_COMMENTS_TO_PROCESS} comments. "
            f"Skipped {skipped_due_to_low_comments} for low comments. "
            f"Targeted {POST_LIMIT} for processing. Actually Processed (New): {processed_count}, "
            f"Skipped (Duplicates): {skipped_duplicates}, Failed: {len(failed_posts)}."
        )
        print(summary_message)
        response_body = {
            'message': summary_message, 
            'processed_count': processed_count, 
            'skipped_duplicates': skipped_duplicates,
            'skipped_low_comments': skipped_due_to_low_comments,
            'failed_posts_count': len(failed_posts)
        }
        if failed_posts:
             print(f"Failed Post Details: {failed_posts}")
             response_body['failed_posts'] = failed_posts

        return {
            'statusCode': 200 if not failed_posts else 207,
            'body': json.dumps(response_body)
        }

    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        print(f"CRITICAL UNEXPECTED ERROR in handler: {error_type} - {error_message}")
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Critical handler error: {error_type} - {error_message}"})
        }