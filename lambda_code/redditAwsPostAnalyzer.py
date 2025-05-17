import boto3
import json
import praw # Reddit API Wrapper
import os
import time
import io # For handling in-memory bytes buffer for Parquet
import re # Import regular expressions for sanitization AND extraction
from datetime import datetime
from decimal import Decimal # For DynamoDB/Parquet compatibility with numbers
import pyarrow as pa # Apache Arrow for Parquet
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from praw.exceptions import PRAWException
from praw.models import MoreComments
import traceback # For detailed error logging if needed

# --- Configuration ---
SECRET_NAME = "reddit_api"
REGION_NAME = os.environ.get("AWS_REGION", "eu-central-1")

# --- Environment Variables ---
try:
    POST_LIMIT = int(os.environ.get("POST_LIMIT", "5"))
    if POST_LIMIT <= 0: raise ValueError("POST_LIMIT must be positive")
except ValueError:
    print(f"Invalid POST_LIMIT env var. Using default 5.")
    POST_LIMIT = 5

SUBREDDIT_NAME = os.environ.get("SUBREDDIT_NAME", "aws")
try:
    COMMENT_LIMIT = int(os.environ.get("COMMENT_LIMIT", "10"))
    if COMMENT_LIMIT < 0: raise ValueError("COMMENT_LIMIT cannot be negative")
except ValueError:
     print(f"Invalid COMMENT_LIMIT env var. Using default 10.")
     COMMENT_LIMIT = 10

DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID")
S3_KEY_PREFIX = os.environ.get("S3_KEY_PREFIX", "reddit-analysis")
MAX_COMMENTS_FOR_PROMPT = 3

# --- Initialize AWS Clients ---
session = boto3.session.Session()
secrets_client = session.client(service_name='secretsmanager', region_name=REGION_NAME)
dynamodb_resource = session.resource('dynamodb', region_name=REGION_NAME)
s3_client = session.client('s3', region_name=REGION_NAME)
bedrock_runtime = session.client(service_name='bedrock-runtime', region_name=REGION_NAME)

# --- Helper Function: Get Secrets ---
def get_secret():
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

# --- Helper Function: Build Bedrock Prompt (REVISED AGAIN FOR STRICTER JSON) ---
def build_bedrock_prompt(title, selftext, top_comments_list):
    """Builds the prompt asking for problem, solution summary, and categories, emphasizing JSON validity."""
    comments_section = f"Top Comments Provided (up to {MAX_COMMENTS_FOR_PROMPT}):\n"
    if top_comments_list:
        for i, comment_body in enumerate(top_comments_list):
            truncated_comment = (comment_body[:1000] + '...') if len(comment_body) > 1000 else comment_body
            # Basic escaping of potential JSON-breaking characters within comments for the prompt context
            escaped_comment = json.dumps(truncated_comment)[1:-1] # Use json.dumps to escape, then remove outer quotes
            comments_section += f"Comment {i+1}:\n{escaped_comment}\n---\n"
    else:
        comments_section += "No relevant comments provided or fetched.\n"
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

# --- Helper Function: Invoke Bedrock (REVISED with Regex Extraction & Pre-Parse Logging) ---
def invoke_bedrock(prompt):
    """Invokes the Bedrock model and parses the JSON response with strict=False and enhanced error logging."""
    if not BEDROCK_MODEL_ID:
        raise ValueError("BEDROCK_MODEL_ID environment variable not set.")

    analysis_text = None # Initialize
    sanitized_json_str = None # Initialize
    analysis_json = None # Initialize

    try:
        # --- Payload and Bedrock Call ---
        messages = [{"role": "user", "content": prompt.replace("Human:","").replace("Assistant:","").strip()}]
        body_payload = {
            "messages": messages, "max_tokens": 1000, "temperature": 0.2,
            "top_p": 0.9, "anthropic_version": "bedrock-2023-05-31"
        }
        body = json.dumps(body_payload)
        print(f"Invoking Bedrock model: {BEDROCK_MODEL_ID}")
        response = bedrock_runtime.invoke_model(
            body=body, modelId=BEDROCK_MODEL_ID,
            accept='application/json', contentType='application/json'
        )
        response_body = json.loads(response['body'].read().decode('utf-8'))
        print("Bedrock response received.")

        # --- Parse Response Structure ---
        if 'content' in response_body and len(response_body['content']) > 0:
            analysis_text = response_body['content'][0].get('text')
        else: analysis_text = None

        if not analysis_text:
             print(f"Bedrock response body for debugging: {response_body}")
             raise ValueError("Bedrock response did not contain expected text field.")

        # --- Extract JSON using Regex (more robust) and Sanitize ---
        # Match the first '{' and the last '}' including everything in between
        match = re.search(r'\{.*\}', analysis_text, re.DOTALL)
        if match:
            analysis_json_str = match.group(0)
            sanitized_json_str = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', analysis_json_str)
            print("Extracted potential JSON block using regex.")
        else:
            print(f"Could not find JSON block boundaries using regex in Bedrock text: {analysis_text}")
            return { # Return error structure
                "problem_summary": "No JSON object found via regex in Bedrock response",
                "problem_explanation": "N/A",
                "solution_summary": "No JSON object found via regex in Bedrock response",
                "solution_explanation": "N/A",
                "suggested_categories": ["Error", "No JSON Found"]
            }

        # --- Log the exact string BEFORE attempting to parse ---
        print(f"Attempting to parse sanitized JSON string (strict=False):\n{repr(sanitized_json_str)}\n")

        # --- Attempt to Parse JSON with strict=False ---
        try:
            analysis_json = json.loads(sanitized_json_str, strict=False)
            print("Parsed JSON from Bedrock response (strict=False).")

        # --- Catch parsing error ---
        except json.JSONDecodeError as e:
            print(f"CRITICAL: Error decoding JSON even with strict=False: {e}")
            print(f"Error details: Line {e.lineno}, Column {e.colno}, Position {e.pos}")
            context_window = 40
            start_context = max(0, e.pos - context_window)
            end_context = min(len(sanitized_json_str), e.pos + context_window)
            if e.pos < len(sanitized_json_str):
                context_str = f"...{sanitized_json_str[start_context:e.pos]}>>>ERROR (char {e.pos}) '{sanitized_json_str[e.pos]}'<<<{sanitized_json_str[e.pos+1:end_context]}..."
            else:
                context_str = f"...{sanitized_json_str[start_context:e.pos]}>>>ERROR (char {e.pos})<<<"
            print(f"Context around error position {e.pos}:\n{context_str}")
            print(f"\nFull string passed to json.loads(strict=False) that failed:\n{repr(sanitized_json_str)}\n")
            return { # Return structured error
                "problem_summary": f"Fatal JSON parsing error: {e}",
                "problem_explanation": "N/A",
                "solution_summary": f"Fatal JSON parsing error: {e}",
                "solution_explanation": "N/A",
                "suggested_categories": ["Error", "Fatal Parsing Failed"]
            }

        # --- Validate required keys ---
        required_keys = ["problem_summary", "problem_explanation", "solution_summary", "solution_explanation", "suggested_categories"]
        missing_keys = [key for key in required_keys if key not in analysis_json]
        if missing_keys:
            print(f"Warning: Bedrock response JSON missing expected keys: {missing_keys}. Got: {analysis_json.keys()}. Filling defaults.")
            analysis_json.setdefault("problem_summary", "N/A")
            analysis_json.setdefault("problem_explanation", "N/A")
            analysis_json.setdefault("solution_summary", "N/A")
            analysis_json.setdefault("solution_explanation", "N/A")
            analysis_json.setdefault("suggested_categories", [])

        if not isinstance(analysis_json.get("suggested_categories"), list):
            print("Warning: 'suggested_categories' is not a list. Wrapping it.")
            analysis_json["suggested_categories"] = [str(analysis_json.get("suggested_categories", "Error"))]

        return analysis_json

    except ClientError as e:
        print(f"Bedrock API ClientError: {e}")
        raise e
    except Exception as e:
        print(f"Unexpected error during Bedrock invocation: {type(e).__name__} - {e}")
        # print(traceback.format_exc())
        raise e

# --- Main Lambda Handler ---
def lambda_handler(event, context):
    """
    Main handler: Fetches Reddit posts, checks duplicates, analyzes via Bedrock, stores Parquet in S3.
    """
    start_time = time.time()
    print(f"Lambda execution started at {datetime.utcnow().isoformat()}Z")

    # --- Validate required environment variables ---
    if not DYNAMODB_TABLE_NAME or not S3_BUCKET_NAME or not BEDROCK_MODEL_ID:
        error_msg = "Missing required environment variables: DYNAMODB_TABLE_NAME, S3_BUCKET_NAME, BEDROCK_MODEL_ID must be set."
        print(f"CRITICAL ERROR: {error_msg}")
        return {'statusCode': 500, 'body': json.dumps({'error': error_msg})}

    # --- Initialization ---
    processed_count = 0
    skipped_count = 0
    failed_posts = []
    dynamodb_table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)

    try:
        # 1. Retrieve Reddit API Credentials (Securely)
        reddit_credentials = get_secret()
        client_id = reddit_credentials['REDDIT_CLIENT_ID']
        client_secret = reddit_credentials['REDDIT_CLIENT_SECRET']
        print("Reddit credentials obtained securely.")

        # 2. Initialize PRAW Client
        # !!! IMPORTANT: Change 'YourRedditUsername' !!!
        user_agent = f'AwsAnalysisBot/0.9 by YourRedditUsername' # Increment version
        reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
        print(f"PRAW initialized (Read-only: {reddit.read_only}). Fetching from r/{SUBREDDIT_NAME}")

        # 3. Fetch New Posts from Reddit
        print(f"Fetching up to {POST_LIMIT} new posts from r/{SUBREDDIT_NAME}...")
        subreddit = reddit.subreddit(SUBREDDIT_NAME)
        new_posts = list(subreddit.new(limit=POST_LIMIT))
        print(f"Found {len(new_posts)} posts to check.")

        # 4. Process Each Post
        for post in new_posts:
            post_id = post.id
            print(f"\n--- Checking Post: {post_id} | Title: {post.title[:80]}... ---")

            # 4a. Deduplication Check against DynamoDB
            try:
                response = dynamodb_table.get_item(Key={'PostID': post_id}, ConsistentRead=True)
                if 'Item' in response:
                    print(f"Post {post_id} already processed (found in DynamoDB). Skipping.")
                    skipped_count += 1
                    continue
                else:
                    print(f"Post {post_id} is new. Proceeding with processing...")
            except ClientError as e:
                print(f"ERROR checking DynamoDB for post {post_id}: {e.response['Error']['Code']} - {e.response['Error']['Message']}. Skipping post.")
                failed_posts.append({'id': post_id, 'reason': f'DynamoDB check failed: {e.response["Error"]["Code"]}'})
                continue

            # --- Start processing logic for NEW post ---
            try:
                # 4b. Fetch Top Comments
                print(f"Fetching up to {COMMENT_LIMIT} comments for {post_id}...")
                top_comments_for_prompt = []
                all_fetched_comments_text_list = []
                comments_fetched_count = 0
                post.comment_sort = 'top'
                post.comments.replace_more(limit=0) # Fetch comments
                for top_level_comment in post.comments.list():
                    if isinstance(top_level_comment, MoreComments): continue
                    if comments_fetched_count < COMMENT_LIMIT:
                        comment_body = top_level_comment.body if top_level_comment.body else ""
                        all_fetched_comments_text_list.append(f"- {comment_body}")
                        if comments_fetched_count < MAX_COMMENTS_FOR_PROMPT:
                            top_comments_for_prompt.append(comment_body)
                        comments_fetched_count += 1
                    else: break
                comments_text_concatenated = "\n".join(all_fetched_comments_text_list)
                print(f"Fetched {comments_fetched_count} comments. Using top {len(top_comments_for_prompt)} for solution analysis prompt.")

                # 4c. Invoke Bedrock for Analysis
                print(f"Preparing prompt and invoking Bedrock...")
                prompt = build_bedrock_prompt(post.title, post.selftext, top_comments_for_prompt)
                analysis_result = invoke_bedrock(prompt) # Uses strict=False internally

                # Check if Bedrock invocation/parsing itself reported an error structure
                if "Fatal Parsing Failed" in analysis_result.get("suggested_categories", []) or \
                   "No JSON Found" in analysis_result.get("suggested_categories", []):
                     # Log the specific parsing error reported by the helper function
                     raise ValueError(f"Bedrock analysis or parsing failed for post {post_id}: {analysis_result.get('problem_summary', 'Unknown parsing error')}")

                # 4d. Prepare Data Structure for Parquet
                current_utc_time = datetime.utcnow()
                data_to_store = {
                    'PostID': post.id, 'Subreddit': post.subreddit.display_name,
                    'Title': post.title, 'Selftext': post.selftext if post.selftext else '',
                    'URL': post.url, 'Author': post.author.name if post.author else '[deleted]',
                    'Score': int(post.score), 'CommentCount': int(post.num_comments),
                    'PostTimestampUTC': datetime.utcfromtimestamp(post.created_utc).isoformat() + 'Z',
                    'FetchedCommentsText': comments_text_concatenated,
                    'ProblemSummary': analysis_result.get("problem_summary", "N/A"),
                    'SolutionSummary': analysis_result.get("solution_summary", "N/A"),
                    'ProblemExplanation': analysis_result.get("problem_explanation", "N/A"), # Added key
                    'SolutionExplanation': analysis_result.get("solution_explanation", "N/A"), # Added key
                    'SuggestedCategories': analysis_result.get("suggested_categories", []),
                    'AnalysisTimestampUTC': current_utc_time.isoformat() + 'Z',
                    'BedrockModelID': BEDROCK_MODEL_ID
                }

                # 4e. Convert to Parquet Format & Write to S3
                print(f"Converting data to Parquet for post {post_id}...")
                table = pa.Table.from_pylist([data_to_store])
                buffer = io.BytesIO()
                pq.write_table(table, buffer, compression='snappy')
                buffer.seek(0)
                s3_key = f"{S3_KEY_PREFIX}/year={current_utc_time.year:04d}/month={current_utc_time.month:02d}/day={current_utc_time.day:02d}/{post.id}.parquet"
                print(f"Writing Parquet file to s3://{S3_BUCKET_NAME}/{s3_key}")
                s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=buffer.getvalue())
                print("Successfully wrote Parquet file to S3.")

                # 4f. Mark Post as Processed in DynamoDB (Critical: Only after S3 success)
                print(f"Marking post {post_id} as processed in DynamoDB...")
                dynamodb_table.put_item(
                    Item={'PostID': post.id,'ProcessedTimestampUTC': current_utc_time.isoformat() + 'Z'}
                )
                print("Successfully marked post in DynamoDB.")
                processed_count += 1

            # --- Catch errors specific to processing THIS post ---
            except (ClientError, PRAWException, ValueError, TypeError, pa.ArrowException, Exception) as e:
                 error_type = type(e).__name__
                 error_message = str(e)
                 print(f"ERROR processing post {post_id}: {error_type} - {error_message}")
                 # print(traceback.format_exc()) # Uncomment for traceback
                 failed_posts.append({'id': post_id, 'reason': f'{error_type}: {error_message[:200]}'})
                 continue

        # --- End of Post Processing Loop ---

        # 5. Log Summary and Construct Final Response
        end_time = time.time()
        duration = end_time - start_time
        summary_message = (
            f"Lambda execution finished. Duration: {duration:.2f}s. "
            f"Checked: {len(new_posts)}, Processed (New): {processed_count}, "
            f"Skipped (Duplicates): {skipped_count}, Failed: {len(failed_posts)}."
        )
        print(summary_message)
        response_body = {'message': summary_message}
        if failed_posts:
             print(f"Failed Post Details: {failed_posts}")
             response_body['failed_posts'] = failed_posts

        return {
            'statusCode': 200 if not failed_posts else 207,
            'body': json.dumps(response_body)
        }

    # --- Catch critical errors outside the loop ---
    except Exception as e:
        error_type = type(e).__name__
        error_message = str(e)
        print(f"CRITICAL UNEXPECTED ERROR in handler: {error_type} - {error_message}")
        print(traceback.format_exc()) # Print traceback for critical errors
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Critical handler error: {error_type} - {error_message}"})
        }