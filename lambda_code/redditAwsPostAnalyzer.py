import boto3
import json
import praw
import os
import time
import io
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal # Not strictly used now, but good to have if needed later
import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from praw.exceptions import PRAWException
from praw.models import MoreComments
import traceback

# --- Configuration ---
SECRET_NAME = os.environ.get("SECRET_NAME_ENV_VAR", "reddit_api") # Allow override via env for SECRET_NAME
REGION_NAME = os.environ.get("AWS_REGION", "eu-central-1")

# --- Environment Variables ---
try:
    POST_LIMIT = int(os.environ.get("POST_LIMIT", "5"))
    if POST_LIMIT <= 0: raise ValueError("POST_LIMIT must be positive")
except (ValueError, TypeError):
    print(f"Invalid POST_LIMIT env var '{os.environ.get('POST_LIMIT')}'. Using default 5.")
    POST_LIMIT = 5

SUBREDDIT_NAME = os.environ.get("SUBREDDIT_NAME", "aws")
try:
    COMMENT_LIMIT = int(os.environ.get("COMMENT_LIMIT", "10"))
    if COMMENT_LIMIT < 0: raise ValueError("COMMENT_LIMIT cannot be negative")
except (ValueError, TypeError):
     print(f"Invalid COMMENT_LIMIT env var '{os.environ.get('COMMENT_LIMIT')}'. Using default 10.")
     COMMENT_LIMIT = 10

DYNAMODB_TABLE_NAME = os.environ.get("DYNAMODB_TABLE_NAME")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")
BEDROCK_MODEL_ID = os.environ.get("BEDROCK_MODEL_ID")
S3_KEY_PREFIX = os.environ.get("S3_KEY_PREFIX", "reddit-analysis")
MAX_COMMENTS_FOR_PROMPT = 3 # Max comments to include in Bedrock prompt

try:
    MIN_COMMENTS_TO_PROCESS = int(os.environ.get("MIN_COMMENTS_TO_PROCESS", "2"))
    if MIN_COMMENTS_TO_PROCESS < 0:
        raise ValueError("MIN_COMMENTS_TO_PROCESS cannot be negative.")
except (ValueError, TypeError) as e:
    print(f"Invalid MIN_COMMENTS_TO_PROCESS env var '{os.environ.get('MIN_COMMENTS_TO_PROCESS')}': {e}. Using default 2.")
    MIN_COMMENTS_TO_PROCESS = 2

try:
    NEW_POST_CHECK_LIMIT = int(os.environ.get("NEW_POST_CHECK_LIMIT", "50"))
    if NEW_POST_CHECK_LIMIT <= 0:
        raise ValueError("NEW_POST_CHECK_LIMIT must be positive.")
    if NEW_POST_CHECK_LIMIT < POST_LIMIT and MIN_COMMENTS_TO_PROCESS > 0: # Ensure enough posts are checked
        print(f"Warning: NEW_POST_CHECK_LIMIT ({NEW_POST_CHECK_LIMIT}) is less than POST_LIMIT ({POST_LIMIT}) "
              f"and MIN_COMMENTS_TO_PROCESS is > 0. May not find enough eligible posts.")
except (ValueError, TypeError) as e:
    print(f"Invalid NEW_POST_CHECK_LIMIT env var '{os.environ.get('NEW_POST_CHECK_LIMIT')}': {e}. Using default 50.")
    NEW_POST_CHECK_LIMIT = 50


# --- Initialize AWS Clients ---
session = boto3.session.Session()
secrets_client = session.client(service_name='secretsmanager', region_name=REGION_NAME)
dynamodb_resource = session.resource('dynamodb', region_name=REGION_NAME)
s3_client = session.client('s3', region_name=REGION_NAME)
bedrock_runtime = session.client(service_name='bedrock-runtime', region_name=REGION_NAME)

# --- Helper Function: Get Secrets ---
def get_secret():
    """Retrieves the Reddit API credentials from AWS Secrets Manager."""
    actual_secret_name = os.environ.get("SECRET_NAME_ENV_VAR", SECRET_NAME) # Use env var if set
    try:
        print(f"Attempting to retrieve secret: {actual_secret_name}")
        get_secret_value_response = secrets_client.get_secret_value(SecretId=actual_secret_name)
        if 'SecretString' in get_secret_value_response:
            secret = json.loads(get_secret_value_response['SecretString'])
            print("Secret JSON parsed successfully.")
            if 'REDDIT_CLIENT_ID' not in secret or 'REDDIT_CLIENT_SECRET' not in secret:
                 raise ValueError("Retrieved secret is missing REDDIT_CLIENT_ID or REDDIT_CLIENT_SECRET keys.")
            return secret
        else:
            # This case should ideally not happen if GetSecretValue succeeds without error
            raise ValueError("SecretString not found in Secrets Manager response despite successful API call.")
    except ClientError as e:
        print(f"Secrets Manager ClientError retrieving secret '{actual_secret_name}': {e.response.get('Error', {}).get('Code', 'UnknownError')} - {e.response.get('Error', {}).get('Message', str(e))}")
        raise e # Re-raise to be caught by main handler and indicate failure
    except (json.JSONDecodeError, ValueError) as e: # Catch issues with parsing or content
        print(f"Error parsing secret JSON or validating content for secret '{actual_secret_name}': {e}")
        # It's crucial to raise here to prevent proceeding without valid credentials
        raise ValueError(f"Secret validation failed for '{actual_secret_name}': {e}") from e

# --- Helper Function: Build Bedrock Prompt ---
def build_bedrock_prompt(title, selftext, top_comments_list):
    """Builds the prompt for the Bedrock model."""
    comments_section = f"Top Comments Provided (up to {MAX_COMMENTS_FOR_PROMPT}):\n"
    if top_comments_list:
        for i, comment_body in enumerate(top_comments_list):
            # Truncate very long comments before escaping, to avoid overly long prompts
            truncated_comment = (comment_body[:1500] + '...') if len(comment_body) > 1500 else comment_body
            # Escape for JSON embedding within the prompt string itself (though Claude handles unescaped better usually)
            escaped_comment = json.dumps(truncated_comment)[1:-1] # Remove surrounding quotes from dumps
            comments_section += f"Comment {i+1}:\n{escaped_comment}\n---\n"
    else:
        comments_section += "No relevant comments provided or fetched.\n"
    
    # Basic sanitization of title and selftext for the prompt
    sanitized_title = title.replace('{','(').replace('}',')')
    sanitized_selftext = selftext.replace('{','(').replace('}',')') if selftext else ""

    combined_text = f"Original Post Title: {sanitized_title}\n\nOriginal Post Body:\n{sanitized_selftext}\n\n{comments_section}"

    prompt = f"""Human: Analyze the following Reddit thread (original post and top comments) about AWS, focusing on explaining the concepts for learning purposes.

1.  **Problem Identification & Summary:** Identify the core technical problem or question from the original post. Provide a concise summary (1-2 sentences) as 'problem_summary'. This MUST be a JSON string value.
2.  **Problem Explanation (Learning Focus):** The purpose of this section is to provide educational context about the technologies involved in the problem. Based on the 'problem_summary' and the *original post's content*:
    a. Identify the 1-2 **primary AWS services or key technical concepts** that are most central to the user's problem.
    b. For **each** of these identified core services/concepts, provide a foundational explanation suitable for a learner (What it is, primary use case, how it functions).
    c. Briefly connect them back to the 'problem_summary'.
    The value for 'problem_explanation' MUST be a JSON object containing a list of objects under the key 'primary_concepts' and a string under the key 'explanation'. Each object in 'primary_concepts' list must have string values for 'name', 'definition', 'use_case', and 'how_it_functions'. Example: {{"primary_concepts": [{{"name": "Concept1", "definition": "def text", "use_case": "uc text", "how_it_functions": "hif text"}}], "explanation": "explanation text"}}
3.  **Solution Identification & Summary:** Analyze the provided top comments. Concisely summarize the main potential solutions, suggestions, or key advice offered as 'solution_summary'. This MUST be a JSON string value. If no relevant solutions are found, state "No specific solutions offered in provided comments."
4.  **Solution Explanation:** Based on the 'solution_summary' and the comments, provide a **concise textual explanation** of the summarized solutions in simple terms. Define any key technical terms from the comments. Aim for clarity and a few sentences. The value for 'solution_explanation' MUST be a single JSON string value. Do NOT create a nested JSON object for this field.
5.  **Categorization:** Identify the **top 3 most relevant** AWS service categories or technical concepts. The value for 'suggested_categories' MUST be a JSON list of strings (maximum 3 strings).

**Output Format:** Format the output strictly as a single, valid JSON object starting with `{{` and ending with `}}`.
*   Use only the keys "problem_summary", "problem_explanation", "solution_summary", "solution_explanation", and "suggested_categories".
*   All string values (including those for 'problem_summary', 'solution_summary', and 'solution_explanation') MUST be enclosed in double quotes.
*   The value for "problem_explanation" MUST be a JSON object as described above.
*   The value for "solution_explanation" MUST be a JSON string.
*   The value for "suggested_categories" MUST be a JSON list of strings.
*   Ensure proper JSON syntax (commas between key-value pairs, etc.). Do not include any text before `{{` or after `}}`.

Example Categories: EC2, S3, Lambda, Networking, Security, Cost Optimization, Migration, Serverless, Databases, IAM, Containers, CI/CD, IaC, Monitoring, API Gateway, Route 53, CloudFront, Other.

Reddit Thread Text:
<thread>
{combined_text}
</thread>

Assistant:"""
    return prompt

# --- Helper Function: Invoke Bedrock ---
def invoke_bedrock(prompt):
    """Invokes Bedrock model and processes the response, ensuring schema consistency."""
    if not BEDROCK_MODEL_ID:
        # This case should ideally be caught by the main handler's env var check
        print("CRITICAL: BEDROCK_MODEL_ID not set in invoke_bedrock.")
        # Fallback to a defined error structure
        return {
            "problem_summary": "Error: BEDROCK_MODEL_ID not configured.",
            "problem_explanation": {"primary_concepts": [], "explanation": "Error: BEDROCK_MODEL_ID not configured."},
            "solution_summary": "Error: BEDROCK_MODEL_ID not configured.",
            "solution_explanation": "Error: BEDROCK_MODEL_ID not configured.",
            "suggested_categories": ["Error", "Configuration Issue"]
        }

    # Define default structures for error cases to maintain schema consistency
    default_problem_explanation_on_error = {
        "primary_concepts": [], # List of structs
        "explanation": "Error: Bedrock analysis or JSON parsing failed." # String
    }
    # SolutionExplanation is now a string
    default_solution_explanation_on_error_str = "Error: Bedrock analysis or JSON parsing failed."
    
    default_categories_on_error = ["Error", "Bedrock Call Failed"] # List of strings

    analysis_text = None
    sanitized_json_str = None # For debugging if needed
    
    try:
        # Claude 3 models expect the prompt without "Human:" / "Assistant:" when using messages API
        user_message_content = prompt.split("Human:", 1)[-1].split("Assistant:", 1)[0].strip()
        messages = [{"role": "user", "content": user_message_content}]
        
        body_payload = {
            "messages": messages,
            "max_tokens": 2500, # Slightly increased for potentially verbose explanations
            "temperature": 0.2,
            "top_p": 0.9,
            "anthropic_version": "bedrock-2023-05-31" 
        }
        body = json.dumps(body_payload)
        
        print(f"Invoking Bedrock model: {BEDROCK_MODEL_ID} with max_tokens: {body_payload['max_tokens']}")
        response = bedrock_runtime.invoke_model(
            body=body,
            modelId=BEDROCK_MODEL_ID,
            accept='application/json',
            contentType='application/json'
        )
        response_body = json.loads(response['body'].read().decode('utf-8'))
        print("Bedrock response received.")

        if 'content' in response_body and isinstance(response_body['content'], list) and len(response_body['content']) > 0:
            if isinstance(response_body['content'][0], dict) and 'text' in response_body['content'][0]:
                analysis_text = response_body['content'][0]['text']
        
        if not analysis_text: # analysis_text is None or empty string
             print(f"Bedrock response body for debugging (no valid text content): {response_body}")
             return {
                 "problem_summary": "Error: Bedrock response did not contain expected text.",
                 "problem_explanation": default_problem_explanation_on_error,
                 "solution_summary": "Error: Bedrock response did not contain expected text.",
                 "solution_explanation": default_solution_explanation_on_error_str,
                 "suggested_categories": default_categories_on_error
             }
        
        print(f"--- RAW BEDROCK TEXT OUTPUT (length: {len(analysis_text)}) --- \n{analysis_text[:1000]}...\n--- END RAW BEDROCK TEXT ---") # Log snippet

        # Regex to find JSON block, assuming it's the main content.
        # This is somewhat forgiving if there's leading/trailing non-JSON text from the model.
        match = re.search(r'^\s*(\{.*?\})\s*$', analysis_text, re.DOTALL) # More strict: starts and ends with {}
        if not match: # Try a more lenient regex if the strict one fails
            match = re.search(r'(\{.*?\})', analysis_text, re.DOTALL) # Finds first occurrence

        if match:
            analysis_json_str = match.group(1) # Group 1 because of capturing parentheses
            # Basic control character sanitization (excluding \t, \n, \r)
            sanitized_json_str = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', analysis_json_str)
            print("Extracted potential JSON block using regex.")
        else:
            print(f"Could not find JSON block boundaries using regex in Bedrock text (first 500 chars): {analysis_text[:500]}")
            return {
                "problem_summary": "Error: No JSON object found via regex in Bedrock output.",
                "problem_explanation": default_problem_explanation_on_error,
                "solution_summary": "Error: No JSON object found via regex.",
                "solution_explanation": default_solution_explanation_on_error_str,
                "suggested_categories": ["Error", "No JSON Found via Regex"]
            }
        
        # print(f"Attempting to parse sanitized JSON string (strict=False):\n{repr(sanitized_json_str)}\n") # Verbose
        try:
            analysis_json = json.loads(sanitized_json_str, strict=False) # strict=False is more forgiving
            print("Parsed JSON from Bedrock response (strict=False).")
        except json.JSONDecodeError as e:
            print(f"CRITICAL: Error decoding JSON even with strict=False: {e}")
            context_start = max(0, e.pos - 40)
            context_end = min(len(sanitized_json_str), e.pos + 40)
            error_char = sanitized_json_str[e.pos] if e.pos < len(sanitized_json_str) else "[EOF]"
            context_str = f"...{sanitized_json_str[context_start:e.pos]}>>>ERROR (char {e.pos}) '{error_char}'<<<{sanitized_json_str[e.pos+1:context_end]}..."
            print(f"Context around error position {e.pos}:\n{context_str}")
            return {
                "problem_summary": f"Fatal JSON parsing error: {e.msg}", # Use e.msg for cleaner error
                "problem_explanation": default_problem_explanation_on_error,
                "solution_summary": f"Fatal JSON parsing error: {e.msg}",
                "solution_explanation": default_solution_explanation_on_error_str,
                "suggested_categories": ["Error", "Fatal Parsing Failed", e.msg]
            }

        # --- Standardize and Validate Parsed JSON ---
        final_analysis = {}

        # problem_summary (string)
        final_analysis["problem_summary"] = str(analysis_json.get("problem_summary", "N/A - Key missing or invalid type"))
        
        # problem_explanation (struct with 'primary_concepts': list of structs, 'explanation': string)
        pe_val = analysis_json.get("problem_explanation", {}) # Default to empty dict if key missing
        std_pe = {"primary_concepts": [], "explanation": "N/A - Content missing"}
        if isinstance(pe_val, dict):
            std_pe["explanation"] = str(pe_val.get("explanation", pe_val.get("relevance_to_problem", "N/A - Explanation missing")))
            concepts_list = pe_val.get("primary_concepts", [])
            if isinstance(concepts_list, list):
                for concept in concepts_list:
                    if isinstance(concept, dict):
                        std_pe["primary_concepts"].append({
                            "name": str(concept.get("name", "N/A")),
                            "definition": str(concept.get("definition", "N/A")),
                            "use_case": str(concept.get("use_case", "N/A")),
                            "how_it_functions": str(concept.get("how_it_functions", "N/A"))
                        })
                    # else: # Log if a concept item isn't a dict (optional)
                    #     print(f"Warning: Item in primary_concepts is not a dict: {concept}")
            # else: # Log if primary_concepts isn't a list (optional)
            #     print(f"Warning: primary_concepts in problem_explanation is not a list: {concepts_list}")
        # else: # Log if problem_explanation itself isn't a dict (optional)
        #     print(f"Warning: problem_explanation is not a dict: {pe_val}")
        final_analysis["problem_explanation"] = std_pe


        # solution_summary (string)
        final_analysis["solution_summary"] = str(analysis_json.get("solution_summary", "N/A - Key missing or invalid type"))

        # solution_explanation (NOW A STRING)
        se_val = analysis_json.get("solution_explanation")
        if isinstance(se_val, dict): # Model returned a dict by mistake
            print(f"Warning: 'solution_explanation' was a dict, converting to string representation. Content: {se_val}")
            try:
                final_analysis["solution_explanation"] = json.dumps(se_val) # Convert dict to JSON string
            except TypeError: # Handle non-serializable content in dict
                 final_analysis["solution_explanation"] = str(se_val) # Fallback to simple string conversion
        elif isinstance(se_val, str):
            final_analysis["solution_explanation"] = se_val
        else: # Not a dict or string, use default error string
            print(f"Warning: 'solution_explanation' not a string or dict. Defaulting. Type: {type(se_val)}")
            final_analysis["solution_explanation"] = default_solution_explanation_on_error_str
        
        # suggested_categories (list of strings)
        sc_val = analysis_json.get("suggested_categories")
        if isinstance(sc_val, list):
            final_analysis["suggested_categories"] = [str(item) for item in sc_val if isinstance(item, (str, int, float, bool))] # Ensure items are strings
        else:
            final_analysis["suggested_categories"] = ["N/A - Invalid type or key missing"]
            
        return final_analysis

    except ClientError as e: # Catch Bedrock API errors
        print(f"Bedrock API ClientError: {e.response.get('Error', {}).get('Code', 'UnknownError')} - {e.response.get('Error', {}).get('Message', str(e))}")
        return { # Return schema-consistent error structure
            "problem_summary": f"Error: Bedrock API ClientError - {e.response.get('Error', {}).get('Code', 'UnknownError')}",
            "problem_explanation": default_problem_explanation_on_error,
            "solution_summary": f"Error: Bedrock API ClientError - {e.response.get('Error', {}).get('Code', 'UnknownError')}",
            "solution_explanation": default_solution_explanation_on_error_str,
            "suggested_categories": default_categories_on_error
        }
    except Exception as e: # Catch any other unexpected errors during Bedrock call/processing
        print(f"Unexpected error during Bedrock invocation: {type(e).__name__} - {e}")
        traceback.print_exc()
        return { # Return schema-consistent error structure
            "problem_summary": f"Error: Unexpected error during Bedrock - {type(e).__name__}",
            "problem_explanation": default_problem_explanation_on_error,
            "solution_summary": f"Error: Unexpected error during Bedrock - {type(e).__name__}",
            "solution_explanation": default_solution_explanation_on_error_str,
            "suggested_categories": default_categories_on_error
        }

# --- Main Lambda Handler ---
def lambda_handler(event, context):
    start_time = time.time()
    current_execution_time_utc = datetime.now(timezone.utc)
    print(f"Lambda execution started at {current_execution_time_utc.isoformat()}")

    # Check for essential environment variables
    required_env_vars = ["DYNAMODB_TABLE_NAME", "S3_BUCKET_NAME", "BEDROCK_MODEL_ID"]
    missing_vars = [var for var in required_env_vars if not os.environ.get(var)]
    if missing_vars:
        error_msg = f"Missing required environment variables: {', '.join(missing_vars)}"
        print(f"CRITICAL ERROR: {error_msg}")
        return {'statusCode': 500, 'body': json.dumps({'error': error_msg})}

    processed_count = 0
    skipped_due_to_low_comments = 0
    skipped_duplicates = 0
    failed_posts_details = [] # Store details of posts that failed processing
    eligible_posts_to_process = []
    
    # Define default structures for storage, matching expected Parquet schema types
    # These are used if analysis_result itself is malformed or a key is unexpectedly missing
    default_pe_struct_for_storage = {"primary_concepts": [], "explanation": "N/A - Analysis result malformed"}
    default_se_string_for_storage = "N/A - Analysis result malformed" 
    default_cat_list_for_storage = ["N/A - Analysis result malformed"]

    try:
        reddit_credentials = get_secret() # This will raise if secrets are not found/valid
        client_id = reddit_credentials['REDDIT_CLIENT_ID']
        client_secret = reddit_credentials['REDDIT_CLIENT_SECRET']
        print("Reddit credentials obtained securely.")

        user_agent = f'AwsAnalysisBot/1.0.0 by YourRedditUsername' # Updated User Agent
        reddit = praw.Reddit(client_id=client_id, client_secret=client_secret, user_agent=user_agent)
        dynamodb_table = dynamodb_resource.Table(DYNAMODB_TABLE_NAME)
        print(f"PRAW initialized. Targetting r/{SUBREDDIT_NAME}. User Agent: {user_agent}")

        # --- Post Fetching Logic ---
        subreddit = reddit.subreddit(SUBREDDIT_NAME)
        print(f"\n--- Post Fetching Parameters ---")
        print(f"MIN_COMMENTS_TO_PROCESS: {MIN_COMMENTS_TO_PROCESS}")
        print(f"NEW_POST_CHECK_LIMIT: {NEW_POST_CHECK_LIMIT}")
        print(f"POST_LIMIT (target eligible posts): {POST_LIMIT}")
        posts_checked_count = 0
        print(f"Fetching up to {NEW_POST_CHECK_LIMIT} newest posts from r/{SUBREDDIT_NAME}...")
        
        try:
            for post in subreddit.new(limit=NEW_POST_CHECK_LIMIT):
                posts_checked_count += 1
                if post.num_comments >= MIN_COMMENTS_TO_PROCESS:
                    eligible_posts_to_process.append(post)
                    if len(eligible_posts_to_process) >= POST_LIMIT:
                        print(f"Collected {POST_LIMIT} eligible posts with >= {MIN_COMMENTS_TO_PROCESS} comments. Stopping fetch.")
                        break 
                else:
                    skipped_due_to_low_comments += 1
                
                if posts_checked_count % 25 == 0: # Log progress less frequently
                     print(f"  ...checked {posts_checked_count} posts, found {len(eligible_posts_to_process)} eligible so far...")
            print(f"Finished scanning {posts_checked_count} newest posts. Found {len(eligible_posts_to_process)} eligible posts. Skipped {skipped_due_to_low_comments} for low comments.")
        except PRAWException as e:
            print(f"PRAWException during subreddit.new() fetch: {e}")
            if not eligible_posts_to_process: # If no posts were gathered before error, it's more critical
                 raise # Re-raise to be caught by the main handler's critical error block
            print(f"Warning: Continuing with {len(eligible_posts_to_process)} posts found before PRAWException during fetch.")
        except Exception as e: # Catch other potential errors during PRAW fetch
            print(f"Unexpected error during PRAW subreddit.new() fetch: {type(e).__name__} - {e}")
            traceback.print_exc()
            if not eligible_posts_to_process:
                raise
            print(f"Warning: Continuing with {len(eligible_posts_to_process)} posts found before unexpected error during fetch.")

        if not eligible_posts_to_process:
            summary_message = (f"No posts found with at least {MIN_COMMENTS_TO_PROCESS} comments "
                               f"after checking the {posts_checked_count} newest posts from r/{SUBREDDIT_NAME}.")
            print(summary_message)
            return {'statusCode': 200, 'body': json.dumps({'message': summary_message, 
                                                            'processed_count': 0, 
                                                            'skipped_duplicates': 0, 
                                                            'skipped_low_comments': skipped_due_to_low_comments,
                                                            'failed_posts_count': 0})}
        
        # --- Post Processing Loop ---
        print(f"\n--- Starting Processing for {len(eligible_posts_to_process)} Eligible Posts ---")
        for post_idx, post in enumerate(eligible_posts_to_process):
            post_id = post.id
            post_created_dt_utc = datetime.fromtimestamp(post.created_utc, timezone.utc)
            # Short title for logging, handle potential missing author
            author_name = post.author.name if post.author else '[deleted]'
            print(f"\n[{post_idx+1}/{len(eligible_posts_to_process)}] Processing Post ID: {post_id} by {author_name} (Comments: {post.num_comments}) | Title: {post.title[:60]}...")

            # Check DynamoDB
            try:
                response = dynamodb_table.get_item(Key={'PostID': post_id}, ConsistentRead=True)
                if 'Item' in response:
                    print(f"  Post {post_id} already processed (found in DynamoDB). Skipping.")
                    skipped_duplicates += 1
                    continue
                print(f"  Post {post_id} is new. Proceeding...")
            except ClientError as e:
                print(f"  ERROR checking DynamoDB for post {post_id}: {e.response.get('Error', {}).get('Code', 'UnknownError')}. Skipping post.")
                failed_posts_details.append({'id': post_id, 'title': post.title[:50], 'reason': f'DynamoDB check failed: {e.response.get("Error", {}).get("Code", "UnknownError")}'})
                continue # Skip to the next post

            # Process individual post (comments, Bedrock, S3, DynamoDB update)
            try:
                # Fetch comments
                top_comments_for_prompt = []
                all_fetched_comments_text_list = []
                comments_fetched_count = 0
                post.comment_sort = 'top' # Ensure 'top' sort for consistency
                post.comments.replace_more(limit=0) # Expand "more comments" links
                
                for top_level_comment in post.comments.list():
                    if isinstance(top_level_comment, MoreComments): continue # Should be handled by replace_more

                    comment_body = top_level_comment.body.strip() if top_level_comment.body else ""
                    if not comment_body or comment_body == "[deleted]" or comment_body == "[removed]":
                        continue # Skip empty or deleted/removed comments
                    
                    if comments_fetched_count < MAX_COMMENTS_FOR_PROMPT:
                        top_comments_for_prompt.append(comment_body)
                    
                    if comments_fetched_count < COMMENT_LIMIT:
                         all_fetched_comments_text_list.append(f"- {comment_body}") # Add bullet for readability
                    
                    comments_fetched_count += 1
                    # Stop if we have enough for both prompt and full list, or just full list if MAX_COMMENTS_FOR_PROMPT is met
                    if comments_fetched_count >= COMMENT_LIMIT:
                        break
                    if comments_fetched_count >= MAX_COMMENTS_FOR_PROMPT and len(all_fetched_comments_text_list) >= COMMENT_LIMIT: # Redundant check but safe
                        break
                
                comments_text_concatenated = "\n".join(all_fetched_comments_text_list)
                print(f"  Fetched {comments_fetched_count} non-empty comments. Using top {len(top_comments_for_prompt)} for Bedrock prompt.")

                # Invoke Bedrock
                print(f"  Preparing prompt and invoking Bedrock for post {post_id}...")
                prompt_text = build_bedrock_prompt(post.title, post.selftext, top_comments_for_prompt)
                analysis_result = invoke_bedrock(prompt_text) # This function now handles its internal errors more gracefully

                # Check if Bedrock processing itself indicated a major failure
                # The analysis_result will have error strings/structs, which is fine for storage schema
                # This check is more for logging/awareness if many Bedrock calls are failing internally
                if "Error:" in str(analysis_result.get("problem_summary", "")): # Simple check for "Error:" in summary
                     print(f"  WARNING: Bedrock analysis for post {post_id} resulted in an error state: {analysis_result.get('problem_summary')}")

                current_analysis_time_obj = datetime.now(timezone.utc)
                
                # Retrieve data from analysis_result, using defaults that match schema types
                # invoke_bedrock now ensures these keys exist and have generally correct types
                problem_summary_data = analysis_result.get("problem_summary", "N/A - Analysis Missing")
                problem_explanation_data = analysis_result.get("problem_explanation", default_pe_struct_for_storage)
                solution_summary_data = analysis_result.get("solution_summary", "N/A - Analysis Missing")
                solution_explanation_data = analysis_result.get("solution_explanation", default_se_string_for_storage) # String
                suggested_categories_data = analysis_result.get("suggested_categories", default_cat_list_for_storage)

                # Final type safety checks before creating Parquet table (belt and suspenders)
                if not isinstance(problem_summary_data, str): problem_summary_data = str(problem_summary_data)
                if not isinstance(problem_explanation_data, dict): problem_explanation_data = default_pe_struct_for_storage
                if not isinstance(solution_summary_data, str): solution_summary_data = str(solution_summary_data)
                if not isinstance(solution_explanation_data, str): solution_explanation_data = str(solution_explanation_data) # Ensure string
                if not isinstance(suggested_categories_data, list): 
                    suggested_categories_data = default_cat_list_for_storage
                else: # Ensure all items in list are strings
                    suggested_categories_data = [str(item) for item in suggested_categories_data]


                data_to_store = {
                    'PostID': post.id, 
                    'Subreddit': post.subreddit.display_name,
                    'Title': post.title, 
                    'Selftext': post.selftext if post.selftext else '',
                    'URL': post.url, 
                    'Author': author_name, # Use sanitized author_name
                    'Score': int(post.score), 
                    'OriginalCommentCount': int(post.num_comments),
                    'FetchedCommentCountForAnalysis': comments_fetched_count,
                    'PostTimestampUTC': post_created_dt_utc.isoformat(), # Already UTC
                    'FetchedCommentsText': comments_text_concatenated,
                    'ProblemSummary': problem_summary_data,
                    'SolutionSummary': solution_summary_data,
                    'ProblemExplanation': problem_explanation_data,   # Struct
                    'SolutionExplanation': solution_explanation_data, # String
                    'SuggestedCategories': suggested_categories_data, # List of Strings
                    'AnalysisTimestampUTC': current_analysis_time_obj.isoformat(),
                    'BedrockModelID': BEDROCK_MODEL_ID
                }

                # Write to Parquet
                print(f"  Converting data to Parquet for post {post_id}...")
                # PyArrow will infer the schema from the data types in data_to_store
                # Ensure data_to_store has consistent types for each key across all records
                table = pa.Table.from_pylist([data_to_store]) 
                buffer = io.BytesIO()
                pq.write_table(table, buffer, compression='snappy')
                buffer.seek(0) # Reset buffer's position to the beginning for reading

                s3_file_name = f"{post.id}.parquet" # Use post ID for filename
                s3_key = f"{S3_KEY_PREFIX}/year={current_analysis_time_obj.year:04d}/month={current_analysis_time_obj.month:02d}/day={current_analysis_time_obj.day:02d}/{s3_file_name}"
                
                print(f"  Writing Parquet file to s3://{S3_BUCKET_NAME}/{s3_key}")
                s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=buffer.getvalue())
                print(f"  Successfully wrote Parquet file to S3 for post {post_id}.")

                # Update DynamoDB
                print(f"  Marking post {post_id} as processed in DynamoDB...")
                dynamodb_table.put_item(
                    Item={'PostID': post.id,'ProcessedTimestampUTC': current_analysis_time_obj.isoformat()}
                )
                print(f"  Successfully marked post {post_id} in DynamoDB.")
                processed_count += 1

            except (ClientError, PRAWException, ValueError, TypeError, pa.ArrowException) as e: # Catch specific, expected errors
                 error_type = type(e).__name__
                 error_message_short = str(e).splitlines()[0] # Get first line of error
                 print(f"  ERROR processing post {post_id}: {error_type} - {error_message_short}")
                 # traceback.print_exc() # Can be very verbose, enable for deep debugging
                 failed_posts_details.append({'id': post_id, 'title': post.title[:50], 'reason': f'{error_type}: {error_message_short[:200]}'})
                 # Continue to the next post in the eligible_posts_to_process list
            except Exception as e: # Catch any other unexpected error for this post
                 error_type = type(e).__name__
                 error_message_short = str(e).splitlines()[0]
                 print(f"  UNEXPECTED ERROR processing post {post_id}: {error_type} - {error_message_short}")
                 traceback.print_exc() # Definitely print full traceback for unexpected errors
                 failed_posts_details.append({'id': post_id, 'title': post.title[:50], 'reason': f'Unexpected {error_type}: {error_message_short[:200]}'})


        # --- End of Post Processing Loop ---

        end_time = time.time()
        duration = end_time - start_time
        summary_message = (
            f"Lambda execution finished. Duration: {duration:.2f}s. "
            f"Checked {posts_checked_count} newest posts. "
            f"Found {len(eligible_posts_to_process)} posts with >= {MIN_COMMENTS_TO_PROCESS} comments. "
            f"Skipped {skipped_due_to_low_comments} (low comments). "
            f"Targeted {POST_LIMIT} eligible posts for processing. "
            f"Successfully Processed (New): {processed_count}, "
            f"Skipped (Duplicates): {skipped_duplicates}, "
            f"Failed during processing: {len(failed_posts_details)}."
        )
        print(f"\n--- EXECUTION SUMMARY ---")
        print(summary_message)
        
        response_body = {
            'message': summary_message, 
            'newly_processed_count': processed_count, 
            'skipped_duplicates_count': skipped_duplicates,
            'skipped_low_comments_count': skipped_due_to_low_comments,
            'failed_during_processing_count': len(failed_posts_details)
        }
        if failed_posts_details:
             print(f"--- FAILED POSTS DETAILS ({len(failed_posts_details)}) ---")
             for failed_post_info in failed_posts_details:
                 print(f"  ID: {failed_post_info['id']}, Title: '{failed_post_info['title']}', Reason: {failed_post_info['reason']}")
             response_body['failed_posts_details'] = failed_posts_details # Add to response

        # Determine overall status code
        # If all eligible posts were processed or skipped as duplicates, it's a success (200).
        # If some posts failed processing after being deemed eligible, it's a partial success (207).
        final_status_code = 200
        if failed_posts_details and processed_count < (len(eligible_posts_to_process) - skipped_duplicates):
            final_status_code = 207 # Multi-Status
        
        return {
            'statusCode': final_status_code,
            'body': json.dumps(response_body)
        }

    except Exception as e: # Catch critical errors in main handler (e.g., PRAW init, secret fetch)
        error_type = type(e).__name__
        error_message = str(e)
        print(f"CRITICAL UNHANDLED ERROR in lambda_handler: {error_type} - {error_message}")
        traceback.print_exc()
        # Return a standard error response
        return {
            'statusCode': 500,
            'body': json.dumps({'error': f"Critical unhandled error in lambda_handler: {error_type} - {error_message}"})
        }