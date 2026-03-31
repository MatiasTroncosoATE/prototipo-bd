import polars as pl
from surveymonkey import SurveyMonkey
from surveymonkey.exceptions import SurveyMonkeyError
import time

def fetch_survey_results_polars(access_token, survey_id, batch_size=100):
    """
    Fetch survey responses directly into Polars DataFrame with efficient memory usage.
    
    Args:
        access_token (str): SurveyMonkey API access token
        survey_id (str): The survey ID
        batch_size (int): Number of responses per batch
    
    Returns:
        pl.DataFrame: Polars DataFrame with flattened response structure
    """
    client = SurveyMonkey(access_token=access_token)
    all_dataframes = []
    page = 1
    
    try:
        # Get survey details
        survey_details = client.surveys.get(survey_id)
        print(f"Fetching results for: {survey_details['title']}")
        
        while True:
            # Fetch a page of responses
            responses = client.surveys.responses.list(
                survey_id, 
                page=page, 
                per_page=batch_size
            )
            
            response_data = responses.get('data', [])
            
            if not response_data:
                break
            
            # Process batch into Polars DataFrame
            batch_df = process_response_batch(response_data, survey_id)
            all_dataframes.append(batch_df)
            
            print(f"Processed page {page}: {len(response_data)} responses")
            
            # Check for more pages
            if not responses.get('links', {}).get('next'):
                break
                
            page += 1
            time.sleep(0.1)  # Be respectful of rate limits
            
    except SurveyMonkeyError as e:
        print(f"Error fetching survey results: {e}")
        return pl.DataFrame()
    
    # Combine all batches
    if all_dataframes:
        return pl.concat(all_dataframes, how='vertical')
    return pl.DataFrame()

def process_response_batch(responses, survey_id):
    """
    Process a batch of responses into a Polars DataFrame.
    
    Args:
        responses (list): List of response dictionaries
        survey_id (str): Survey ID for reference
    
    Returns:
        pl.DataFrame: Processed Polars DataFrame for the batch
    """
    # Build rows for the DataFrame
    rows = []
    
    for resp in responses:
        # Get base response info
        response_id = resp.get('id')
        date_created = resp.get('date_created')
        date_modified = resp.get('date_modified')
        
        # Extract answers from pages
        pages = resp.get('pages', [])
        for page in pages:
            page_id = page.get('id')
            page_title = page.get('title', '')
            
            for question in page.get('questions', []):
                question_id = question.get('id')
                question_heading = question.get('heading', '')
                family = question.get('family', '')
                subtype = question.get('subtype', '')
                
                # Handle different question types
                answers = question.get('answers', [])
                
                if not answers:
                    # No answers (e.g., skipped question)
                    rows.append({
                        'response_id': response_id,
                        'survey_id': survey_id,
                        'page_id': page_id,
                        'page_title': page_title,
                        'question_id': question_id,
                        'question_heading': question_heading,
                        'question_family': family,
                        'question_subtype': subtype,
                        'answer_text': None,
                        'answer_choice_id': None,
                        'answer_other': None,
                        'date_created': date_created,
                        'date_modified': date_modified,
                    })
                else:
                    for answer in answers:
                        # Extract answer details
                        answer_text = answer.get('text', '')
                        choice_id = answer.get('choice_id')
                        other_text = answer.get('other_text')
                        
                        rows.append({
                            'response_id': response_id,
                            'survey_id': survey_id,
                            'page_id': page_id,
                            'page_title': page_title,
                            'question_id': question_id,
                            'question_heading': question_heading,
                            'question_family': family,
                            'question_subtype': subtype,
                            'answer_text': answer_text,
                            'answer_choice_id': choice_id,
                            'answer_other': other_text,
                            'date_created': date_created,
                            'date_modified': date_modified,
                        })
    
    # Convert to Polars DataFrame with proper types
    df = pl.DataFrame(rows)
    
    # Add schema optimizations
    if df.height > 0:
        # Convert date columns
        date_cols = ['date_created', 'date_modified']
        for col in date_cols:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).str.strptime(pl.Datetime, format="%Y-%m-%dT%H:%M:%S%z")
                )
        
        # Set categorical for repeated string columns
        categorical_cols = ['survey_id', 'page_id', 'question_id', 'question_family', 'question_subtype']
        for col in categorical_cols:
            if col in df.columns:
                df = df.with_columns(pl.col(col).cast(pl.Categorical))
    
    return df

# Usage
if __name__ == "__main__":
    ACCESS_TOKEN = "your_token_here"
    SURVEY_ID = "your_survey_id"
    
    # Option 1: Two-step approach
    raw = fetch_survey_results_raw(ACCESS_TOKEN, SURVEY_ID)
    df = to_polars_dataframe(raw)
    
    # Option 2: Direct approach (recommended)
    df = fetch_survey_results_polars(ACCESS_TOKEN, SURVEY_ID)
    
    # Explore the data
    print(df.head())
    print(f"Shape: {df.shape}")
    print(f"Memory usage: {df.estimated_size('mb')} MB")
    
    # Pivot to wide format if needed
    pivot_df = df.pivot(
        values='answer_text',
        index='response_id',
        columns='question_heading',
        aggregate_function='first'
    )
    print(pivot_df.head())
