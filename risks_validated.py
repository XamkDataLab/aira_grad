"""
Script to extract LLM-generated JSON from the 'risks' JSONB column
and store it in a new 'risks_validated' JSONB column in the texts table.
"""

import json
import re
from db import get_engine, execute_query
from sqlalchemy import text


def run_query(query):
    """Execute a SQL query and return results as a DataFrame."""
    df, error = execute_query(query)
    if error:
        print("Error:", error)
        return None
    return df


def execute_statement(statement):
    """Execute a SQL statement (no return expected)."""
    try:
        engine = get_engine()
        with engine.connect() as connection:
            connection.execute(text(statement))
            connection.commit()
            print("Statement executed successfully.")
    except Exception as e:
        print("Error:", e)


def extract_json_from_llm_response(risks_data):
    """
    Extract the JSON object from the LLM response.
    The JSON is typically in choices[0]['text'] field.
    """
    if risks_data is None:
        return None
    
    # Handle if it's a string (shouldn't be for jsonb, but just in case)
    if isinstance(risks_data, str):
        try:
            risks_data = json.loads(risks_data)
        except json.JSONDecodeError:
            return None
    
    if not isinstance(risks_data, dict):
        return None
    
    # Extract text from choices[0]['text']
    try:
        choices = risks_data.get('choices', [])
        if not choices or len(choices) == 0:
            return None
        
        text_content = choices[0].get('text', '')
        if not text_content:
            return None
        
        # Find JSON object in the text
        # The JSON typically starts after some reasoning text and begins with '{'
        # We need to find the outermost JSON object
        
        # Method 1: Try to find JSON that starts with { "relevance"
        json_match = re.search(r'\{\s*"relevance"', text_content)
        if json_match:
            start_idx = json_match.start()
            json_str = text_content[start_idx:]
            
            # Find the matching closing brace
            brace_count = 0
            end_idx = 0
            for i, char in enumerate(json_str):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_idx = i + 1
                        break
            
            if end_idx > 0:
                json_str = json_str[:end_idx]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    pass
        
        # Method 2: Try to find any JSON object starting with {
        # Find the last occurrence of a complete JSON object
        json_match = re.search(r'\{[^{}]*(?:\{[^{}]*\}[^{}]*)*\}', text_content)
        if json_match:
            try:
                return json.loads(json_match.group())
            except json.JSONDecodeError:
                pass
        
        # Method 3: More aggressive - find first { and try to parse from there
        first_brace = text_content.find('{')
        if first_brace != -1:
            # Try to find balanced braces
            potential_json = text_content[first_brace:]
            brace_count = 0
            end_idx = 0
            for i, char in enumerate(potential_json):
                if char == '{':
                    brace_count += 1
                elif char == '}':
                    brace_count -= 1
                    if brace_count == 0:
                        end_idx = i + 1
                        break
            
            if end_idx > 0:
                json_str = potential_json[:end_idx]
                try:
                    return json.loads(json_str)
                except json.JSONDecodeError:
                    pass
        
        return None
        
    except Exception as e:
        print(f"Error extracting JSON: {e}")
        return None


def ensure_column_exists():
    """Ensure the risks_validated column exists in the texts table."""
    check_column = """
    SELECT column_name 
    FROM information_schema.columns 
    WHERE table_name = 'texts' AND column_name = 'risks_validated';
    """
    result = run_query(check_column)
    
    if result is None or len(result) == 0:
        print("Creating risks_validated column...")
        execute_statement("""
            ALTER TABLE texts 
            ADD COLUMN IF NOT EXISTS risks_validated JSONB;
        """)
        print("Column created.")
    else:
        print("Column risks_validated already exists.")


def process_risks_batch(batch_size=1000):
    """
    Process risks column in batches and extract JSON to risks_validated.
    Only processes rows where risks is not null.
    """
    ensure_column_exists()
    
    # Count total rows to process
    count_query = """
    SELECT COUNT(*) as cnt 
    FROM texts 
    WHERE risks IS NOT NULL 
      AND risks_validated IS NULL;
    """
    count_result = run_query(count_query)
    total_rows = count_result['cnt'].iloc[0] if count_result is not None else 0
    print(f"Total rows to process: {total_rows}")
    
    if total_rows == 0:
        print("No rows to process.")
        return
    
    processed = 0
    failed = 0
    
    while True:
        # Fetch a batch of rows
        fetch_query = f"""
        SELECT url, risks 
        FROM texts 
        WHERE risks IS NOT NULL 
          AND risks_validated IS NULL
        LIMIT {batch_size};
        """
        
        batch = run_query(fetch_query)
        
        if batch is None or len(batch) == 0:
            break
        
        print(f"Processing batch of {len(batch)} rows...")
        
        for idx, row in batch.iterrows():
            url = row['url']
            risks = row['risks']
            
            # Extract JSON from LLM response
            extracted_json = extract_json_from_llm_response(risks)
            
            if extracted_json is not None:
                # Update the row with extracted JSON
                json_str = json.dumps(extracted_json).replace("'", "''")
                update_query = f"""
                UPDATE texts 
                SET risks_validated = '{json_str}'::jsonb
                WHERE url = '{url.replace("'", "''")}';
                """
                try:
                    execute_statement(update_query)
                    processed += 1
                except Exception as e:
                    print(f"Error updating {url}: {e}")
                    failed += 1
            else:
                # Mark as processed but with null (to avoid reprocessing)
                # Set to empty object to indicate it was processed but no valid JSON found
                update_query = f"""
                UPDATE texts 
                SET risks_validated = 'null'::jsonb
                WHERE url = '{url.replace("'", "''")}';
                """
                try:
                    execute_statement(update_query)
                    failed += 1
                except Exception as e:
                    print(f"Error marking {url}: {e}")
        
        print(f"Progress: {processed} extracted, {failed} failed")
    
    print(f"\nDone! Total extracted: {processed}, Total failed: {failed}")


def test_extraction():
    """Test the extraction on a sample row."""
    sample_query = """
    SELECT url, risks 
    FROM texts 
    WHERE risks IS NOT NULL 
    LIMIT 1;
    """
    sample = run_query(sample_query)
    
    if sample is not None and len(sample) > 0:
        print("Sample URL:", sample['url'].iloc[0])
        print("\nOriginal risks type:", type(sample['risks'].iloc[0]))
        
        extracted = extract_json_from_llm_response(sample['risks'].iloc[0])
        
        if extracted:
            print("\nExtracted JSON keys:", list(extracted.keys()))
            print("\nExtracted JSON (pretty):")
            print(json.dumps(extracted, indent=2, ensure_ascii=False)[:2000])
        else:
            print("\nFailed to extract JSON")
    else:
        print("No sample data found")


if __name__ == "__main__":
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "test":
        print("Running test extraction...")
        test_extraction()
    else:
        print("Starting risks validation extraction...")
        process_risks_batch(batch_size=500)
