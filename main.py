import pandas as pd
from pathlib import Path

def decode_can_data(input_row):
    """
    Decode CAN data from raw format to structured format.
    
    Args:
        input_row (dict): Input dictionary containing raw CAN data
        
    Returns:
        dict: Decoded CAN data in structured format
    """
    try:
        # Convert hex string CAN ID to integer
        can_id = int(input_row['id'], 16)
        
        # Convert binary string to bytes
        binary_str = input_row['data']
        data_bytes = []
        
        # Process 8 bytes (64 bits) of data
        for i in range(0, 64, 8):
            byte_str = binary_str[i:i+8]
            byte_val = int(byte_str, 2)
            data_bytes.append(byte_val)
        
        # Create output dictionary
        decoded_data = {
            'timestamp': int(float(input_row['timestamp'])),  # Handle string input
            'can_id': can_id,
            'data_length': 8,
            'data': data_bytes
        }
        
        return decoded_data
    except (ValueError, KeyError, IndexError) as e:
        print(f"Error processing row: {input_row}")
        print(f"Error details: {str(e)}")
        return None

def process_csv(input_file, output_file, max_rows=10):
    """
    Process CAN data from CSV file and write decoded results to new CSV file.
    
    Args:
        input_file (str): Path to input CSV file
        output_file (str): Path to output CSV file
        max_rows (int): Maximum number of rows to process
    """
    try:
        # Read input CSV
        df = pd.read_csv(input_file)
        
        # Take only first max_rows
        df = df.head(max_rows)
        
        # Process each row
        decoded_rows = []
        for _, row in df.iterrows():
            decoded = decode_can_data(row.to_dict())
            if decoded:
                # Convert data bytes to hex string for CSV storage
                decoded['data'] = ','.join([f"0x{x:02X}" for x in decoded['data']])
                decoded_rows.append(decoded)
        
        # Create output DataFrame
        output_df = pd.DataFrame(decoded_rows)
        
        # Convert CAN ID to hex string for better readability
        output_df['can_id'] = output_df['can_id'].apply(lambda x: f"0x{x:X}")
        
        # Save to CSV
        output_df.to_csv(output_file, index=False)
        print(f"Successfully processed {len(decoded_rows)} rows")
        print(f"Output saved to: {output_file}")
        
    except Exception as e:
        print(f"Error processing file: {str(e)}")

if __name__ == "__main__":
    # Example usage
    input_csv = "can_data_input.csv"
    output_csv = "can_data_decoded.csv"
    
    # Check if input file exists
    if not Path(input_csv).exists():
        print(f"Error: Input file '{input_csv}' not found!")
        exit(1)
    
    # Process the file
    process_csv(input_csv, output_csv)
    
    # Display first few lines of output file
    print("\nFirst few lines of output file:")
    try:
        print(pd.read_csv(output_csv).head().to_string())
    except Exception as e:
        print(f"Error reading output file: {str(e)}")