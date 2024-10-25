import pandas as pd
from pathlib import Path

def decode_j1939_id(can_id_hex):
    """
    Decode J1939 CAN ID to extract PGN
    """
    try:
        can_id = int(can_id_hex, 16)
        # Extract PGN - for 18FEF100, we want to get FEF1
        pgn = (can_id >> 8) & 0xFFFF
        print(f"CAN ID: {can_id_hex}, Extracted PGN: {hex(pgn)}")
        return pgn
    except Exception as e:
        print(f"Error decoding CAN ID {can_id_hex}: {str(e)}")
        return None

def decode_spn_100(data_binary):
    """
    Decode SPN 100 (Engine Oil Pressure) from binary data
    """
    try:
        # Print the full binary string for debugging
        print(f"Full binary data: {data_binary}")
        
        # For byte 4 (index 3), we need bits 24-31
        byte_value = int(data_binary[24:32], 2)
        print(f"Extracted byte value: {byte_value} (binary: {data_binary[24:32]})")
        
        # Apply resolution (4 kPa per bit)
        pressure_kpa = byte_value * 4
        print(f"Calculated pressure: {pressure_kpa} kPa")
        
        return pressure_kpa
    except Exception as e:
        print(f"Error decoding SPN 100: {str(e)}")
        return None

def process_can_data(input_file, output_file):
    """
    Process CAN data to extract SPN 100 information
    """
    try:
        # Read input CSV
        df = pd.read_csv(input_file)
        print(f"Read {len(df)} records from input file")
        
        decoded_rows = []
        
        for idx, row in df.iterrows():
            print(f"\nProcessing row {idx + 1}:")
            print(f"CAN ID: {row['id']}")
            print(f"Data: {row['data']}")
            
            # Decode J1939 ID
            pgn = decode_j1939_id(row['id'])
            
            if pgn is not None:
                # Check if PGN matches Engine Fluid Level/Pressure 1 (0xFEF1 = 65265)
                print(f"Checking PGN: {hex(pgn)} against target: 0xFEF1")
                if pgn == 0xFEF1:
                    print("PGN match found!")
                    # Decode SPN 100
                    oil_pressure = decode_spn_100(row['data'])
                    
                    if oil_pressure is not None:
                        decoded_row = {
                            'timestamp': int(float(row['timestamp'])),
                            'pgn': f"0x{pgn:X}",
                            'oil_pressure_kpa': oil_pressure,
                            'original_can_id': row['id']
                        }
                        decoded_rows.append(decoded_row)
                        print(f"Successfully decoded: Oil Pressure = {oil_pressure} kPa")
                else:
                    print(f"PGN {hex(pgn)} does not match target 0xFEF1")
        
        if decoded_rows:
            output_df = pd.DataFrame(decoded_rows)
            output_df.to_csv(output_file, index=False)
            print(f"\nSuccessfully processed {len(decoded_rows)} SPN 100 records")
            print(f"Output saved to: {output_file}")
            
            print("\nEngine Oil Pressure Statistics:")
            print(f"Average: {output_df['oil_pressure_kpa'].mean():.2f} kPa")
            print(f"Maximum: {output_df['oil_pressure_kpa'].max():.2f} kPa")
            print(f"Minimum: {output_df['oil_pressure_kpa'].min():.2f} kPa")
        else:
            print("\nNo valid SPN 100 records found in the input data")
            
    except Exception as e:
        print(f"Error processing file: {str(e)}")

# Let's also create proper test data
def create_test_data(filename="can_data_input_test_spn_100.csv"):
    """
    Create test data file with correct format
    """
    data = [
        {
            'timestamp': '1630324545.121231',
            'can_line': 'can1',
            'id': '18FEF100',
            'data': '0000000000000000000000000001111000000000000000000000000000000000'
        },
        {
            'timestamp': '1630324545.221231',
            'can_line': 'can1',
            'id': '18FEF100',
            'data': '0000000000000000000000000001111000000000000000000000000000000000'
        },
        {
            'timestamp': '1630324545.321231',
            'can_line': 'can1',
            'id': '18FEF100',
            'data': '0000000000000000000000000001111000000000000000000000000000000000'
        }
    ]
    
    df = pd.DataFrame(data)
    df.to_csv(filename, index=False)
    print(f"Created test file: {filename}")
    return filename

if __name__ == "__main__":
    # Create test data
    input_csv = create_test_data()
    output_csv = "spn_100.csv"
    
    # Process the file
    process_can_data(input_csv, output_csv)