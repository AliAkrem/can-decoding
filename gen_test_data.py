import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def create_test_data():
    """
    Create test data for J1939 SPN 100 (Engine Oil Pressure)
    PGN 65263 (0xFEF1) - Engine Fluid Level/Pressure 1
    """
    # Number of records
    n_records = 20
    
    # Start timestamp
    base_timestamp = datetime.now().timestamp()
    
    # Initialize lists for data
    records = []
    
    for i in range(n_records):
        # Create timestamp with 100ms intervals
        timestamp = base_timestamp + (i * 0.1)
        
        # Create CAN ID for PGN 65263 (0xFEF1)
        # Format: 18FEF100 where:
        # 18 - Priority 6 (0x18)
        # FEF1 - PGN 65263
        # 00 - Source address
        can_id = '18FEF100'
        
        # Create data field
        # We'll set realistic oil pressure values between 100-400 kPa
        oil_pressure_kpa = np.random.randint(100, 400)
        # Convert to raw value (divide by 4 due to resolution)
        oil_pressure_raw = int(oil_pressure_kpa / 4)
        
        # Create 8-byte data field
        # Byte 4 (index 3) contains oil pressure
        data_bytes = ['00'] * 8
        data_bytes[3] = f'{oil_pressure_raw:02X}'
        
        # Convert to binary string
        data_binary = ''.join([bin(int(byte, 16))[2:].zfill(8) for byte in data_bytes])
        
        records.append({
            'timestamp': timestamp,
            'can_line': 'can1',
            'id': can_id,
            'data': data_binary
        })
    
    # Create DataFrame
    df = pd.DataFrame(records)
    
    # Save to CSV
    test_file = "can_data_input.csv"
    df.to_csv(test_file, index=False)
    print(f"Created test file: {test_file}")
    print("\nSample of generated data:")
    print(df.head().to_string())
    
    return test_file

if __name__ == "__main__":
    # Generate test data
    test_file = create_test_data()
    
    # Now let's test the decoder with this data
    from j1939_decoder import process_can_data
    
    # Process the test data
    output_file = "spn_100.csv"
    process_can_data(test_file, output_file)
    
    # Display the decoded results
    if Path(output_file).exists():
        print("\nDecoded SPN 100 data:")
        decoded_data = pd.read_csv(output_file)
        print(decoded_data.head().to_string())
        
        # Show some statistics
        print("\nOil Pressure Statistics:")
        print(f"Average: {decoded_data['oil_pressure_kpa'].mean():.2f} kPa")
        print(f"Maximum: {decoded_data['oil_pressure_kpa'].max():.2f} kPa")
        print(f"Minimum: {decoded_data['oil_pressure_kpa'].min():.2f} kPa")