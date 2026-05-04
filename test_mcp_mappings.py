import sys
import os
import pandas as pd

sys.path.append(os.getcwd())
from mcp_server import load_mappings_from_paths

BASE_PATH = r'C:\Users\shobhit.sharma\Downloads\Carvan Prior Payroll Setup'
PATHS = [
    os.path.join(BASE_PATH, 'Payroll Mappings - Earnings Mapping.csv'),
    os.path.join(BASE_PATH, 'Payroll Mappings - Deductions Mapping.csv'),
    os.path.join(BASE_PATH, 'Payroll Mappings - Contributions Mapping.csv'),
    os.path.join(BASE_PATH, 'Payroll_Mappings_Tax_Mapping_CORRECTED.csv')
]

mappings = load_mappings_from_paths(PATHS)
print(f"Total mappings loaded via MCP: {len(mappings)}")
if mappings:
    print("First 2 mappings:")
    print(mappings[:2])
    print("Last 2 mappings:")
    print(mappings[-2:])
    
    # Check if all mappings have 'ADP_Name' and 'UZIO_Name'
    keys = set().union(*(m.keys() for m in mappings))
    print(f"Keys found in mappings: {keys}")
