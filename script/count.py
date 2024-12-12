import pandas as pd

# Load the dataset
df = pd.read_csv('AIMOTIVE INC_.csv')  # Replace with the correct file loading method if it's not a CSV

# Get the unique values count in the 'cause' column
unique_causes_count = df['Cause'].nunique()

print(f"Number of unique values in 'cause': {unique_causes_count}")