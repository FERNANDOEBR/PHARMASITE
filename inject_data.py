import pandas as pd
import json
import re

# Read CSV and fill NaNs
df = pd.read_csv('municipios_sp_scored.csv')
for col in df.columns:
    if df[col].dtype == 'float64':
        df[col] = df[col].fillna(0).round(2)
    elif df[col].dtype == 'int64':
        df[col] = df[col].fillna(0)

# Convert to JSON records
records = df.to_dict(orient='records')
json_str = json.dumps(records, ensure_ascii=False)

# Inject into HTML
with open('dashboard.html', 'r', encoding='utf-8') as f:
    html = f.read()

# Replace the ALL_DATA object
html = re.sub(r'const ALL_DATA = \[.*?\];', f'const ALL_DATA = {json_str};', html, count=1, flags=re.DOTALL)

with open('dashboard.html', 'w', encoding='utf-8') as f:
    f.write(html)

print("Injected new ALL_DATA into HTML successfully.")
