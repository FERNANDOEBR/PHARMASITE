import os
import sys
from anthropic import Anthropic
from dotenv import load_dotenv

load_dotenv()

# Verify API key
api_key = os.getenv("ANTHROPIC_API_KEY")
if not api_key or api_key == "your_key_here":
    print("Error: ANTHROPIC_API_KEY not found or invalid in .env")
    sys.exit(1)

client = Anthropic(api_key=api_key)

# Read the newly rewritten scores.py script
file_path = "pipeline/scores.py"
try:
    with open(file_path, "r", encoding="utf-8") as f:
        code_content = f.read()
except FileNotFoundError:
    print(f"Error: Could not find {file_path}")
    sys.exit(1)

prompt = f"""
I am building a Geomarketing scoring engine for pharmaceutical logistics in Brazil.
I just rewrote my scores calculation to stop using heuristic, manually assigned weights.
Instead, it now uses a Hybrid Weighting system that blends Principal Component Analysis (for correlation extraction) and the Entropy Weight Method (for dispersion differentiation).

Please act as a Principal Data Scientist and review this specific Python implementation.
Does the math in `calculate_entropy_weights` and the PCA sub-weighting look mathematically correct and pythonic? 

Keep your answer to the point, focusing mostly on the math and sklearn usage.

Here is the Code:
```python
{code_content}
```
"""

print("Sending scores.py to Claude (Anthropic API) for review...")

try:
    response = client.messages.create(
        model="claude-3-haiku-20240307",  # Fallback to Haiku to avoid 404s
        max_tokens=1500,
        temperature=0.2,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    print("\n--- CLAUDE'S EXPERT REVIEW ---\n")
    print(response.content[0].text)
except Exception as e:
    print(f"Error communicating with Anthropic API: {e}")
