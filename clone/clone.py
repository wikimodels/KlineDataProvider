import os

# Get current directory (where clone.py is located)
folder = os.path.dirname(os.path.abspath(__file__))

# Process all files in the current folder
for filename in os.listdir(folder):
    file_path = os.path.join(folder, filename)

    # Skip directories and .txt files (to avoid reprocessing)
    if not os.path.isfile(file_path) or filename.endswith(".txt"):
        continue

    # Get base name without extension
    name, _ = os.path.splitext(filename)

    # Read content
    with open(file_path, "r", encoding="utf-8") as f:
        content = f.read()

    # Write to .txt file
    txt_path = os.path.join(folder, f"{name}.txt")
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(content)

print("✅ All files in clone/ converted to .txt")
