
def find_first_occurrence(file_path, substring):
    with open(file_path, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, start=1):
            if substring in line:
                return line_num
    return -1  # not found

# Example usage
file_path = 'yourfile.txt'
substring = '"Ribola"'
line = find_first_occurrence("output\combined_2025-05-30.ndjson", substring)
if line != -1:
    print(f"Found at line {line}")
else:
    print("Not found")
