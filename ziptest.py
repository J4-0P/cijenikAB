import libarchive

def extract_zip_in_memory(zip_bytes):
    extracted = {}
    with libarchive.memory_reader(zip_bytes) as archive:
        for entry in archive:
            chunks = []
            for block in entry.get_blocks():
                chunks.append(block)
            extracted[entry.pathname] = b"".join(chunks)
    return extracted

if __name__ == "__main__":
    with open("PROIZVODI-2025-06-01.zip", "rb") as f:
        zip_bytes = f.read()

    files = extract_zip_in_memory(zip_bytes)
    for filename in files:
        text = files[filename].decode('utf-8')  # decode bytes to string, adjust encoding if needed
        print(text)
    else:
        print(f"{filename} not found in the archive.")

