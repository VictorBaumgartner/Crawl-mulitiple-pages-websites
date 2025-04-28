import csv

input_csv = 'input.csv'    # Your source CSV file
output_csv = 'urls_only.csv'  # File where we save only the URLs

with open(input_csv, mode='r', encoding='utf-8') as infile, \
     open(output_csv, mode='w', encoding='utf-8', newline='') as outfile:
    
    reader = csv.DictReader(infile)
    writer = csv.writer(outfile)

    # Write header
    writer.writerow(['url'])

    for row in reader:
        url = row.get('url', '').strip()
        if url:  # Only write non-empty URLs
            writer.writerow([url])

print(f"✅ URLs extracted to {output_csv}")
