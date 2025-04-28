import csv

input_csv = r'C:\Users\victo\Desktop\crawl\musees_france_nettoye.csv'    # Your source CSV file
output_csv = r'C:\Users\victo\Desktop\crawl\field_csv.csv'  # File where we save only the URLs

with open(input_csv, mode='r', encoding='utf-8') as infile, \
     open(output_csv, mode='w', encoding='utf-8', newline='') as outfile:
    
    reader = csv.DictReader(infile)
    writer = csv.writer(outfile)

    # Write header
    writer.writerow(['url'])

    for row in reader:
        url = row.get('url', '').strip()
        if url:  # Only process non-empty URLs
            if url.startswith('www.'):
                url = 'https://' + url  # Add https:// if it starts with 'www.'
            writer.writerow([url])

print(f"✅ URLs extracted to {output_csv}")
