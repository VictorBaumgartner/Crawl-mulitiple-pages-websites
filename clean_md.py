import re

def clean_markdown(md_text):
    """
    Cleans a Markdown string by:
    - Removing all URLs
    - Keeping the textual content
    """
    # 1. Remove Markdown links but keep the visible text
    md_text = re.sub(r'\[([^\]]+)\]\((http[s]?://[^\)]+)\)', r'\1', md_text)

    # 2. Remove inline URLs like http://example.com
    md_text = re.sub(r'http[s]?://\S+', '', md_text)

    # 3. Remove image links ![alt](url)
    md_text = re.sub(r'!\[([^\]]*)\]\((http[s]?://[^\)]+)\)', '', md_text)

    # 4. Optionally, remove empty parentheses left behind
    md_text = re.sub(r'\(\)', '', md_text)

    # 5. Collapse multiple spaces and newlines
    md_text = re.sub(r'\n\s*\n', '\n\n', md_text)  # Compact blank lines
    md_text = re.sub(r'[ \t]+', ' ', md_text)  # Remove extra spaces

    return md_text.strip()

# Example usage
if __name__ == "__main__":
    with open("input.md", "r", encoding="utf-8") as f:
        markdown_content = f.read()

    cleaned_content = clean_markdown(markdown_content)

    with open("output_cleaned.md", "w", encoding="utf-8") as f:
        f.write(cleaned_content)

    print("Markdown cleaned successfully!")
