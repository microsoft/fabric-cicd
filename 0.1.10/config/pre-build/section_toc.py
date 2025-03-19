from pathlib import Path
import re

def on_page_markdown(markdown, page, config, files):
    if "<!--BEGIN-SECTION-TOC-->\n" in markdown:
        start_index = markdown.index("<!--BEGIN-SECTION-TOC-->\n") + len("<!--BEGIN-SECTION-TOC-->\n")
        end_index = markdown.index("<!--END-SECTION-TOC-->\n")

        # Get the current page path
        current_page_path = Path(page.file.abs_src_path)
        current_page_dir = current_page_path.parent

        # Find all markdown files at the same level
        toc = []
        for md_file in current_page_dir.glob("*.md"):
            if md_file == current_page_path:
                continue

            # Extract headers from the markdown file, excluding code blocks
            with md_file.open("r", encoding="utf-8") as f:
                content = f.read()
                # Remove code blocks
                content_no_code = re.sub(r"```.*?```", "", content, flags=re.DOTALL)
                headers = re.findall(r"^(#{1,6})\s+(.*)", content_no_code, re.MULTILINE)
                for header in headers:
                    level = len(header[0])
                    title = header[1]
                    toc.append(f"{'   ' * level}- [{title}]({md_file.name}#{title.lower().replace(' ', '-')})")

        toc_content = "\n".join(toc)
        new_markdown = markdown[:start_index] + toc_content + markdown[end_index:]
        return new_markdown
    return markdown
