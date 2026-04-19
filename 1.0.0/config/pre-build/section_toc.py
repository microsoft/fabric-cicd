from pathlib import Path
import re

import yaml

def get_section_order(nav, current_dir_str):
    """
    Recursively find the order of markdown files in the current directory as defined in nav.
    """
    order = []
    for item in nav:
        if isinstance(item, dict):
            for key, value in item.items():
                if isinstance(value, list):
                    # Recurse into subsections
                    order += get_section_order(value, current_dir_str)
                elif isinstance(value, str):
                    path = Path(value)
                    if str(path.parent).replace("\\", "/") == current_dir_str:
                        order.append(path.name)
        elif isinstance(item, str):
            path = Path(item)
            if str(path.parent).replace("\\", "/") == current_dir_str:
                order.append(path.name)
    return order

def on_page_markdown(markdown, page, config, files):
    if "<!--BEGIN-SECTION-TOC-->\n" in markdown:
        start_index = markdown.index("<!--BEGIN-SECTION-TOC-->\n") + len("<!--BEGIN-SECTION-TOC-->\n")
        end_index = markdown.index("<!--END-SECTION-TOC-->\n")

        current_page_path = Path(page.file.abs_src_path)
        current_page_dir = current_page_path.parent

        # Compute docs root (where mkdocs.yml lives)
        mkdocs_yml = Path(config['config_file_path'])
        docs_root = mkdocs_yml.parent

        # Get current dir relative to docs root, as string
        current_dir_rel = str(current_page_dir.name)

        # Load mkdocs.yml and extract nav order
        with mkdocs_yml.open("r", encoding="utf-8") as f:
            mkdocs_config = yaml.safe_load(f)
        nav = mkdocs_config.get('nav', [])
        section_order = get_section_order(nav, current_dir_rel)
        

        toc = []
        for md_name in section_order:
            md_file = current_page_dir / md_name
            if not md_file.exists() or md_file == current_page_path:
                continue
            with md_file.open("r", encoding="utf-8") as f:
                content = f.read()
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
