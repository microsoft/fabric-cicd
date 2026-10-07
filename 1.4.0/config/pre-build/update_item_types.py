import sys
from pathlib import Path

root_directory = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(root_directory / "src"))

import fabric_cicd.constants as constants


def on_page_markdown(markdown, **kwargs):
    if "<!--BEGIN-SUPPORTED-ITEM-TYPES-->\n" in markdown:
        start_index = markdown.index("<!--BEGIN-SUPPORTED-ITEM-TYPES-->\n") + len("<!--BEGIN-SUPPORTED-ITEM-TYPES-->\n")
        end_index = markdown.index("<!--END-SUPPORTED-ITEM-TYPES-->\n")

        supported_item_types = constants.ACCEPTED_ITEM_TYPES
        markdown_content = "\n".join([f"-   {item}" for item in supported_item_types])

        new_markdown = markdown[:start_index] + markdown_content + markdown[end_index:]
        return new_markdown
    return markdown
