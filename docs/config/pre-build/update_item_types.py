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
        
        # Define categories with their item types
        categories = {
            "Data Engineering": [
                "DataPipeline", "Lakehouse", "Notebook", "Warehouse", "SQLDatabase", 
                "SparkJobDefinition", "Environment", "Dataflow", "CopyJob", "ApacheAirflowJob"
            ],
            "Real-Time Intelligence": [
                "Eventhouse", "KQLDatabase", "KQLQueryset", "KQLDashboard", 
                "Eventstream", "Reflex"
            ],
            "Data Science": [
                "MLExperiment"
            ],
            "Data Integration": [
                "MirroredDatabase", "MountedDataFactory", "DataAgent"
            ],
            "Business Intelligence": [
                "Report", "SemanticModel"
            ],
            "Other": [
                "GraphQLApi", "UserDataFunction", "VariableLibrary", "OrgApp"
            ]
        }
        
        # Validation: Ensure all item types are categorized
        all_categorized = set(item for items in categories.values() for item in items)
        uncategorized = set(supported_item_types) - all_categorized
        if uncategorized:
            raise ValueError(f"Uncategorized item types found: {uncategorized}. Please add them to a category.")
        
        # Build categorized table
        markdown_content = "| Category | Item Types |\n"
        markdown_content += "|----------|------------|\n"
        
        for category_name, items in categories.items():
            # Only include items that are in ACCEPTED_ITEM_TYPES
            category_items = [item for item in items if item in supported_item_types]
            if category_items:
                items_str = ", ".join(category_items)
                markdown_content += f"| {category_name} | {items_str} |\n"

        new_markdown = markdown[:start_index] + markdown_content + markdown[end_index:]
        return new_markdown
    return markdown
