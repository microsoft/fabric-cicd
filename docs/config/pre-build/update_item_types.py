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
        
        # Categorize item types
        data_engineering = [
            "DataPipeline", "Lakehouse", "Notebook", "Warehouse", "SQLDatabase", 
            "SparkJobDefinition", "Environment", "Dataflow", "CopyJob", "ApacheAirflowJob"
        ]
        data_science = [
            "MLExperiment"
        ]
        real_time_intelligence = [
            "Eventhouse", "KQLDatabase", "KQLQueryset", "KQLDashboard", 
            "Eventstream", "Reflex"
        ]
        data_integration = [
            "MirroredDatabase", "MountedDataFactory", "DataAgent"
        ]
        business_intelligence = [
            "Report", "SemanticModel"
        ]
        other = [
            "GraphQLApi", "UserDataFunction", "VariableLibrary", "OrgApp"
        ]
        
        # Build categorized table
        markdown_content = "| Category | Item Types |\n"
        markdown_content += "|----------|------------|\n"
        
        categories = [
            ("Data Engineering", data_engineering),
            ("Real-Time Intelligence", real_time_intelligence),
            ("Data Science", data_science),
            ("Data Integration", data_integration),
            ("Business Intelligence", business_intelligence),
            ("Other", other)
        ]
        
        for category_name, items in categories:
            # Only include items that are in ACCEPTED_ITEM_TYPES
            category_items = [item for item in items if item in supported_item_types]
            if category_items:
                items_str = ", ".join(category_items)
                markdown_content += f"| {category_name} | {items_str} |\n"

        new_markdown = markdown[:start_index] + markdown_content + markdown[end_index:]
        return new_markdown
    return markdown
