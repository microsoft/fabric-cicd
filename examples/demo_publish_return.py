#!/usr/bin/env python3
"""
Demonstration script showing the new return functionality of publish_all_items.

This script demonstrates how to use the updated publish_all_items function
to get information about deployed items, specifically semantic models and reports.
"""

from fabric_cicd import FabricWorkspace, publish_all_items

def demo_publish_all_items_return():
    """Demonstrate the new return functionality of publish_all_items."""
    
    # Example setup (these would be your actual values)
    workspace_id = "your-workspace-id"
    repository_directory = "/path/to/your/repository"
    
    # Create workspace object with SemanticModel and Report in scope
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        repository_directory=repository_directory,
        item_type_in_scope=['SemanticModel', 'Report'],
        # token_credential=your_token_credential  # Add your credential here
    )
    
    # Publish all items and get information about what was published
    published_items = publish_all_items(target_workspace)
    
    print("=== Published Items Summary ===")
    print(f"Total item types processed: {len(published_items)}")
    
    # Process semantic models
    semantic_models = published_items.get("SemanticModel", {})
    if semantic_models:
        print(f"\n📊 Semantic Models Published: {len(semantic_models)}")
        for name, item_info in semantic_models.items():
            print(f"  • {name}")
            print(f"    - GUID: {item_info['guid']}")
            print(f"    - Description: {item_info['description']}")
            print(f"    - Logical ID: {item_info['logical_id']}")
    else:
        print("\n📊 No semantic models were published")
    
    # Process reports
    reports = published_items.get("Report", {})
    if reports:
        print(f"\n📈 Reports Published: {len(reports)}")
        for name, item_info in reports.items():
            print(f"  • {name}")
            print(f"    - GUID: {item_info['guid']}")
            print(f"    - Description: {item_info['description']}")
            print(f"    - Logical ID: {item_info['logical_id']}")
    else:
        print("\n📈 No reports were published")
    
    # You can also access the full item information for programmatic use
    # For example, to get all GUIDs of published semantic models:
    semantic_model_guids = [
        item_info['guid'] 
        for item_info in semantic_models.values()
    ]
    
    report_guids = [
        item_info['guid']
        for item_info in reports.values()
    ]
    
    print(f"\n🔗 Semantic Model GUIDs: {semantic_model_guids}")
    print(f"🔗 Report GUIDs: {report_guids}")
    
    return published_items

def demo_filtered_publishing():
    """Demonstrate how to use the return value for conditional processing."""
    
    # Example: Only process items that were successfully published
    workspace_id = "your-workspace-id"
    repository_directory = "/path/to/your/repository"
    
    target_workspace = FabricWorkspace(
        workspace_id=workspace_id,
        repository_directory=repository_directory,
        item_type_in_scope=['SemanticModel', 'Report', 'Notebook'],
    )
    
    published_items = publish_all_items(target_workspace)
    
    # Example: Create a mapping of item names to GUIDs for downstream processes
    item_guid_mapping = {}
    for item_type, items in published_items.items():
        for item_name, item_info in items.items():
            item_guid_mapping[f"{item_type}:{item_name}"] = item_info['guid']
    
    print(f"\n📋 Item GUID Mapping: {item_guid_mapping}")
    
    # Example: Log published items for audit trail
    total_published = sum(len(items) for items in published_items.values())
    print(f"\n✅ Audit: {total_published} items published successfully")
    
    return item_guid_mapping

if __name__ == "__main__":
    print("🚀 Demonstrating publish_all_items return functionality\n")
    
    try:
        # Run the basic demonstration
        demo_publish_all_items_return()
        
        print("\n" + "="*50)
        
        # Run the filtered processing demonstration  
        demo_filtered_publishing()
        
    except Exception as e:
        print(f"❌ Demo failed: {e}")
        print("Note: This demo requires valid workspace credentials and repository setup")