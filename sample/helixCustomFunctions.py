from deployfabric.CustomPrint import CustomPrint

'''
Functions for preprocessing Helix workspaces.
'''

def preprocess_all_items(fabric_workspace_obj):
    """
    Coordinates preprocessing of all notebooks and environments.

    :param fabric_workspace_obj: An instance of FabricWorkspace containing repository items and workspace ID.
    """
    CustomPrint.header('Pre Processing Items')
    CustomPrint.timestamp('No pre processing steps')