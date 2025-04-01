import requests

### ELLIE

ellie_settings = None

"""
Make a new connection to Ellie.

The connection settings are stored globally and used by other functions in this module.

Parameters:
    settings (dict): Dictionary containing Ellie connection parameters:
        - organization: Ellie organization URL
        - token: Ellie API token
        - api_version: API version (default: 'v1')
        
Returns:
    None
"""
def ellie_connect(settings):
    global ellie_settings
    
    if 'api_version' not in settings: 
        settings['api_version'] = 'v1'
    
    ellie_settings = settings

"""
Import a model exported from a database to Ellie.

This function sends the model data to Ellie API and creates a new model.

Parameters:
    name (str): Name for the new model in Ellie
    model (dict): Model data for creating the new model, in Ellie model format
    level (str): Model level (conceptual, logical, or physical). Default: 'conceptual'
    
Returns:
    requests.Response: Response from the Ellie API
    
Note:
    The API response JSON will typically include:
    - id: The ID of the created model
    - success: Whether the creation was successful
    - other metadata about the model
"""
def ellie_model_import(name, model, level='conceptual'):
    print(f"Creating {level} model: {name}")
    model['model']['name'] = name
    model['model']['level'] = level
    url = f'''{ellie_settings['organization']}/api/{ellie_settings['api_version']}/models?token={ellie_settings['token']}'''
    return requests.post(url=url, json=model)

"""
Export a model from Ellie.

Parameters:
    model_id (int): ID of the model to export
    
Returns:
    dict: Model data in Ellie format
"""
def ellie_model_export(model_id):
    url = f'''{ellie_settings['organization']}/api/{ellie_settings['api_version']}/models/{model_id}?token={ellie_settings['token']}'''
    return requests.get(url=url).json()