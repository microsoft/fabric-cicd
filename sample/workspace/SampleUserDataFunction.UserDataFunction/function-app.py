import azure.functions as func
import logging
import json

app = func.FunctionApp()

@app.function_name(name="SampleFunction")
@app.route(route="sample", auth_level=func.AuthLevel.ANONYMOUS)
def sample_function(req: func.HttpRequest) -> func.HttpResponse:
    """
    Sample User Data Function that processes HTTP requests.
    
    This function demonstrates basic data processing capabilities
    that can be used in Microsoft Fabric CI/CD workflows.
    """
    logging.info('Python HTTP trigger function processed a request.')

    # Get query parameters
    name = req.params.get('name')
    if not name:
        try:
            req_body = req.get_json()
        except ValueError:
            req_body = {}
        
        if req_body:
            name = req_body.get('name')

    if name:
        response_data = {
            "message": f"Hello, {name}! This is a sample User Data Function.",
            "status": "success",
            "processed_at": "2024-01-01T00:00:00Z"
        }
        return func.HttpResponse(
            json.dumps(response_data),
            status_code=200,
            mimetype="application/json"
        )
    else:
        error_response = {
            "error": "Please provide a name parameter",
            "status": "error"
        }
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=400,
            mimetype="application/json"
        )


@app.function_name(name="DataProcessor")
@app.route(route="process", auth_level=func.AuthLevel.ANONYMOUS)
def data_processor(req: func.HttpRequest) -> func.HttpResponse:
    """
    Example data processing function for Fabric data operations.
    """
    logging.info('Data processing function triggered.')
    
    try:
        # Simulate data processing
        result = {
            "processed_records": 100,
            "status": "completed",
            "processing_time": "2.5s"
        }
        
        return func.HttpResponse(
            json.dumps(result),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        error_response = {
            "error": str(e),
            "status": "failed"
        }
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=500,
            mimetype="application/json"
        )
