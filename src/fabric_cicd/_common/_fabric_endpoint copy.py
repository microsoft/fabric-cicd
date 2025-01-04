try:
    # Code that may raise an exception
    print("In try block")
    raise Exception("An error occurred")
except Exception as e:
    # Code to handle the exception
    print(f"In except block: {e}")
    raise Exception("New exception in except block")
finally:
    # Code that will always be executed
    print("In finally block")
