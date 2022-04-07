class TimeplusAPIError(Exception):
    """Exception raised for errors in the timeplus api.

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, method, code, message="failed to call timeplus API"):
        self.method = method
        self.code = code
        self.message = f"http method {method}, response code {code}, {message}"
        super().__init__(self.message)
