import requests


class CustomRequest:
    def __init__(self, method, url, headers, bearer_token, body=None, files=None):
        self._request_method = method
        self._request_url = url
        self._request_body = body
        self._request_files = files

        self._request_headers = {"Authorization": f"Bearer {bearer_token}"}
        if files is None:
            self._headers["Content-Type"] = "application/json; charset=utf-8"

    def submit(self):
        return requests.request(
            method=self._request_method,
            url=self._request_url,
            headers=self._headers,
            data=self._request_body,
            files=self._request_files,
        )


class CustomResponse:
    def __init__(self, request):
        self._request = request
        self._response = self.request.submit()

    @property
    def request(self):
        return self._request

    @property
    def response(self):
        return self._response


if __name__ == "main":
    pass
