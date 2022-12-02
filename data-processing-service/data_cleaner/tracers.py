from datetime import datetime


def endpoint_tracer(response, *args, **kwargs) -> None:
    """
    A hook function to print-out response from the requests module.
    """
    timestamp = datetime.now().isoformat()

    endpoint = f"{timestamp}: {response.request.method} {response.url}"
    res_code = f"{response.status_code} {response.reason}"

    print(f"\n{endpoint} - {res_code}")


def response_tracer(response, *args, **kwargs) -> None:
    """
    A hook function to print-out response from the requests module.
    """
    timestamp = datetime.now().isoformat()

    endpoint = f"{timestamp}: {response.request.method} {response.url}"
    res_code = f"{response.status_code} {response.reason}"
    res_data = f"{timestamp}: {str(response.content, 'utf-8')}"

    print(f"\n{endpoint} - {res_code}\n{res_data}")
