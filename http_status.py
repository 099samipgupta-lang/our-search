def classify_status(status):
    if 200 <= status < 300:
        return "success"

    if 300 <= status < 400:
        return "redirect"

    if 400 <= status < 500:
        return "client_error"

    if 500 <= status < 600:
        return "server_error"

    return "unknown"


def is_redirect(status):
    return 300 <= status < 400
