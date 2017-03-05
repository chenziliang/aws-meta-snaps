import urllib2
import boto3_proxy_patch


# boto3 response
def is_http_ok(response):
    return response["ResponseMetadata"]["HTTPStatusCode"] in (200, 201)


def http_code(response):
    return response["ResponseMetadata"]["HTTPStatusCode"]


def is_likely_gzip(data):
    # Maybe not gzip actually, but we don't care
    if len(data) < 2:
        return False
    return data[0] == "\037" and data[1] == "\213"


def assemble_proxy_url(hostname, port, username=None, password=None):
    endpoint = '{host}:{port}'.format(
        host=hostname,
        port=port
    )
    auth = None
    if username:
        auth = urllib2.quote(username.encode(), safe='')
        if password:
            auth += ':'
            auth += urllib2.quote(password.encode(), safe='')

    if auth:
        return auth + '@' + endpoint
    return endpoint


def set_proxy_env(config):
    if not config.get("proxy_hostname"):
        return

    username = config.get("proxy_username")
    password = config.get("proxy_password")
    hostname = config["proxy_hostname"]
    port = config["proxy_port"]
    url = assemble_proxy_url(hostname, port, username, password)
    boto3_proxy_patch.set_proxies("http://" + url, "https://" + url)
