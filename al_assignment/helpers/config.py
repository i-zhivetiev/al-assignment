import json


def read(path):
    with open(path) as f:
        config = json.load(f)
    return config
