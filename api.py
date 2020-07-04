import config


def get_api_keys():

    with open(config.KEYS_FILE, 'r') as f:
        data = f.read()

    keys = [x.strip() for x in data.split('\n') if x]
    return keys
