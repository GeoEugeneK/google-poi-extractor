import config


def get_valid_types():

    with open(config.VALID_TYPES_FILE, 'r') as f:
        data = f.read()

    types = [i.strip() for i in data.split("\n") if i]
    return list(set(types))


def get_search_types():

    with open(config.SEARCH_TYPES_FILE, 'r') as f:
        data = f.read()

    types = [i.strip() for i in data.split("\n") if i]
    return list(set(types))
