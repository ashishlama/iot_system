def preprocess(data):
    data['value'] = round(data['value'], 3)
    return data