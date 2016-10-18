import json
def fix_array(info):
    ''' Change out dict items in the following case:
           - dict value is another dict
           - the sub-dictionary only has one entry
           - the key in the subdictionary starts with '$'
        In this specific case, one level of indirection
        is removed, and the dict value is replaced with
        the sub-dict value.
    '''
    for item in info:
        for key, value in item.items():
            if not isinstance(value, dict) or len(value) != 1:
                continue
            (subkey, subvalue), = value.items()
            if not subkey.startswith('$'):
                continue
            item[key] = subvalue

with open("tweet.json", 'w+') as tweetFile:
    line = json.load(tweetFile)
    fix_array(line)
    print(line)
    json.dumps(line, tweetFile)
    tweetFile.close()
