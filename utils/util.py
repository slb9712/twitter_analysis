def decode_entity_unicode(data_list):
    """将包含 Unicode 编码的字符串转换为可读字符"""
    for item in data_list:
        item['entity'] = item['entity'].encode('utf-8').decode('unicode_escape')
    return data_list
