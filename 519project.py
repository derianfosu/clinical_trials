#LIBRARIES
import xml.etree.cElementTree as ET
import os
import xmltodict

tags_dict = dict()
to_process = []

def get_xml_files(path):
    """
    Read all the files in the given path and return a list of XML files
    """

    return [f for f in os.listdir(path) if f.endswith('.xml')]

def process_file(root, path, initial, record):
    if root.text == None:
        return ""
    if root.text.strip() != '':
        #print(path)
        try:
            tags_dict[path] += 1
        except KeyError:
            tags_dict[path] = 1
        return path
    for item in root.iter():
        if item.tag != root.tag and item.tag not in record:
            record.append(item.tag)
            path = path + "-" + item.tag
            process_file(item, path, path, record)
            path = initial

def get_text(root, tag):
    return {item.text for item in root.findall(tag)}

def get_multiple_text(root, tag, tags):
    if len(tags) == 0:
        #get_text(root, tag)
        return get_text(root, tag)
    else:
        try:
            if len(tags) > 1:
                return get_multiple_text(root.findall(tag)[0], tags[0], tags[1:])
            else:
                return get_multiple_text(root.findall(tag)[0], tags[0], [])
        except:
            return get_text(root, tag)

def get_tags(path, xml_files, max_access_count):
    accessed = 0
    while accessed < max_access_count:
        record = []
        tree = ET.parse(path + xml_files[accessed])
        root = tree.getroot()

        for item in tree.iter():
            if item.tag not in record and item.tag != 'clinical_study' and root.findall(item.tag) != []:
                process_file(item, item.tag, item.tag, record)
        accessed += 1        

def find_matches(tags_dict, threshold):
    return [key for key in tags_dict if tags_dict[key] > threshold]

def initialize_type(matched_tags):
    for item in matched_tags:
        data_type[item] = 'text'

def temp(path, xml_files, max_access_count, matched_tags, data_type):
    accessed = 0
    while accessed < max_access_count:
        tree = ET.parse(path + xml_files[accessed])
        root = tree.getroot()
        for tag in matched_tags:
            tags = tag.split('-')
            if len(tags) == 1:
                result = get_text(root, tags[0])
            else:
                #print(xml_files[accessed])
                result = get_multiple_text(root, tags[0], tags[1:])
            if len(result) > 1:
                data_type[tag] = 'list'
        accessed += 1

if __name__ == "__main__":
    data_source_path = './data/'
    xml_files = get_xml_files(data_source_path)
    
    file_capacity = len(xml_files)
    threshold = int(0.3*file_capacity)
    
    get_tags(data_source_path, xml_files, file_capacity)
    matched_tags = find_matches(tags_dict, threshold)
    data_type = dict()
    initialize_type(matched_tags)
    
    temp(data_source_path, xml_files, file_capacity, matched_tags, data_type)

    create_table_query = """CREATE TABLE clinical_trial(
                nct_id text PRIMARY KEY,"""
    for k in data_type:
        print(k + ": " + data_type[k])
        if data_type[k] == 'list':
            create_table_query = create_table_query + "\n" + k + " text[],"
        else:
            create_table_query = create_table_query + "\n" + k + " text,"
    create_table_query = create_table_query[:-1] + ")"
    print(create_table_query)

    """
    count = 0
    print(tags_dict)
    for i in tags_dict:
        if tags_dict[i] > threshold:
            print(i)
            count += 1
    print("Total Tags: " + str(len(tags_dict)))
    print("Tags Matched: " + str(count))
    
    tree = ET.parse(data_source_path + xml_files[0])
    root = tree.getroot()
    
    tags = ['sponsors', 'lead_sponsor', 'agency']
    if len(tags) == 1:
        result = get_text(root, tags[0])
    else:
        result = get_multiple_text(root, tags[0], tags[1:])
    print(result)
    
    """

    
