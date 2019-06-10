#LIBRARIES
import xml.etree.cElementTree as ET
import os
import postgres_operations as po


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
    return {item.text.replace('{', '').replace('}','') for item in root.findall(tag)}

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

def update_data_type(path, xml_files, max_access_count, matched_tags, data_type):
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

def integrate_data(path, xml_files, max_access_count, commit_threshold, data_type, conn, cur, table_name, checkpoint, skip):
    accessed = checkpoint
    try:
        while accessed < max_access_count:
            #print(accessed)
            if accessed not in skip:
                tree = ET.parse(path + xml_files[accessed])
                root = tree.getroot()
                attributes = [xml_files[accessed]]
                #print(xml_files[accessed])
                for tag in data_type:
                    tags = tag.split('-')
                    if len(tags) == 1:
                        result = get_text(root, tags[0])
                    else:
                        result = get_multiple_text(root, tags[0], tags[1:])
                    
                    if data_type[tag] == 'list':
                        attributes.append(str(result).replace('set()', '{}').replace("\"", "\'").replace(' ,', '').replace(',,',','))
                    elif data_type[tag] == 'text':
                        if len(result) > 0:
                            attributes.append(list(result)[0])
                        else:
                            attributes.append(None)
                po.insert_value(cur, table_name, len(data_type), tuple(attributes))
                if accessed % commit_threshold == 0:
                    po.commit_transaction(conn)
                    checkpoint = accessed
    
                    
                    
            accessed += 1
            
    except Exception as e:
        #print(e)
        po.rollback_transaction(conn)
        skip.append(accessed)
        cur = po.start_transaction(conn)
        integrate_data(path, xml_files, max_access_count, commit_threshold, data_type, conn, cur, table_name, checkpoint, skip)

def interactive(cur):
    user_input = None
    while True:
        user_input = input("Please choose from the following options:\n1.Insert Query\n2.Exit\n")
        if user_input == "1":
            query = input("Please type your query in a single line\n")
            try:# DELIMITER
                sql = "COPY (" + query + ") TO STDOUT WITH CSV DELIMITER  ',' HEADER" 
                csv = input("Please specify csv name for the output (e.g. results.csv):\n")

                with open(csv, "w") as file:
                    cur.copy_expert(sql, file)
                print("Results Generated as " + csv)
            except Exception as e:
                print(e)
                print("ERROR DETECTED ! Please check your query and csv name")
                po.rollback_transaction(conn)
                cur = po.start_transaction(conn)
                os.remove(csv)
                file.close()
                continue
        elif user_input == "2":
            print("Thank you for using this interactive SQL transaction")
            break
        else:
            print("Please check your query or csv file name!")
                
                
        
if __name__ == "__main__":
    data_source_path = './data/'
    xml_files = get_xml_files(data_source_path)
    
    file_capacity = 1500#len(xml_files)
    threshold = int(0.5*file_capacity)
    commit_threshold = int(0.05*file_capacity)
    table_name = "ct_data"
    
    print("processing tags..")
    get_tags(data_source_path, xml_files, file_capacity)
    matched_tags = find_matches(tags_dict, threshold)
    
    
    print("initializing and updating data type..")
    ## Initialize and Identify data type
    data_type = dict()
    skip = []
    initialize_type(matched_tags)
    update_data_type(data_source_path, xml_files, int(file_capacity*0.2), matched_tags, data_type)

    ## Connect to database
    conn = po.connect_database()
    cur = po.start_transaction(conn)
    print("constructing table.. TABLE_NAME: " + table_name)
    ## Construct relation
    po.construct_case_table(cur, table_name, "nct_id", "text", data_type)

    ## Commit changes to DB
    po.commit_transaction(conn)

    print("integrating data to " + table_name)
    ## Insert value to database
    integrate_data(data_source_path, xml_files, file_capacity, commit_threshold, data_type, conn, cur, table_name, 0, skip)

    ## Commit changes to DB
    po.commit_transaction(conn)

    print("data has been successfully integrated!\n\n")
    print("Welcome to the interactive SQL transaction")
    interactive(cur)
    

    ## Disconnect from DB
    po.end_transaction(cur, conn)
