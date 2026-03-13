import json

# for asserting it exits the program
def assertExit(test_func, expected_code=1):
    try:
        test_func()
    except SystemExit as e:
        assert e.code == expected_code, f"Expected exit code {expected_code}, got {e.code}"
    else:
        raise AssertionError("SystemExit was not raised")

#to read txt file
def read_txt(file_name):

    content = ""
    with open(file_name, 'r', encoding='utf-8') as file:
        content = file.read() 
    return content

#to read yml file
def read_yml(file_name):

    content = ""
    with open(file_name, 'r', encoding='utf-8') as file:
        content = file.read() 
    return content


#to read json file
def read_json(local_file_path):

    with open(local_file_path, 'r', encoding='utf-8') as file:
        lst = json.load(file)
    return lst

# to write yml
def write_yml(local_file_path, content):
    with open(local_file_path, "w", encoding="utf-8") as f:
            f.write(content)