from  ruamel.yaml import YAML

from pathlib import Path

in_file = Path(r'C:\Users\info\Pyspark\config\config_local.yaml')

def load_yaml():
    read_yaml= YAML(typ="safe")
    load_yaml_= read_yaml.load(in_file)
    return load_yaml_
# print(load_yaml())
