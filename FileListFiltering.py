#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  imports

# script to filter a *.yml list of files for filenames only differentiated by YYYY-YYYY identifier

import argparse
import os
import re

from ruamel.yaml import ruamel

# argument parser definition
parser = argparse.ArgumentParser(description="Calculate some analysis metrics on specified files")
# path to *.yml file with settings to be used


parser.add_argument(
    "--data"
    , type=str,
    required=True,
    help="YML list of datafile(s)"
)

args = parser.parse_args()

# load file with data filepaths

yaml = ruamel.yaml.YAML()
with open(args.data, 'r') as stream:
    try:
        data = yaml.load(stream)
    except yaml.YAMLError as exc:
        print(exc)


list_of_models = []

for i_data in data:
    model = re.search('(.*/)(output_)(.*)(_\d{4}\d{4})(.nc)$', i_data)
    if (model):
        model_string = model.group(3)
    else:
        model_string = "model_not_identified"
    list_of_models.append(model_string)

set_of_models = set(list_of_models)

sorted_models = {}
for i_model in set_of_models:
    files_from_model = []
    for i_data in data:
        model = re.search('(.*/)(output_' + i_model + '_\d{4}\d{4})(.nc)$', i_data)
        if (model):
            model_string = model.string
            files_from_model.append(model_string)
        else:
            model_string = "model_not_identified"
    sorted_models[i_model] = files_from_model

# export as yml files
# get dir of data
outputdir = os.path.dirname(args.data)

for i_model in set_of_models:
    os.chdir(outputdir)
    yaml = ruamel.yaml.YAML()
    yaml.default_flow_style = None
    with open("data_" + i_model + ".yml", "w") as output:
        yaml.dump(sorted_models[i_model], output)
