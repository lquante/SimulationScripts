#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  imports
import argparse
import os
import sys
import imp
import yaml
import pyaml
import glob
import numpy
from shutil import copyfile

# defining parser & properties of ensemble run

parser = argparse.ArgumentParser(description="Schedule an ensemble of model runs")
parser.add_argument(
    "--model",
    type=str,
    help="Path to modell binary (default: CURRENT/model)",
)
parser.add_argument("--cpus", type=int, default=16, help="Number of cpus (default: 16)")
parser.add_argument(
    "--maxtime",
    type=str,
    default="1-00:00:00",
    help="Max runtime (default: 1-00:00:00)",
)
parser.add_argument(
    "--settings", type=str, default="settings.yml", help="Settings file"
)
parser.add_argument(
    "--parameters", type=str, default="parameters.py", help="parameters file"
)
# variants for execution
parser.add_argument("--local", action="store_true", help="run locally, not on cluster")
parser.add_argument("--dry", action="store_true", help="dry run (do not run model)")
parser.add_argument("--verbose", action="store_true", help="be verbose")

args = parser.parse_args()
# default settings
if not args.model:
    args.model = os.path.join(os.getcwd(), "model")

if not os.path.exists(args.model):
    exit("Model binary '{}' not found".format(args.model))
if os.path.exists(args.parameters):
    exit("Parameter '{}' file not found".format(args.parameters))

# run parameters
indices = []
sys.dont_write_bytecode = True
parameters = imp.load_source("parameters", args.parameters).parameters
# TODO replace imp.load with non depreciated method
run_cnt = 0

# writhe paths to yml nodes
def set_in_yml(paths, value):
    global yml_nodes
    for p in paths:
        node = yml_nodes
        nodes = p.split(".")
        for n in nodes[:-1]:
            try:
                n = int(n)
            except ValueError:
                if not n in node:
                    exit(f"Path '{p}' not found!")
            node = node[n]
        n = nodes[-1]
        if not n in node:
            exit(f"Path '{p}' not found!")
        node[n] = value

#iterate run
def next_step():
    for i, ind in enumerate(indices):
        indices[i] += 1
        if indices[i] < len(parameters[i]["values"]):
            set_in_yml(parameters[i]["paths"], parameters[i]["values"][indices[i]])
            return True
        else:
            indices[i] = 0
            set_in_yml(parameters[i]["paths"], parameters[i]["values"][indices[i]])

# describe run
def run_description():
    res = ""
    for i, ind in enumerate(indices):
        res += "{} = {}\n".format(parameters[i]["name"], parameters[i]["values"][ind])
    return res

# describe run in CSV
def run_description_csv(run_label):
    res = f'"{run_label}"'
    for i, ind in enumerate(indices):
        res += ',"{}"'.format(parameters[i]["values"][ind])
    return res

# prepare run
def schedule_run():
    global run_id
    global run_index
    global settings_yml
    global run_cnt
    run_label = f"run{run_id}"
    if os.path.exists(run_label):
        run_id += 1
        return
    os.mkdir(run_label)
    desc = run_description()
    f = open(f"{run_label}/parameters.txt", "w")
    f.write(desc)
    f.close()
    run_index.write(run_description_csv(run_label))
    run_index.write("\n")
    with open(f"{run_label}/settings.yml", "w") as f:
        f.write(pyaml.dump(settings_yml))
        for nc in glob.glob("*.nc"):
            copyfile(nc, "%s/%s" % (run_label, nc))
    run_id += 1
    if args.dry:
        return
    if args.local:
        os.system(
            "cd {} && {} {} >output.txt 2>errors.txt && cd ..".format(
                run_label, args.model, args.settings
            )
        )
    else:
        cmd = (
            "start-model"
            + " --model {}".format(args.model)
            + " --cpus {}".format(args.cpus)
            + " --jobname '{}/{}'".format(os.path.basename(os.getcwd()), run_label)
            + " --logdir {}".format(run_label)
            + " --maxtime {}".format(args.maxtime)
            + " --queue {}".format(args.queue)
            + " --workdir {}".format(run_label)
            + " {}/settings.yml".format(run_label)
        )

        if args.verbose:
            print(cmd)

        os.system(cmd)
        run_cnt += 1


num = 1
for par in parameters:
    if "values" not in par:
        exit("Key 'values' not found")
    if "name" not in par:
        exit("Key 'name' not found")
    if "paths" not in par:
        exit("Key 'paths' not found")
    num *= len(par["values"])
    indices.append(0)

if num > 1:
    print("Number of runs to be scheduled: %s" % num)
    sys.stdout.write("Run? y/N : ")
    if sys.version_info >= (3, 0):
        if input() != "y":
            exit("Aborted")
    else:
        if raw_input() != "y":
            exit("Aborted")

with open(args.settings, "r") as f:
    settings_yml = yaml.load(f.read())
yml_nodes = settings_yml

run_index = open("index.csv", "w")
run_index.write('"Run"')
for i in range(len(indices)):
    run_index.write(',"{}"'.format(parameters[i]["name"]))
    set_in_yml(parameters[i]["paths"], parameters[i]["values"][0])
run_index.write("\n")
run_id = 0
schedule_run()
while next_step():
    schedule_run()
run_index.close()

if num > 1:
    print(f"Scheduled {run_cnt} runs")