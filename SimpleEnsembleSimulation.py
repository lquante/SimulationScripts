#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#  imports
import argparse
import os
import re
import shutil
import sys

from pip._vendor.distlib.compat import raw_input
from ruamel.yaml import ruamel

# defining parser & properties of ensemble run

parser = argparse.ArgumentParser(description="Schedule an ensemble of model runs")
parser.add_argument(
    "--model",
    type=str,
    help="Path to model binary (default: CURRENT/model)",
)
parser.add_argument("--cpus", type=int, default=16, help="Number of cpus (default: 16)")
parser.add_argument("--memory", type=int, default=4000, help="RAM per job in MB (default: 4000)")
parser.add_argument(
    "--time",
    type=str,
    default="1-00:00:00",
    help="Max runtime (default: 1-00:00:00)",
)

parser.add_argument("--queue", type=str, default="short", help="queue to be used on the cluster")

parser.add_argument(
    "--settings", type=str, help="File containing paths to individual settings files"
)

parser.add_argument(
    "--dependency", type=int, help="JOB ID that needs to finish before Job starts"
)

# variants for execution
parser.add_argument("--local", action="store_true", help="run locally, not on cluster")
parser.add_argument("--dry", action="store_true", help="dry run (do not run model)")
parser.add_argument("--python", action="store_true", help="run model with python")
parser.add_argument("--verbose", action="store_true", help="be verbose")
# initialize argument parser
args = parser.parse_args()
# default model location
if not args.model:
    args.model = os.path.join(os.getcwd(), "model")
if not os.path.exists(args.model):
    exit("Model binary '{}' not found".format(args.model))

# default location of settings collection
if not args.settings:
    args.settings = os.path.join(os.getcwd(), "/list_of_settings.yml")
if not os.path.exists(args.settings):
    exit("List of settings '{}' not found".format(args.settings))

# open list of settings
yaml = ruamel.yaml.YAML()
with open(args.settings, 'r') as stream:
    list_of_settings = yaml.load(stream)
# determine number of runs for which settings are provided
numberOfRuns = len(list_of_settings)


# prepare run
def schedule_run():
    global settings_yml
    global run_cnt
    # load path of settingsfile
    run_settings_file = list_of_settings[run_cnt]
    run_settings_paths = os.path.dirname(run_settings_file)
    # create run label
    model = re.search('(.*/)(settings_)(.*)(.yml)$', run_settings_file)
    if (model):
        identifier = model.group(3)
    else:
        identifier = "model_not_identified"
    run_label = os.path.join(run_settings_paths, identifier + "_run_" + str(run_cnt))
    # check if directory for run already exisits
    if os.path.exists(run_label):
        run_cnt += 1
        return
    # create directory for run
    os.mkdir(run_label)
    # copy settings file

    path_settings = os.path.join(run_label + "/settings.yml")
    shutil.copy(run_settings_file, path_settings)
    if args.dry:
        return
    if args.local:
        # shell script needs to be in same directory as this script
        if (args.python):
            cmd = ("./local-model"
               + " --python 1"
               + " --model {}".format(args.model)
               + " --logdir {}".format(run_label)
               + " --workdir {}".format(run_label)
               + " {}".format(path_settings))
        else:
            cmd = ("./local-model"
                   + " --model {}".format(args.model)
                   + " --logdir {}".format(run_label)
                   + " --workdir {}".format(run_label)
                   + " {}".format(path_settings))
        if args.verbose:
            print(cmd)
            print(os.getcwd())
        os.system(cmd)
        run_cnt += 1

    else:
        # shell script needs to be in same directory as this script
        if (args.python & args.dependency):
            cmd = ("./start-model"
                   + " --model {}".format(args.model)
                   + " --dependency {}".format(args.dependency)
                   + " --python 1"
                   + " --cpus {}".format(args.cpus)
                   + " --memory {}".format(args.memory)
                   + " --jobname '{}'".format(run_label)
                   + " --logdir {}".format(run_label)
                   + " --time {}".format(args.time)
                   + " --queue {}".format(args.queue)
                   + " --workdir {}".format(run_label)
                   + " {}".format(path_settings)
                   )

        if (args.python):
            cmd = ("./start-model"
                   + " --model {}".format(args.model)
                   + " --python 1"
                   + " --cpus {}".format(args.cpus)
                   + " --memory {}".format(args.memory)
                   + " --jobname '{}'".format(run_label)
                   + " --logdir {}".format(run_label)
                   + " --time {}".format(args.time)
                   + " --queue {}".format(args.queue)
                   + " --workdir {}".format(run_label)
                   + " {}".format(path_settings)
                   )
        else:
            cmd = ("./start-model"
                   + " --model {}".format(args.model)
                   + " --cpus {}".format(args.cpus)
                   + " --memory {}".format(args.memory)
                   + " --jobname '{}'".format(run_label)
                   + " --logdir {}".format(run_label)
                   + " --time {}".format(args.time)
                   + " --queue {}".format(args.queue)
                   + " --workdir {}".format(run_label)
                   + " {}".format(path_settings)
                   )
        if args.verbose:
            print(cmd)
        os.system(cmd)
        run_cnt += 1


# execute runs
if numberOfRuns >= 1:
    print("Number of runs to be scheduled: %s" % numberOfRuns)
    sys.stdout.write("Run? y/N : ")
    if sys.version_info >= (3, 0):
        if input() != "y":
            exit("Aborted")
    else:
        if raw_input() != "y":
            exit("Aborted")
run_cnt = 0
schedule_run()
while run_cnt < numberOfRuns:
    schedule_run()
