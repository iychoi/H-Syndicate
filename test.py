#! /usr/bin/env python

import shutil
import os
import os.path
import subprocess
import sys
import glob

# global variables
jar_dependency_dirs = [
'/home/iychoi/Desktop/hadoop-0.20.2-cdh3u5/lib/'
]

jar_copy_to = './dist/lib/'

def findjars(path):
    realpath = os.path.realpath(path)
    findpattern = realpath + "/*.jar"
    jars = glob.glob(findpattern)
    return jars

def dep():
    for jar_dir in jar_dependency_dirs:
	jars = findjars(jar_dir)
        for jar in jars:
            copyto = os.path.realpath(os.path.abspath(jar_copy_to))
            print "copying", jar
            shutil.copy2(jar, copyto)
    print "done!"

def run(args):
    programargs = ""
    for x in range(0, len(args)):
        arg = args[x]
        programargs += arg

    subprocess.call("time java -cp dist/lib/*:dist/HSynth.jar " + programargs, shell=True)

def runSequenceIDIndexBuilder():
    #remove outdir
    if os.path.exists('sample/output'):
        shutil.rmtree('sample/output')

    subprocess.call("time java -cp dist/lib/*:dist/HSynth.jar edu.arizona.cs.hsynth.hadoop.example.FastaSequenceIDIndexBuilder sample/input sample/output", shell=True)

def main():
    if len(sys.argv) < 2:
        print "command : ./test.py run <program arguments> ..."
        print "command : ./test.py dep"
        print "command : ./test.py seqidx"
    else:
        command = sys.argv[1]

        if command == "run":
            run(sys.argv[2:])
        elif command == "dep":
            dep()
        elif command == "seqidx":
            runSequenceIDIndexBuilder()
        else:
            print "invalid command"

if __name__ == "__main__":
    main()
