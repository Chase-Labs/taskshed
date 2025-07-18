from os.path import abspath, dirname, join
from sys import path

TOP = dirname(dirname(dirname(abspath(__file__))))
path.append(TOP)
path.append(join(TOP, "taskshed"))
