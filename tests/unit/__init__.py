import sys
from os.path import abspath, dirname, join

TOP = dirname(dirname(abspath(__file__)))

sys.path.append(join(TOP, "taskshed"))
