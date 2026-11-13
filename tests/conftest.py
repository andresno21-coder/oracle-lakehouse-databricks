import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "proceso"))
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
