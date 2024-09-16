from kfp import compiler
from pipeline import my_pipeline


compiler.Compiler().compile(pipeline_func=my_pipeline, package_path="sample_pipeline.json")
