# udfs/my_functions.py

from pyflink.table.udf import udf
from pyflink.table import DataTypes

@udf(result_type=DataTypes.STRING())
def upper_case(s):
    return s.upper() if s else None

@udf(result_type=DataTypes.INT())
def calculate_score(value1, value2):
    return (value1 + value2) * 2


