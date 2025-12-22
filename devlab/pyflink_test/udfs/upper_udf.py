#######################################################################################################################
#
#   Project             :   PyFlink UDF Lab
#   File                :   upper_udf.py
#   Description         :   Scalar UDF to uppercase a string.
#
########################################################################################################################
__author__      = "George Leonard"
__email__       = "georgelza@gmail.com"
__version__     = "1.0.0"
__copyright__   = "Copyright 2025, - G Leonard"

from pyflink.table import DataTypes
from pyflink.table.udf import udf

# Define the scalar UDF using the @udf decorator
@udf(result_type=DataTypes.STRING())
def uppercase_str(input_string: str) -> str:
    """
    Converts input string to uppercase.
    
    Args:
        input_string: String to convert
        
    Returns:
        Uppercase version of input string, or None if input is None
    """
    if input_string is None:
        return None
    return input_string.upper()