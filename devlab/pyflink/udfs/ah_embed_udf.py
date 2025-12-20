#######################################################################################################################
#
#
#   Project             :   Account Holder... Streaming account holder embedding vectorizer 
#
#   File                :   ah_embed_udf.py
#
#   Created             :   8 Dec 2025
#
#   Description         :   Calculate vector values for record arriving in source table, outputting the columns + vector 
#                       :   to a new target table
#                       :
#   Misc Reading        :   https://www.kdnuggets.com/2025/05/confluent/a-data-scientists-guide-to-data-streaming
#                       :   https://www.youtube.com/watch?v=Tn4n9xKE1ug
#                       :   https://www.decodable.co/blog/a-hands-on-introduction-to-pyflink
#
#
#   in Jobmanager i.e.  (NOT SURE)
#
#   /opt/flink/bin/flink run \
#        -m jobmanager:8081 \
#        -py /pyflink/udfs/ah_embed_udf.py \
#        -j /opt/flink/lib/flink-sql-connector-postgres-cdc-3.5.0.jar \
#        -j /opt/flink/lib/flink-python-1.20.1.jar \
#
#   /opt/flink/bin/flink run \
#        -m jobmanager:8081 \
#        -py /pyflink/udfs/ah_embed_udf.py
#
########################################################################################################################
__author__      = "George Leonard"
__email__       = "georgelza@gmail.com"
__version__     = "1.0.0"
__copyright__   = "Copyright 2025, - G Leonard"

"""
Standalone Python UDF for generating account holder embeddings
Can be registered in Flink SQL using CREATE TEMPORARY FUNCTION
"""

from pyflink.table.udf import udf
from pyflink.table import DataTypes
import torch
from sentence_transformers import SentenceTransformer

# Embedding model configuration
model = 'sentence-transformers/all-MiniLM-L6-v2'
#dimensions = 384


@udf(result_type=DataTypes.ARRAY(DataTypes.FLOAT()))
def generate_ah_embedding(dimensions,
                          firstname, 
                          lastname, 
                          dob, 
                          gender, 
                          children, 
                          address, 
                          accounts, 
                          emailaddress, 
                          mobilephonenumber):
    """
    Generate 384-dimensional embeddings for user profiles using sentence-transformers.
    
    This UDF lazy-loads the model to avoid serialization issues.

    Args:
        embedding dimensions,
        accountHolder details,
        
    Returns:
        array of float: ....
    """
    
    
    
    # Use a class variable to cache the model across invocations
    if not hasattr(generate_ah_embedding, 'model'):
        generate_ah_embedding.model = SentenceTransformer(model)
        generate_ah_embedding.model.eval()
    
    # Build text representation of the user profile
    transaction_parts = []
    
    if firstname:
        transaction_parts.append(f"First name: {firstname}")
    if lastname:
        transaction_parts.append(f"Last name: {lastname}")
    if dob:
        transaction_parts.append(f"Date of birth: {dob}")
    if gender:
        transaction_parts.append(f"Gender: {gender}")
    if children is not None:
        transaction_parts.append(f"Children: {children}")
    if address:
        transaction_parts.append(f"Address: {address}")
    if accounts:
        transaction_parts.append(f"Accounts: {accounts}")
    if emailaddress:
        transaction_parts.append(f"Email: {emailaddress}")
    if mobilephonenumber:
        transaction_parts.append(f"Mobile: {mobilephonenumber}")
    
    # Combine all parts into a single text
    transaction_text = ". ".join(transaction_parts)
    
    # Handle empty profile
    if not transaction_text.strip():
        transaction_text = "Unknown account holder profile"
    
    # Generate embedding
    with torch.no_grad():
        embedding = generate_ah_embedding.model.encode(transaction_text, convert_to_numpy=True)
    
    # Truncate to target dimensions
    embedding = embedding[:dimensions]
    
    # Convert to list of floats for Flink
    return embedding.tolist()
# end generate_ah_embedding