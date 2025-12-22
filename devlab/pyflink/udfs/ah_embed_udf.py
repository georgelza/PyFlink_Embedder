#######################################################################################################################
#
#
#   Project             :   Account Holder... Streaming account holder embedding vectorizer 
#   File                :   ah_embed_udf.py
#   Created             :   8 Dec 2025
#   Description         :   Calculate vector values for record arriving in source table, outputting the columns + vector 
#                       :   to a new target table
#                       :
#   Misc Reading        :   https://www.kdnuggets.com/2025/05/confluent/a-data-scientists-guide-to-data-streaming
#                       :   https://www.youtube.com/watch?v=Tn4n9xKE1ug
#                       :   https://www.decodable.co/blog/a-hands-on-introduction-to-pyflink
#
#
#
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
from sentence_transformers import SentenceTransformer
import torch
import sys


# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------
MODEL           = 'sentence-transformers/all-MiniLM-L6-v2'


# --------------------------------------------------------------------------
# Define UDF for embedding generation
# --------------------------------------------------------------------------
@udf(result_type=DataTypes.ARRAY(DataTypes.DOUBLE()))
def generate_ah_embedding(target_dimensions
                          ,firstname 
                          ,lastname 
                          ,dob
                          ,gender
                          ,children 
                          ,address 
                          ,accounts
                          ,emailaddress
                          ,mobilephonenumber):
    """
        Generate <target_dimensions> dimensional embeddings for user profiles using sentence-transformers.
        This UDF lazy-loads the model to avoid serialization issues.

        Args:
            embedding dimensions,
            accountHolder details,
            
        Returns:
            array of float: ....
    """
    
    try:
    
    
        # Use a class variable to cache the model across invocations
        if not hasattr(generate_ah_embedding, 'model'):
            generate_ah_embedding.model = SentenceTransformer(MODEL)
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
            #print(embedding)

        final_size = int(target_dimensions)
                                
        # Convert to list of floats for Flink
        return embedding[:final_size].astype('float64').tolist()
    
    except Exception as e:
        print(f"UDF Error: {str(e)}", file=sys.stderr)
        raise e

# end generate_ah_embedding