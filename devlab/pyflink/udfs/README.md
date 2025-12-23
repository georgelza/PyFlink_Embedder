## Define our 2 Python routines, to calculate embedding vectors.

### Calculate embedding vectors for accountholders

pyflink/udfs/ah_embed_udf.py

### Calculate embedding vectors for transactions

pyflink/udfs/txn_embed_udf.py


## MODEL

We used the The `all-MiniLM-L6-v2` for both the accountHolder and tansaction embedding.

The `all-MiniLM-L6-v2` model is one of the most lightweight and efficient sentence-transformer models. Its memory footprint is very small, making it suitable for resource-constrained environments like Docker containers or small cloud instances.

Take note of the below numbers and how they impact the settings in `devlab/conf/confi.yaml`


### 1. Base Memory Requirements

For a standard Python environment (using `sentence-transformers` with `PyTorch`), the memory requirements are:

Model Disk Size: ~80 MB – 91 MB.

VRAM/RAM to load (Inference):

FP32 (Default): ~87 MB to 150 MB.

FP16: ~44 MB.

INT8 (Quantized): ~22 MB.

Peak Runtime Memory: Typically 200 MB to 500 MB total for the Python process, including the model, dependencies (PyTorch/Transformers), and input/output buffers.


### 2. Practical Docker/Container Limits

While the model itself is small, the `Python` runtime and `PyTorch library` add overhead.

Minimum: A container with 512 MB of RAM can often run this model for basic tasks.

Recommended: 1 GB – 2 GB of RAM is ideal to ensure stability during bulk encoding and to avoid OOM (Out of Memory) kills, especially if you are processing large batches of text at once.


### 3. Scaling Factors

Your memory usage will increase based on:

Batch Size: Larger batches in model.encode() require more memory to store temporary activation tensors.

Output Storage: If you encode a large dataset (e.g., millions of rows) and keep all embeddings in memory simultaneously, you will need approximately 1.5 KB per embedding (384 dimensions * 4 bytes/float32).

Example: 1 million embeddings ≈ 1.5 GB of additional RAM.

Why this matters for your Flink Job

In your current Flink setup, you are running this model inside a Python UDF.

Python Worker Overhead: Each Flink `TaskManager` starts a separate Python process. If your `TaskManager` has 4 slots, it may start 4 Python workers, each loading its own copy of this model (~400MB x 4 = 1.6GB overhead).

Managed Memory: Ensure your `config.yaml` leaves enough `Off-Heap Memory` for these `Python` processes, as they live outside the JVM heap. A setting of `taskmanager.memory.task.off-heap.size: 1gb` is usually the "sweet spot" for this specific model.

