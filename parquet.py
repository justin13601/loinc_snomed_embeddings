import os
from pyarrow.parquet import ParquetFile
import pyarrow as pa 
import pyarrow.parquet as pq

output_folder = 'loinc_labels_embeddings/'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

pf = ParquetFile('df_loinc_labels_embeddings.parquet')

for i, each_batch in enumerate(pf.iter_batches(batch_size = 4500)):
    df = pa.Table.from_batches([each_batch]).to_pandas() 
    print(f'Writing part_{i+1}.parquet')
    output_file_path = f'{output_folder}part_{i+1}.parquet'
    table = pa.Table.from_pandas(df)
    pq.write_table(table, output_file_path)

# import os
# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq

# # Specify the input and output file paths
# input_file_path = 'df_snomed_descriptions_embeddings.parquet'
# output_folder = 'split_files/'

# # Open the Parquet file using pyarrow
# parquet_file = pq.ParquetFile(input_file_path)

# # Get the schema and metadata from the original file
# schema = parquet_file.schema
# metadata = parquet_file.metadata

# # Set the maximum number of rows per smaller file
# max_rows_per_file = 100

# # Calculate the number of smaller files needed
# total_rows = metadata.num_rows
# num_files = (total_rows + max_rows_per_file - 1) // max_rows_per_file

# # Iterate through the original file and create smaller Parquet files
# for i in range(num_files):
#     start_row = i * max_rows_per_file
#     end_row = min((i + 1) * max_rows_per_file, total_rows)
    
#     # Read a chunk of data from the original file using pyarrow
#     table_chunk = parquet_file.read_row_group(i, columns=parquet_file.schema.names)
#     df_chunk = table_chunk.to_pandas()

#     # Write the chunk to a new Parquet file
#     print(f'Writing split_{i}.parquet')
#     output_file_path = f'{output_folder}split_{i}.parquet'
#     table = pa.Table.from_pandas(df_chunk, schema=schema, metadata=metadata)
#     pq.write_table(table, output_file_path)