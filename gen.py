import csv
import random
import string
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from xeger import Xeger
import progressbar
import os

# ------------|
# PARAMETERS  |
# ------------|
#
record_sizes = [1e3, 64e3, 1024e3]
match_percentage = 0.05
data_length = 100
regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'
parquet_chunksize = 1e6
parquet_compression = 'none'
outdir = './diving'

# Print parameters at the start of each run
print('\nDATASET GENERATOR')
print('Records:\t' + str(record_sizes))
print('Match percentage:\t' + str(match_percentage))
print('Data length:\t' + str(data_length))
print('Regular expression:\t' + repr(regex))
print('Parquet chuncksize:\t' + str(parquet_chunksize))
print('Parquet compression:\t' + parquet_compression + '\n')

# Main loop for each of the requested record sizes
print('Generating ' + str(len(record_sizes)) + ' datasets...\n')
for i, records in enumerate(record_sizes):

    # Convert records to int and format to readable string
    records = int(records)
    if (records >= 1e6):
        records_str = str(int(records / 1e6)) + 'M'
    else:
        records_str = str(records)

    # Create output directory if it does not yet exist
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    # Generate data and write to csv file
    file = outdir + '/data-' + str(records_str) + '.csv'
    print('Generating ' + file)
    with open(file, mode='w') as f:
        writer = csv.writer(f, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

        # Write the header row
        writer.writerow(['value', 'string'])

        # Generate each record
        for j in progressbar.progressbar(range(records)):

            val = random.randint(1, 100)

            # One in every (1/match_percentage) records should match the regex
            if (j % (1 / match_percentage) == 0):
                x = Xeger(limit=10)     # Limit repeats of regex operators such as * and + to 10
                data = x.xeger(regex)
                # Add padding such that the length of the data string is equal to the requested length
                data += ''.join(random.choices(string.ascii_uppercase + string.ascii_uppercase + string.digits + string.whitespace, k=max(0, data_length - len(data))))
            else:
                data = ''.join(random.choices(string.ascii_uppercase + string.ascii_uppercase + string.digits + string.whitespace, k=data_length))

            # Write the generated record
            writer.writerow([val, repr(data)])

    # Convert the csv to a parquet file
    parquet_file = outdir + '/data-' + str(records_str) + '.parquet'
    csv_stream = pd.read_csv(file, chunksize=parquet_chunksize, low_memory=False)
    print('Converting to ' + parquet_file)
    for j, chunk in progressbar.progressbar(enumerate(csv_stream)):

        # Initialize the parquet writer on the first chunk
        if j == 0:

            # Guess the schema of the csv file
            df = pa.Table.from_pandas(df=chunk)
            parquet_schema = df.schema

            # Initialize a Parquet writer
            parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression=parquet_compression)

        # Write csv chunk to the parquet file
        table = pa.Table.from_pandas(chunk, schema=parquet_schema)
        parquet_writer.write_table(table)

    # Close parquet writer after each chunk has been written
    parquet_writer.close()