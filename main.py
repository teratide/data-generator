import gen

if __name__ == '__main__':

    # Parameters
    record_sizes = [1e3, 2e3, 4e3, 8e3, 16e3, 32e3, 64e3, 128e3, 256e3, 512e3, 1024e3, 2048e3]
    parquet_chunksizes = [1e3, 1e6]
    match_percentage = 0.05
    data_length = 100
    regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'
    parquet_compression = 'none'
    outdir = './diving'

    for parquet_chunksize in parquet_chunksizes:
        gen.regex_strings(record_sizes, match_percentage, data_length, regex, parquet_chunksize, parquet_compression, outdir)