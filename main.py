import gen

if __name__ == '__main__':

    # Parameters
    record_sizes = [1e3, 64e3, 1024e3, 2048e3]
    match_percentage = 0.05
    data_length = 100
    regex = '.*[tT][eE][rR][aA][tT][iI][dD][eE][ \t\n]+[dD][iI][vV][iI][nN][gG][ \t\n]+([sS][uU][bB])+[sS][uU][rR][fF][aA][cC][eE].*'
    parquet_chunksize = 1e6
    parquet_compression = 'none'
    outdir = './diving'

    gen.regex_strings(record_sizes, match_percentage, data_length, regex, parquet_chunksize, parquet_compression, outdir)