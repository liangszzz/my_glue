# flow1 Etl001

1.input file

    input_file_path example:
        s3://bucket/path_name/file_id/yyyyMMdd/file_id_yyyyMMdd_001.csv.gz
        s3://bucket/path_name/file_id/yyyyMMdd/file_id_yyyyMMdd_001.tsv.gz
        s3://bucket/path_name/file_id/yyyyMMdd/file_id_yyyyMMdd_001.txt.gz
        s3://bucket/path_name/file_id/yyyyMMdd/file_id_yyyyMMdd_001.fixed.gz

    output_file_path example:s3://bucket/path_name_hander/file_id/yyyyMMdd/file_id_yyyyMMdd_001.gz

    a.csv
    b.tsv
    c.text
    d.fixed lengrh text csv
    e.fixed lengrh text shift-jis

handle:

    a.trim
    b.decimal parse
    c.date format
    d.irregule filter

export to parquet

    a.export parquet
