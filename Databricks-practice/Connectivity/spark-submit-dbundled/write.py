def write(df, tgt_dir, file_format):
    df.coalesce(16). \
        write. \
        partitionBy('year', 'month', 'day'). \
        mode('append'). \
        format(file_format). \
        save(tgt_dir)
