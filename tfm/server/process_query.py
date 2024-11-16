def process_query(dataset, query):
    optimized = dataset.optimized
    if optimized:
        dataframe = dataset.read_optimized(row_input = query.get('row'),
                                           rows_input = query.get('rows'))
    else:
        dataframe = dataset.read()

    if query.get('random') is True:
        if query.get('number_of_samples'):
            dataframe = dataframe.sample(query.get('number_of_samples'))
        else:
            dataframe = dataframe.sample(1)
    if query.get('column'):
        dataframe = dataframe[query['column']]
    if query.get('columns'):
        dataframe = dataframe[query['columns']]
    if query.get('rows') :
        step = None
        if query.get('step'):
            step = query.get('step')
        else:
            step = 1
        # -1 to make it not include the last number
        dataframe = dataframe.loc[query["rows"][0]:(query["rows"][1] - 1):step]
        # Reindex to give a Series/DataFrame with indexes starting from 0
        dataframe = dataframe.reset_index(drop=True)

    if query.get('row') is not None:
        dataframe = dataframe.loc[query["row"]]

    return dataframe
