#Paquetes internos
from etl import extract,transform,load,handlerErrors

path_file = './source/VentasVideojuegos.xlsx'


def run():

    #Extraccions
    dataFromFile = extract.readFile(path_file)

    #Trasformations
    """Delete duplicates from the column YEAR from dataFrame"""
    df_year = transform.drop_duplicated_by_column(dataFromFile,'Año')['Año']
    dfYearToTuple = [(int(año),)for año in df_year]

    #Load
    load.load_tbl_d_tiempo(dfYearToTuple)


    #Extraccions
    dataFromTableTime = extract.get_data_from_DB('tbl_dimension_tiempo',['id_tiempo','anio'])
    #Trasformations
    df_merge_to_load_game = transform.transform_to_load_tbl_game(dataFromFile,dataFromTableTime)

    #Load
    load.load_tbl_d_game(df_merge_to_load_game)

    #Transformations
    df_plataform = transform.drop_duplicated_by_column(dataFromFile,'Plataforma')['Plataforma']
    df_plataform_tuple = [(str(platform),)for platform in df_plataform]

    #Load
    load.load_tbl_d_plataform(df_plataform_tuple)

    #Trasformations
    df_editiorial = transform.drop_duplicated_by_column(dataFromFile,'Editorial')['Editorial']
    df_editiorial_tuple = [(str(edito),) for edito in df_editiorial]

    #load
    load.load_tbl_d_editorial(df_editiorial_tuple)

    #Extraccions
    dataFromTableGame = extract.get_data_from_DB('tbl_dimension_game',['id_game','nombre'])
    dataFromTableEdito = extract.get_data_from_DB('tbl_dimension_editorial',['id_edito','nombre'])
    dataFromTablePlata = extract.get_data_from_DB('tbl_dimension_plataforma',['id_plata','nombre'])

    joinData = {
        'FromTime':dataFromTableTime,
        'FromGame': dataFromTableGame,
        'FromEditorial': dataFromTableEdito,
        'FromPlataforma': dataFromTablePlata
    }

    #Trasformations
    result_load_ventas = transform.transform_to_load_tbl_ventas(dataFromFile,joinData)

    #Load
    load.load_tbl_fact_venta(result_load_ventas)


if __name__ == '__main__':
    run()
else:
    handlerErrors.logging.error(f'Ups,something went wrong with the module!')