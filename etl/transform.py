import pandas as pd
import numpy as np
#paquetes internos
import handlerErrors as handlerErrors

def drop_duplicated_by_column(dataFrame,column):
    try:
        """
        Elimina filas duplicadas del DataFrame basándose en una columna específica.

        Args:
            dataFrame (pd.DataFrame): El DataFrame de entrada.
            column (str): El nombre de la columna sobre la que se eliminarán duplicados.

        Returns:
            pd.DataFrame: El DataFrame sin duplicados en la columna dada.
        """
        result = dataFrame.drop_duplicates(subset=[column],keep='first')
        return result
    except Exception as e:
        handlerErrors.logging.critical(f'Error intentando borrar duplicados {e}')


def transform_to_load_tbl_game(DataFrame,dataFromTable):
    try:
        dataFromTable = pd.DataFrame(dataFromTable,columns=['id_tiempo','anio'])
        df_merge = pd.merge(DataFrame,dataFromTable,left_on='Año',right_on='anio',how='inner').copy()
        df_merge.loc[:,'Nombre'] = df_merge['Nombre'].astype(str)

        df_name_cut, log = cut_string_surpassed(df_merge,'Nombre',100)

        #Traemos los resultados del dataframe que queremos enviar, tecnicamente filtrar
        df_result = df_name_cut[['Nombre','id_tiempo']]

        #Hacemos un log de los nombre que sufrieron modificacion (truncate) para mantener una auditoria
        log.to_csv('logs/truncated_names',sep='|',index=False)


        """
        Para pasar de un DATAFRAME a unas tuplas o lista y se pueda guardar en base de datos
        ya que el dataframe si lo enviamos directamente al insert, no lo acepta ya que se envia
        con el valor object, por ende usamos la opcion que estamos usando ahora que es mas eficiente
        o se puede usar esta otra:
        df_result = df_result.values.tolist()
        """
        df_result = [(row.Nombre,row.id_tiempo) for row in df_result.itertuples(index=False)]

        return df_result
    except Exception as e:
        handlerErrors.logging.critical(f'Error en transformacion para la tabla game {e}')


def transform_to_load_tbl_ventas(DataFrame,DataTables):
    df_final = DataFrame.copy()

    df_final = df_final.rename(columns={'Ventas Global':'Ventas_Global'})

    if 'FromEditorial' in DataTables:
        df_edito = pd.DataFrame(DataTables['FromEditorial'],columns=['id_edito','nombreEdi'])
        df_final = pd.merge(df_final,df_edito,left_on='Editorial',right_on='nombreEdi',how='inner')
    if 'FromPlataforma' in DataTables:
        df_plata = pd.DataFrame(DataTables['FromPlataforma'],columns=['id_plata','nombrePla'])
        df_final = pd.merge(df_final,df_plata,left_on='Plataforma',right_on='nombrePla',how='inner')
    if 'FromTime' in DataTables:
        df_time = pd.DataFrame(DataTables['FromTime'],columns=['id_tiempo','anio'])
        df_final = pd.merge(df_final,df_time,left_on='Año',right_on='anio',how='inner')
    if 'FromGame' in DataTables:
        df_game = pd.DataFrame(DataTables['FromGame'],columns=['id_game','nombreGam'])
        df_game,log = cut_string_surpassed(df_game,'nombreGam',100)
        df_final = pd.merge(df_final,df_game,left_on='Nombre',right_on='nombreGam',how='inner')


    df_to_ventas = df_final[['id_game','id_edito','id_plata','id_tiempo','Ventas_Global']]

    df_to_ventas = [(row.id_game,row.id_edito,row.id_plata,row.id_tiempo,row.Ventas_Global) for row in df_to_ventas.itertuples(index=False)]

    return df_to_ventas


def cut_string_surpassed(df,column,max_lenght):
    try:
        df[column] = df[column].astype(str)

        #Guardo true o false de la condicion 
        mask = df[column].str.len() > max_lenght

        #Obtengo los valores que son true
        log_df = df[mask][[column]].copy()
        log_df['Original'] = log_df[column]

        #Trunco DataFrame (nombre) que sera el log para validar los nombre modificados!
        log_df['Truncate'] = log_df[column].str.slice(0,max_lenght)
        
        log_df = log_df.drop(column,axis=1,inplace=False)
        
        #Trunco DataFrame que se enviara a la DB
        df.loc[mask,column] = df.loc[mask,column].str.slice(0,max_lenght)

        return df, log_df
    except Exception as e:
        handlerErrors.logging.critical(f'Error cortando registro mayor al max_length de la tabla')