import pandas as pd
import logging
import os
import hashlib
from datetime import datetime
import time
import re
import colorlog
import sys
import pyodbc
import shutil
import time
import csv
from worker import update_task_file

from config import (
    INVENTARIO_FILE, PADRON_FILE,
    ERROR_VALIDACION_DIR, DB_INGESTA,
    DUPLICATES_VALIDACION_FILE, SHARED_DIR, OUTPUT_DIR, REPORT_DIR,
    MUESTRA_DIR, ERROR_VALIDACION_SALIDA_FILE
)


pd.set_option('display.max_rows', None)    # Show all columns
pd.set_option('display.width', 100000)          # Set a large width for wide display
pd.set_option('display.expand_frame_repr', False)  # Prevent line wrapping

start_time = time.time()
current_datetime = datetime.fromtimestamp(start_time)
date_string = current_datetime.strftime("%Y-%m-%d %H:%M:%S")

# Set up logging
logger = colorlog.getLogger('main')
logger.setLevel(logging.DEBUG)

# Create handlers
handler = colorlog.StreamHandler(sys.stdout)
log_path = os.path.join(OUTPUT_DIR, "log.txt")
file_handler = logging.FileHandler(log_path, mode='a', encoding='utf-8')

logger.addHandler(handler)
logger.addHandler(file_handler)

sample_50_global = pd.DataFrame()

# all_good_global = True
# df_matches_global = pd.DataFrame()

def run_app(base_file, diccionario_file, seccion="app_bd", task_file=None, job_id=None):
        logger.info("----------------------------------------------------------------------------------------------------")
        logger.info(f"Proceso iniciado: validaciones iniciales para el instrumento con seccion={seccion} ejecutando..")
        logger.info(f"Job ID recibido: {job_id}")
        codigo_ficha = None
        if job_id:
                match = re.search(r"_(\d+)_", job_id)
                if match:
                    codigo_ficha = match.group(1)
                    logger.info(f"Código extraído del job_id: {codigo_ficha}")
                else:
                    logger.warning("No se pudo extraer código de job_id")
        # Asignamos rutas dinámicas
        dictionary = diccionario_file
        database = base_file
        

        try:
            # Paso 1: Validación inicials

            logger.info("----------------------------------------------------------------------------------------------------")
            logger.info(f"Reporte generado el: {date_string}")
            logger.info("Validaciones Iniciales")

            namemuestra_global = ''
            all_good_global = True
            errors = []
        
            def generate_hash():
                current_time = datetime.now()
                timestamp_str = f"{current_time.year}{current_time.month}{current_time.day}{current_time.second}"
                hash_object = hashlib.sha256()
                hash_object.update(timestamp_str.encode('utf-8'))
                hex_dig = hash_object.hexdigest()
                return hex_dig
            def count_empty_rows(df, column_names):
                """
                Counts the number of rows in a DataFrame that contain empty strings ('') or are null
                for any of the specified columns.

                Args:
                    df (pd.DataFrame): The input DataFrame.
                    column_names (list): A list of column names to check.

                Returns:
                    int: The number of rows with empty values or nulls in the specified columns.
                """
                # Create a mask to find rows with null or empty values in any of the specified columns
                mask = df[column_names].isnull() | (df[column_names] == '')
            
                # Sum the rows that have True in the mask (indicating empty or null values)
                return mask.any(axis=1).sum()
                # Check each column name
                # for col in column_names:
                #     # Use isnull() and eq('') to find rows that are either null or have an empty string
                #     mask = df[col].isnull().values | (df[col] == '').values
                #     if any(mask):
                #         return len(df[mask])  # Return the count of rows with empty values

                # # If no columns match, return 0
                # return 0
            ## git lab nooo funciona
            # def count_nulls_per_column(df, column_names):
            #     null_counts = ''
            #     for col in column_names:
            #         null_count = df[col].isnull().sum() 
            #         null_counts[col] = null_count
            #     return null_counts
            logger.info("Paso 1: Procesamiento de datos")
            def validate_regex_bd(df_bd,df_regex,time_str,hash_generated):
                series = df_regex[['Variable', 'regex_pattern','regex_description']].reset_index(drop=True)
                arrayRegex = series.values
                namemuestra_global = 'muestra_50_'+time_str+'_'+hash_generated+'.csv'
                df_matches_global=pd.DataFrame()
                df_matches_g_new = pd.DataFrame(columns=list(df_bd.columns))
                all_good_global=True

                results = []
                counter = 0
                df_bd_copy_error_macro = df_bd.copy()
                df_bd_copy_error_macro['ERROR_DESCRIPTIONS']=None
                

                for index, row in df_bd_copy_error_macro.iterrows():
                    errors_common_columns = []
                    cantidad_filas_incumplen_patron=0
                    for index2, row2 in series.iterrows():
                        column_name = row2['Variable']
                        pattern = row2['regex_pattern']
                        regex_description = row2['regex_description']
                        
                        element = row[column_name]

                        if not re.match(pattern, str(element)):
                                errors_common_columns.append(
                                    f"La columna { str(column_name) } no sigue el patrón : { str(regex_description) }"
                                )
                                cantidad_filas_incumplen_patron += 1

                        # if ( str(element) == 'nan') or  element is None or str(element).strip() == '' or re.sub(r'^\s*$', '', str(element)) == '' or str(element).replace('\\n', '') == '' or str(element).replace('\\r\\n', '')=='' or re.sub(r'\r\n', '', str(element))=='' or str(element).replace('\n', '').replace(' ', '')=='':
                        #     continue
                            # errors_common_columns.append(
                            #     f"La columna { str(column_name) } contiene valor nulo"
                            # )
                            # cantidad_filas_incumplen_patron += 1
                        # else:
                        #     if not re.match(pattern, str(element)):
                        #         errors_common_columns.append(
                        #             f"La columna { str(column_name) } no sigue el patrón : { str(regex_description) }"
                        #         )
                        #         cantidad_filas_incumplen_patron += 1

                    if(len(errors_common_columns)==0):
                        df_matches_g_new.loc[len(df_matches_g_new)] = row
                    else:
                        all_good_global = False
                    cadena_erccc = ",".join(str(item) for item in errors_common_columns)
                    df_bd_copy_error_macro.loc[index,'ERROR_DESCRIPTIONS'] = cadena_erccc
                    counter += 1
                
                for index, row in series.iterrows():
                    column_name = row['Variable']
                    pattern = row['regex_pattern']
                    regex_description_value = row['regex_description']
                    null_values = df_bd[column_name].isnull()
                    trim_or_whitespace = df_bd[column_name].apply(lambda x: str(x).replace(' ', '') == '')
                    mask = null_values | trim_or_whitespace
                    mask_sum = mask.sum()
                    # df_no_matches = df_bd.loc[(~df_bd[column_name].astype(str).str.contains(pattern)) & ~mask]
                    # df_matches = df_bd.loc[(df_bd[column_name].astype(str).str.contains(pattern)) & ~mask]
                    df_no_matches = df_bd.loc[(~df_bd[column_name].astype(str).str.contains(pattern))]
                    df_matches = df_bd.loc[(df_bd[column_name].astype(str).str.contains(pattern))]
                    df_matches_global = df_matches
                    # namemuestra_global = 'muestra_50_'+time_str+'_'+hash_generated+'.csv'

                    if(len(df_matches)>=50):
                        random_rows = df_matches.sample(n=50, random_state=None)
                        random_rows.astype(str)
                        sample_50_global = random_rows
                    if not df_no_matches.empty:
                        all_good_global = False
                        df_errors = df_bd.loc[(null_values | trim_or_whitespace)].reset_index(drop=True)
                        df_errors['ERROR_TYPE'] = 'NULL_VALUES_VENDOR_DATA'
                        df_errors['ERROR_DESCRIPTION'] = 'Valores nulos en columna '+column_name
                        df_no_matches_altered = df_no_matches.copy()
                        df_errors = pd.concat([df_errors, df_no_matches_altered], ignore_index=True)
                        #name_errors = '_filter_errors_'+time_str+'_'+hash_generated+'.csv'
                        #name_errors = f"_filter_errors_{time_str}_{hash_generated}.csv"
                        df_errors.astype(str)
                        #df_errors.to_csv(os.path.join(ERROR_VALIDACION_DIR, name_errors), index=False, header=True, encoding='latin1')
                        for index, row in df_no_matches.iterrows():
                            # print(f"{column}: {value}")
                            logger.info(f"REGEX VALIDATION: COLUMNA : {column_name} - No cumplió el patrón regex: { row['cod_barra'] }")

                        results.append(
                            f"{column_name}: { str(len(df_no_matches)) }"
                        )
                    else:
                        logger.info(f"REGEX VALIDATION: COLUMNA : {column_name} - SUCCEED")
                        results.append(
                            f"{column_name}: SUCCEED"
                        )
                # df_bd_copy_error_macro.to_csv('comida.csv', index=False, header=True,encoding='latin1')
                # chain_results
                name_errors = 'filter_errors_'+time_str+'_'+hash_generated+'.csv'
                # df_bd_copy_error_macro = df_bd_copy_error_macro.query('ERROR_DESCRIPTIONS != None')
                df_bd_copy_error_macro = df_bd_copy_error_macro[df_bd_copy_error_macro['ERROR_DESCRIPTIONS'].str.strip() != '']
                df_bd_copy_error_macro.astype(str)
                df_bd_copy_error_macro.to_csv(os.path.join(ERROR_VALIDACION_DIR, name_errors),index=False,header=True,encoding='latin1')
                chain_results = "\n".join(str(item) for item in results)
                if(len(df_no_matches)>0):
                    all_good_global=True

                return df_matches_g_new,chain_results,namemuestra_global,df_matches_global,all_good_global
                # return "null"
            time.sleep(1)
            update_task_file(task_file, progress=10)

            logger.info("Paso 2: Validación inicial de archivos")
            def validate_dataframe(df, num_col_base,file_path):
                """
                Validates a pandas DataFrame by checking the number of columns in each row.
                
                Args:
                    df (pd.DataFrame): The DataFrame to be validated.
                    num_col_base (int): The expected number of columns.

                Returns:
                    None
                """

                # Make a copy of the original DataFrame
                df_copy = df.copy()

                # Add a new column 'row_column_count' to count the number of non-null values in each row
                df_copy['row_column_count'] = df_copy.apply(lambda x: len(x), axis=1)

                # Identify rows with an incorrect number of columns
                invalid_rows = df_copy[df_copy['row_column_count'] != num_col_base]

                # Identify rows with all null values
                empty_or_invalid = df_copy[df_copy.isnull().all(axis=1)]

                # Log the results
                logger.info(f"INFO Reading file : { file_path }")
                logger.info(f"Número esperado de columnas: {num_col_base}")
                logger.info(f"Número de filas con columnas incorrectas: {len(invalid_rows)}")
                logger.info(f"Número de filas con todos valores nulos : {len(empty_or_invalid)}")
                logger.info(f"INFO : Cantidad de filas leídas : {len(df_copy)}")
                logger.info(f"\n")
                logger.info(f"----------------------------------------------------------------------------------------------------")
            update_task_file(task_file, progress=10)    


            def count_nulls(df, column_name):
                """
                Cuenta el número de valores nulos en una columna específica del Dataframe.

                Parámetros:
                df (pandas.DataFrame): DataFrame de entrada.
                column_name (str): Nombre de la columna a revisar para valores nulos.

                Returns:
                tupla: Cantidad de valores nulos y cadena formateada.
                """
                null_count = df[column_name].isnull().sum()
                result_str = f"{column_name}: {null_count}"
                return result_str

            def find_duplicates(df2, columns_to_check):
                """
                Find duplicate rows in df2 based on the specified columns.

                Args:
                    df2 (pd.DataFrame): DataFrame to search for duplicates.
                    columns_to_check (list of str): List of column names to check for duplicates.

                Returns:
                    A dictionary containing information about the duplicates found.
                """

                # Check if all columns exist in df2
                if not all(col in df2.columns for col in columns_to_check):
                    return {
                        'duplicated': False,
                        'duplicated_rows': None,
                        'total_duplicates': 0
                    }

                # Use groupby and size to find duplicate rows with the same combination of values in all columns
                duplicates = df2[df2.duplicated(subset=columns_to_check, keep=False)].groupby(columns_to_check).size().reset_index(name='count')
                
                # Filter out groups with only one duplicate (i.e., they were already present in the original data)
                duplicates = duplicates[duplicates['count'] > 1]

                repetidos = not duplicates.empty

                duplicated_rows = df2[df2.duplicated(subset=columns_to_check, keep=False)].drop_duplicates(subset=columns_to_check, keep='first')

                # Print the number of duplicated values
                duplicated_values_count = df2[df2.duplicated(subset=columns_to_check, keep=False)].shape[0]

                return {
                    'duplicated': repetidos,
                    'duplicated_rows': duplicated_rows if repetidos else None,
                    'total_duplicates': duplicated_values_count
                }

            # Ruta del archivo Excel   2
            dictionary = diccionario_file
            database = base_file
            inventary = INVENTARIO_FILE
            padron = PADRON_FILE
            file = open(inventary, 'r')

            # Read the first line of the file
            first_line = file.readline()
            first_line = first_line.split()

            # Leer el archivo Excel utilizando pandas
            df = pd.read_excel(dictionary, dtype=str)
            df2 = pd.read_csv(database, delimiter='\t', encoding='latin1', dtype=str)
            df3 = pd.read_csv(inventary, delimiter='\t', encoding='latin1', usecols=first_line, dtype=str)
            df4 = pd.read_excel(padron, dtype=str)

            # Imprimir los primeros 5 registros del DataFrame
            num_columnas = len(df.columns)
            num_rows_dictionary1 = len(df['Variable'])
            variable_dictionary = df['Variable'].tolist()
            num_col_base = len(df2.columns.values)
            # validate_dataframe(df2,num_col_base)
            validate_dataframe(df2, num_col_base,database)
            validate_dataframe(df3, len(df3.columns.values),inventary)
            validate_dataframe(df4, len(df4.columns.values),padron)
            validate_dataframe(df, len(df.columns.values),dictionary)
            # logger.info(f"INFO Reading file : { padron }")
            # logger.info(f"INFO : Cantidad de filas leídas : {len(df4)}")
            # df_copy4 = df4.copy()
            # empty_or_invalid_padron = df_copy4[df_copy4.isnull().all(axis=1)]
            # logger.info(f"Número de filas con todos valores nulos : {len(empty_or_invalid_padron)}")

            # df2 ya cargado
            # Aseguramos que columnas clave sean enteros (excepto cod_barra que se queda en str)
            df2["dhoja"] = df2["dhoja"].astype(int)
            df2["hoja"] = df2["hoja"].astype(int)
            df2["pos"] = df2["pos"].astype(int)

            # 1. Calcular dhoja máximo por cod_barra
            max_dhoja = df2.groupby("cod_barra", as_index=False)["dhoja"].max().rename(columns={"dhoja": "max_dhoja"})

            # 2. Generar todas las combinaciones esperadas
            expected_list = []
            for _, row in max_dhoja.iterrows():
                cod = row["cod_barra"]
                dhoja_max = row["max_dhoja"]

                # n1: valores desde 1 hasta (dhoja_max - 3)
                for n1 in range(1, dhoja_max - 2):  
                    hoja = n1 + 3  # empieza en 4
                    dhoja = dhoja_max

                    # n2: valores desde 1 hasta 4
                    for n2 in range(1, 5):
                        expected_list.append([cod, hoja, dhoja, n2])

            expected = pd.DataFrame(expected_list, columns=["cod_barra", "hoja", "dhoja", "pos"])

            # 3. Detectar faltantes (left join y filtrar los que no existen en df2)
            merged = expected.merge(df2, on=["cod_barra", "hoja", "dhoja", "pos"], how="left", indicator=True)
            faltantes = merged[merged["_merge"] == "left_only"]
            faltantes = faltantes[["cod_barra", "hoja", "dhoja", "pos"]]
            # 4. Ordenar como en SQL
            faltantes = faltantes.sort_values(by=["cod_barra", "hoja", "pos"]).reset_index(drop=True)

            print(faltantes)
            logger.info(f"Lista de coincidencias faltantes : \n { faltantes }")

            temp = df2.copy()
            first_row_values = temp.columns.values
            lista1 = variable_dictionary
            lista2 = first_row_values
            # print("DICTIONARY")
            # print(lista1)
            # print("BASE")
            # print(lista2)
            # print("COMPARASION")
            # print(num_rows_dictionary)
            # print("2nd_val")
            # print(num_col_base)

            solo_en_lista1 = [clave for clave in set(lista1) if clave not in lista2]
            # Obtiene los elementos que están presentes solo en la segunda lista
            solo_en_lista2 = [clave for clave in set(lista2) if clave not in lista1]

            result_base_needs=''
            result_base_needs2=''
            if(len(solo_en_lista1)!=0):        
                result_base_needs = "\n".join(solo_en_lista1)
                all_good_global=False
            if(len(solo_en_lista2)!=0):        
                result_base_needs2 = "\n".join(solo_en_lista2)

            logger.info(f"Lista de columnas faltantes : \n { result_base_needs2 }")

            comunes = [clave for clave in set(lista1) if clave in lista2]
            df3_filtered = df3.loc[(df3['ESTADO'] == '1') & (df3["COD_PREFIJO"] == codigo_ficha)]
            list_cod_instr = df3_filtered['COD_INSTRUMENTO'].tolist()
            list_cod_barra = df2['cod_barra'].tolist()

            cant_inv = df3_filtered.shape[0]
            cant_base = len(list_cod_barra)

            conjunto7 = set(list_cod_barra)
            conjunto8 = set(list_cod_instr)

            conjunto7.discard(None)
            conjunto8.discard(None)

            diferencia9 = conjunto7 - conjunto8

            cant_no_corr = len(solo_en_lista1)
            
            logger.info(f"----------------------------------------------------------------------------------------------------")           
            logger.info(f"Cantidad esperada para validación A1:  {num_rows_dictionary1}")
            logger.info(f"Cantidad encontrada para la validación A1: {num_col_base}")
            logger.info(f"Cantidad que corresponde para la validación A1: {num_rows_dictionary1 - cant_no_corr}")
            logger.info(f"Cantidad que no corresponde para la validación A1: {cant_no_corr}")
            logger.info(f"\n")
            logger.info(f"----------------------------------------------------------------------------------------------------")

            logger.info(f"----------------------------------------------------------------------------------------------------")           
            logger.info(f"Cantidad esperada para validación A2:  {cant_inv}")
            logger.info(f"Cantidad encontrada para la validación A2: {cant_base}")
            logger.info(f"Cantidad que corresponde para la validación A2: {cant_base - len(diferencia9)}")
            logger.info(f"Cantidad que no corresponde para la validación A2: {len(diferencia9)}")
            logger.info(f"\n")
            logger.info(f"----------------------------------------------------------------------------------------------------")

            columnas = ['Litho', 'cod_barra', 'cor_minedu', 'cod_mod7', 'anexo', 'ie', 'seccion',
                            'departamento', 'provincia', 'distrito']

            # filtered_columnas = set(columnas) & set(lista2)
            # columnas = list(filtered_columnas)
            columnas = [col for col in columnas if col in lista2]

            resultados = []
            max_nunmber_rows_empty = 0
            for columna in columnas:
                df02 = df2[[columna]]
                resultado = count_nulls(df02, columna)
                cant_filas_bigger = df02[columna].isnull().sum()
                if ( max_nunmber_rows_empty < cant_filas_bigger):
                    max_nunmber_rows_empty = cant_filas_bigger
                resultados.append(resultado)
            cadena_resultados = "\n".join(resultados)

            repetidos = df2[['Litho', 'cod_barra', 'cor_minedu', 'cod_mod7', 'anexo', 'ie', 'seccion',
                            'departamento', 'provincia', 'distrito']].duplicated().any()
            columnas_01 = ['Litho', 'cod_barra', 'cor_minedu', 'cod_mod7', 'anexo', 'ie', 'seccion', 'departamento', 'provincia', 'distrito']
            

            # null_or_empty_cols = [col for col in ['Litho', 'cod_barra', 'cor_minedu', 'cod_mod7', 'anexo', 'ie',
            #                                     'departamento', 'provincia', 'distrito']
            #                     if df2[col].dtype == object and (df2[col].isnull().any() or df2[col].str.contains('\s').any())]
            # print("REPETIDOS")
            # print(repetidos)

            # lista_repetidos = []
            # #Copiar un duplicado

            # if(repetidos == False):
            #     repetidos='0'
            # else:
            #     repetidos = str(len(repetidos))
            #     lista_repetidos = repetidos['cod_barra'].values

            df_base_temp = df2.copy()
            df_padron_temp = df4.copy()

            # df_base_temp['cor_minedu_cod_mod7_anexo']= df_base_temp['cor_minedu_cod_mod7_anexo']
            df_base_temp['cor_minedu_cod_mod7_anexo'] = df_base_temp.apply(lambda row: str(row.cor_minedu) + ' ' + str(row.cod_mod7) + ' ' + str(row.anexo), axis=1)

            df_padron_temp['cor_minedu_cod_mod7_anexo'] = df_padron_temp.apply(lambda row: str(row.cor_minedu) + ' ' + str(row.cod_mod7) + ' ' + str(row.anexo), axis=1)

            # lista_cor_minedu_base = df2['cor_minedu'].tolist()
            # lista_cor_minedu_padron = df4['cor_minedu'].tolist()
            lista_cor_minedu_base = df_base_temp['cor_minedu_cod_mod7_anexo'].tolist()
            lista_cor_minedu_padron = df_padron_temp['cor_minedu_cod_mod7_anexo'].tolist()

            # Eliminar los valores vacíos de las listas
            # conjunto1 = set(df2['cor_minedu'].tolist())
            # conjunto2 = set(df4['cor_minedu'].tolist())
            conjunto1 = set(df_base_temp['cor_minedu_cod_mod7_anexo'].tolist())
            conjunto2 = set(df_padron_temp['cor_minedu_cod_mod7_anexo'].tolist())
            cant_conjunto2 = len(conjunto2)

            conjunto1.discard(None)
            conjunto2.discard(None)

            diferencia = conjunto1 - conjunto2

            if(str(diferencia)=='nan'):
                diferencia='Sin error'
            else:
                all_good_global= False

            diferencia2 = conjunto2 - conjunto1

            if(str(diferencia2)=='nan'):
                diferencia2='Sin error'

            solo_lista_2_foo = ''
            if solo_en_lista2 == [] or solo_en_lista2 is None:
                solo_lista_2_foo=''
            else:
                solo_lista_2_foo = solo_en_lista2

            mensaje_cantidad_variables = ''
            if(solo_lista_2_foo==''):
                mensaje_cantidad_variables = "Todas las columnas requeridas están presentes y el conteo coincide"
            else:
                mensaje_cantidad_variables = "No todas las columnas requeridas están presentes y el conteo no coincide"
                all_good_global=False



            current_time = datetime.now()
            timestamp_str = f"{current_time.year}{current_time.month}{current_time.day}"

            hash_generated = generate_hash()

            archivo =  timestamp_str + "_report_log_" + hash_generated + ".txt"

            log_folder_path = OUTPUT_DIR
            df_comunes = df.loc[df['Variable'].isin(comunes)]
            # common_df_columnes = df_comunes.loc[df_comunes['VARIABLE'].isin(columnas)]
            # columnas
            # Validación regex 
            # temp,namemuestra_global,df_matches_global,all_good_global =  validate_regex_bd(df2,df_comunes,timestamp_str,hash_generated)
            df_matches_g_new,temp,namemuestra_global,df_matches_global,all_good_global =  validate_regex_bd(df2,df_comunes,timestamp_str,hash_generated)
            # temp =  validate_regex_bd(df2,df_comunes,timestamp_str,hash_generated)
            # return
            

            formatted_time = current_time.strftime('%Y-%m-%d %H:%M:%S')
            gee = find_duplicates(df2,columnas_01)
            filas_duplicadas = ''
            if (gee['duplicated_rows'] is None or len(gee['duplicated_rows'])==0):
                filas_duplicadas = ''
            else:
                filas_duplicadas= gee['duplicated_rows']
            inventario_new_elements = ''
            if(diferencia9 is None or len(diferencia9)==0 ):
                inventario_new_elements = ''
            else:
                inventario_new_elements = diferencia9

            registo_faltante_patron=''
            if(diferencia is None or len(diferencia) ==0):
                registo_faltante_patron=''
            else:
                registo_faltante_patron=diferencia
            #filas_duplicadas_2 = filas_duplicadas[['Litho', 'cod_barra', 'cor_minedu', 'cod_mod7', 'anexo', 'ie', 'seccion', 'departamento', 'provincia', 'distrito']]
            logger.info("Paso 3: Procesamiento de REGEX")
            # Texto a exportar mensaje_cantidad_variables
            texto_a_exportar = ["----------------------------------------------------------------------------------------------------",
                                "Reporte generado el: "+str(formatted_time),
                                "Validaciones Iniciales de Correspondencia A1",
                                "Resultado de la validación : "+ mensaje_cantidad_variables + ".",
                                result_base_needs2,
                                "----------------------------------------\n",
                                "----------------------------------------------------------------------------------------------------",
                                "Dataset con valores nulos en las columnas: ['Litho', 'cod_barra', 'cor_minedu', 'cod_mod7', 'anexo', 'ie', 'seccion', 'departamento', 'provincia', 'distrito']:",                    
                                cadena_resultados,
                                "----------------------------------------",
                                "----------------------------------------------------------------------------------------------------",
                                "Reporte generado el: "+str(formatted_time),
                                "Validaciones Iniciales de Correspondencia A2",
                                "Registros faltantes del inventario: "+str(len(diferencia9)),
                                "----------------------------------------",
                                str(inventario_new_elements),
                                "----------------------------------------",
                                "----------------------------------------------------------------------------------------------------",
                                "Reporte generado el: "+str(formatted_time),
                                "Validaciones Iniciales de Correspondencia A3",
                                "Registros duplicados : "+  str(gee['total_duplicates']) ,
                                "----------------------------------------",
                                str( filas_duplicadas  ),
                                "----------------------------------------",
                                "----------------------------------------------------------------------------------------------------",
                                "Reporte generado el: "+str(formatted_time),
                                "Validaciones Iniciales de Correspondencia A4",
                                "Cantidad Registros faltantes del padrón: "+str(len(diferencia)),
                                "----------------------------------------",
                                str(registo_faltante_patron),
                                "----------------------------------------",
                                "----------------------------------------------------------------------------------------------------",
                                "Reporte generado el: "+str(formatted_time),
                                "Reporte Acumulado de validaciones de Reglas Por Columna",
                                temp]

            subfolder_path = os.path.join(log_folder_path, REPORT_DIR)
            archivo_agil = os.path.join(subfolder_path, archivo)
            update_task_file(task_file, progress=60)
            


            with open(archivo_agil, 'a') as f:
                for linea in texto_a_exportar:
                    f.write(linea + '\n')

            
            cant_empty_null_total_from_columns = count_empty_rows(df2,columnas)
            # null_counts_column = count_nulls_per_column(df2, columnas)
            # total_nulos_columnas = sum(null_counts_column)
            # total_nulos_filas = sum(cant_empty_null_total_from_columns)
            # total_nulos = total_nulos_columnas + total_nulos_filas
            
            logger.info(f"Reporte guardado o actualizado en : { archivo_agil }")
            logger.info(f"Inconsistencia de archivos base.")

            logger.info(f"----------------------------------------------------------------------------------------------------")
            logger.info(f"# Identificar filas con valores null en columnas especificadas")
            logger.info(f"Cantidad de valores faltantes por columna:")
            logger.info(cadena_resultados)
            logger.info(f"Total de filas con valores faltantes en columnas ['Litho', 'cod_barra', 'cor_minedu', 'cod_mod7', 'anexo', 'ie', 'seccion', 'departamento', 'provincia', 'distrito']")
            logger.info(cant_empty_null_total_from_columns)


            dfdex=df2.sample(n=50)
            dfdex.to_csv(os.path.join(MUESTRA_DIR, namemuestra_global), index=False, header=True, encoding='latin1')

            ##2 TRAMO PARA INSERTAR A LA BASE DE DATOS EN sqlserver....
            #    
            # while True:
            #     respuesta = input("¿Desea insertar las validaciones iniciales en la base de datos? (si/no): ").strip().lower()
            
            #     if respuesta == "si":


            
            print("Continuando con el proceso de inserción en la base de datos...")



            def mover_archivo_mas_reciente_y_reemplazar(carpeta_origen, carpeta_destino):
                    
                        try:
                            os.makedirs(carpeta_origen, exist_ok=True)
                            os.makedirs(carpeta_destino, exist_ok=True)
                            
                            # Obtener la lista de archivos en la carpeta de origen
                            archivos = [f for f in os.listdir(carpeta_origen) if os.path.isfile(os.path.join(carpeta_origen, f))]

                            if not archivos:
                                print("No hay archivos en la carpeta de origen.")
                                return

                            # Obtener el archivo más reciente por fecha de creación
                            archivo_mas_reciente = max(archivos, key=lambda f: os.path.getctime(os.path.join(carpeta_origen, f)))

                            # Construir la ruta del archivo más reciente en la carpeta de origen
                            ruta_origen = os.path.join(carpeta_origen, archivo_mas_reciente)

                            print(f"Archivo más reciente encontrado: {archivo_mas_reciente}")
                            # Eliminar todos los archivos existentes en la carpeta de destino
                            archivos_destino = [f for f in os.listdir(carpeta_destino) if os.path.isfile(os.path.join(carpeta_destino, f))]
                            for archivo in archivos_destino:
                                print(f"Eliminando {len(archivos_destino)} archivos en la carpeta de destino.")
                                os.remove(os.path.join(carpeta_destino, archivo))
                            
                            print("Esperando 5 segundos antes de mover el archivo...")
                            time.sleep(5)

                            # Construir la ruta de destino y mover el archivo más reciente
                            ruta_destino = os.path.join(carpeta_destino, archivo_mas_reciente)
                            
                            shutil.copy2(ruta_origen, ruta_destino)
                            print(f"Archivo '{archivo_mas_reciente}' copiado de '{carpeta_origen}' a '{carpeta_destino}'.")
                        
                        except Exception as e:
                            print(f"Error al mover el archivo: {e}")

            carpeta_origen = os.path.join(os.getcwd(), REPORT_DIR)
            carpeta_destino = os.path.join(os.getcwd(), DB_INGESTA)

            mover_archivo_mas_reciente_y_reemplazar(carpeta_origen, carpeta_destino)

                    #carpeta = os.path.join(os.getcwd(), 'report/db_ingesta')
                    
            def extraer_parte_nombre(ruta_archivo, delimitador='_', posicion=3):

                            nombre_archivo = os.path.basename(ruta_archivo)
                            # Remueve la extensión
                            nombre_sin_extension = os.path.splitext(nombre_archivo)[0]
                            # Divide el nombre por el delimitador
                            partes = nombre_sin_extension.split(delimitador)
                            
                            if len(partes) > posicion:
                                return partes[posicion]
                            else:
                                raise ValueError(f"El archivo '{nombre_archivo}' no cumple con el delimitador '{delimitador}' para extraer la posición {posicion}.")
                    
            carpeta = DB_INGESTA
                
            logger.info("----------------------------------------------------------------------------------------------------")
            logger.info(f"Buscando reporte para inserción de la base de datos")
            logger.info(f"Recolectando resultados de validaciones iniciales de la ruta {carpeta}")
            logger.info("----------------------------------------------------------------------------------------------------")
            logger.info(f"\n")
            logger.info(f"Recolectando datos a la base de datos")

            try:
                        for archivo in os.listdir(carpeta):
                            if archivo.endswith('.txt'):  # Solo procesar archivos .txt
                                ruta_completa = os.path.join(carpeta, archivo)
                                parte_extraida = extraer_parte_nombre(ruta_completa)
                                print(f"cod_ficha extraída: {parte_extraida}")
            except Exception as e:
                            print(f"Error: {e}")

            # def extraer_datos_nombre_archivo(ruta_archivo_2):
            #             try:
            #                 nombre_archivo_2 = os.path.basename(ruta_archivo_2)
            #                 nombre_sin_ext, _ = os.path.splitext(nombre_archivo_2)
            #                 return {"nombre_archivo_2":nombre_sin_ext}

            #                 # if len(partes2) < 6:
            #                 #     raise ValueError(f"El nombre del archivo '{nombre_archivo_2}' no tiene suficientes partes para extraer los datos.")

            #                 # datos_extraidos = {
            #                 #     "tipo_bd": partes2[0],
            #                 #     "codigo": partes2[1],
            #                 #     "descripcion": partes2[2],
            #                 #     "grado": partes2[3],
            #                 #     "nivel": partes2[4],
            #                 #     "tipo_prueba": partes2[5]
            #                 # }

            #                 # return datos_extraidos
                        
            #             except Exception as e:
            #                 logger.error(f"Error al procesar el archivo {ruta_archivo_2}: {e}")
            #                 return None  # Asegurarse de que la función siempre retorne un valor

                    # Procesar archivos en la carpeta
            #carpeta2 = SHARED_DIR

            # logger.info("----------------------------------------------------------------------------------------------------")
            # logger.info(f"Recolectando resultados de validaciones iniciales de la ruta {carpeta2}")

            # # Buscar archivos .txt en la carpeta
            # archivos_txt = [f for f in os.listdir(carpeta2) if f.endswith('.txt')]

            datos = None

            # if archivos_txt:
            #     # Obtener el archivo más reciente
            #     archivo_reciente = max(
            #         archivos_txt,
            #         key=lambda f: os.path.getmtime(os.path.join(carpeta2, f))
            #     )
            #     ruta_completa2 = os.path.join(carpeta2, archivo_reciente)
            #     datos_extraidos = extraer_datos_nombre_archivo(ruta_completa2) 

            # for archivo2 in os.listdir(carpeta2):
            #         if archivo2.endswith('.txt'):
            #             ruta_completa2 = os.path.join(carpeta2, archivo2)
            #             datos_extraidos = extraer_datos_nombre_archivo(ruta_completa2)
            #             if datos_extraidos:
            #                 nombre_final = datos_extraidos["nombre_archivo_2"]


            #                 logger.info(f" Archivo procesado: {nombre_final}")

            #                 try:
            #                     os.remove(ruta_completa2)
            #                     logger.info(f" Archivo eliminado: {nombre_final}")
            #                 except Exception as e:
            #                     logger.error(f" No se pudo eliminar {nombre_final}: {e}")
                    
            tabla_datos = [] 


            def obtener_csv_mas_reciente(carpeta_csv):
                        archivos_csv = [os.path.join(carpeta_csv, f) for f in os.listdir(carpeta_csv) if f.endswith('.csv')]
                        archivo_mas_reciente_csv = max(archivos_csv, key=os.path.getctime)
                        return archivo_mas_reciente_csv

            def escribir_datos_en_csv(archivo_salida, datos, header=False):

                        archivo_nuevo = not os.path.isfile(archivo_salida)
                        
                        with open(archivo_salida, mode='a', newline='', encoding='latin1') as file:
                            writer = csv.writer(file)
                            if archivo_nuevo or header:
                                writer.writerow(['fk_ficha', 'cod_barra', 'error_descripcion'])
                            # Escribe los datos
                            writer.writerow(datos)

                    

                    #exportar csv con duplicados y sus campos

            if str(gee['total_duplicates']) == '0':
                        print("No se encontró duplicados en la validación... continuando con la inserción")
                    
            else:
                        # Verificar el tipo de filas_duplicadas
                        if not isinstance(filas_duplicadas, pd.DataFrame):
                            raise TypeError(f"filas_duplicadas debe ser un DataFrame, pero es {type(filas_duplicadas)}")

                        # Crear un DataFrame con las filas duplicadas
                        df_seleccionado = filas_duplicadas.copy()

                        # Verificar que las columnas existen
                        columnas_requeridas = ['Litho', 'cod_barra', 'cor_minedu', 'cod_mod7', 'anexo', 'ie', 'seccion', 'departamento', 'provincia', 'distrito']
                        if not all(col in df_seleccionado.columns for col in columnas_requeridas):
                            raise ValueError(f"Faltan columnas en filas_duplicadas: {set(columnas_requeridas) - set(df_seleccionado.columns)}")

                        # Crear la nueva columna concatenada
                        df_seleccionado['Litho_cod_barra_cor_minedu_cod_mod7_anexo_ie_seccion_departamento_provincia_distrito'] = df_seleccionado.apply(
                            lambda row: ' '.join([str(row[col]) for col in columnas_requeridas]), axis=1
                        )
                        # Asignar las nuevas columnas adicionales
                        df_seleccionado['fk_ficha'] = parte_extraida
                        df_seleccionado['fk_campo'] = '2'
                        df_seleccionado['cantidad'] = str(gee['total_duplicates'])

                        columnas_a_exportar = ['Litho_cod_barra_cor_minedu_cod_mod7_anexo_ie_seccion_departamento_provincia_distrito']

                        # Definir el orden final de las columnas
                        df_seleccionado_final = ['fk_ficha', 'fk_campo'] + columnas_a_exportar + ['cantidad']

                        # Seleccionar solo las columnas necesarias
                        df_seleccionado = df_seleccionado[df_seleccionado_final]

                        # Exportar el DataFrame a un archivo CSV
                        archivo_salida2 = DUPLICATES_VALIDACION_FILE
                        df_seleccionado.to_csv(archivo_salida2, index=False, header=False, sep=',', encoding='latin1')
                        # #df_seleccionado.to_csv(archivo_salida2, index=False, sep=',', encoding='latin1')
                        # str_filas_duplicadas_sin_encabezado = "\n".join(str_filas_duplicadas.split('\n')[1:])
                        # # Escribir la cadena en un archivo CSV
                        # with open(archivo_salida2, mode='w', newline='', encoding='latin1', index=False, sep=',') as file:
                        #     writer = csv.writer(file)
                            
                        #     # Escribir la cadena completa como una única columna
                        #     for fila in str_filas_duplicadas_sin_encabezado.split('\n'):  
                        #         writer.writerow([fila])  
                        print(f"Los datos se han exportado correctamente a {archivo_salida2}")
                    
                    
        

                    # Ruta de la carpeta y archivo de salida
            carpeta_csv = ERROR_VALIDACION_DIR
            archivo_reciente_csv = obtener_csv_mas_reciente(carpeta_csv)


            archivo_salida = ERROR_VALIDACION_SALIDA_FILE
            fk_ficha = parte_extraida
                    # Procesar y escribir cada fila en el archivo de salida
            with open(archivo_reciente_csv, mode='r', encoding='latin1') as file:
                        csv_reader = csv.DictReader(file)
                        
                        # Verifica si ya se escribió el encabezado
                        encabezado_escrito = False
                        
                        for row in csv_reader:
                            try:
                                cod_barra = row.get('cod_barra','').strip()
                                errores = row.get('ERROR_DESCRIPTIONS','').strip()

                                if not cod_barra:
                                    cod_barra = "SIN_COD_BARRA"
                                
                                # Divide en caso de múltiples códigos o errores
                                if errores:
                                    cod_barras = cod_barra.split(', ')
                                    errores_list = errores.split(', ')
                                
                                # Itera sobre combinaciones de códigos de barra y errores
                                    for cod, err in zip(cod_barras, errores_list):
                                        escribir_datos_en_csv(
                                            archivo_salida, 
                                            [fk_ficha, cod.strip(), err.strip()], 
                                            header=not encabezado_escrito
                                        )
                                        encabezado_escrito = True
                            
                            except KeyError as e:
                                print(f"Columna no encontrada: {e}")
                            except Exception as e:
                                print(f"Error inesperado: {e}")

                    
            logger.info(f"\n")
            logger.info("----------------------------------------------------------------------------------------------------")
            logger.info(f"Insertando datos a la base de datos")
                    
                    # conectando a la base de datos
            server = 'sql5106.site4now.net'
            database = 'db_aac49f_depuracioningesta2'
            username = 'db_aac49f_depuracioningesta2_admin' 
            password = 'Admin2025#'
            logger.info("Paso 4: Inserción a la base de datos")
            connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
            connection = None

                    
            try:
                        logger.info("Insertando en base de datos")
                        connec = pyodbc.connect(connection_string)
                        print("Insertando archivos a la base de datos")

                        # cursor para ejecutar consultas
                        cursor = connec.cursor()

                        validacion_a1_1= int(cant_empty_null_total_from_columns)
                        validacion_a1_2= len(solo_en_lista2)
                        #cadena = cant_empty_null_total_from_columns + str(len(result_base_needs2))
                        suma= validacion_a1_1 + validacion_a1_2
                        #match = re.search(r'\d+', suma)
                        validacion_a1= suma
                        validacion_a2= int(len(inventario_new_elements))
                        validacion_a3= int(gee['total_duplicates'])
                        validacion_a4= int(len(registo_faltante_patron))
                        validacion_a5= int(len(faltantes))
                        validacion_a6= int(len(faltantes))

                        # Agregar los datos a la lista tabla_datos
                        tabla_datos.append({
                            "cod_ficha": parte_extraida,
                            "desc_ficha": job_id, 
                            "desc_grado": None,
                            "desc_nivel": None,
                            "tipo_prueba": None,
                            "validacion_a1": int(validacion_a1),
                            "validacion_a2": validacion_a2,
                            "validacion_a3": validacion_a3,
                            "validacion_a4": validacion_a4,
                            "validacion_a5": validacion_a5,
                            "validacion_a6": validacion_a6,
                            "fecha_proceso": formatted_time
                        })

                        # Insertar los datos en SQL Server
                        for fila in tabla_datos:
                            cursor.execute('''
                            INSERT INTO tb_ficha_instrumento (
                                cod_ficha, desc_ficha, desc_grado, desc_nivel, tipo_prueba, 
                                validacion_a1, validacion_a2, validacion_a3, validacion_a4, 
                                validacion_a5, validacion_a6, fecha_proceso
                            ) 
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            ''', (
                            fila["cod_ficha"], 
                            fila["desc_ficha"], 
                            fila["desc_grado"], 
                            fila["desc_nivel"], 
                            fila["tipo_prueba"],
                            fila["validacion_a1"], 
                            fila["validacion_a2"], 
                            fila["validacion_a3"], 
                            fila["validacion_a4"],
                            fila["validacion_a5"], 
                            fila["validacion_a6"], 
                            fila["fecha_proceso"]
                        ))
                        print(f'Se insertó el reporte {parte_extraida} en la tb_ficha_instrumento correctamente.')


                        # Insertando datos a la base de datos tb_validacion_a1
                        cadena_resultados2 = cadena_resultados
                        # Litho: 0
                        # cod_barra: 0
                        # cod_mod7: 0
                        # anexo: 0
                        # ie: 0
                        # departamento: 0
                        # provincia: 0
                        # distrito: 0
                        # """

                        fk_ficha = parte_extraida
                        tipo_validacion = mensaje_cantidad_variables
                        campo = result_base_needs2.strip() if result_base_needs2.strip() !="" else 0

                        # Extraer los valores de la cadena
                        datos_extraidos2 = dict(re.findall(r'(\w+):\s*(.+)', cadena_resultados2))

                        # Transformar los valores a cadenas y asegurarse de que estén en el formato adecuado
                        valores_para_insertar = (
                            fk_ficha,
                            tipo_validacion,
                            campo,  # Este campo puede ser None
                            str(datos_extraidos2.get("Litho", "")).strip(),
                            str(datos_extraidos2.get("cod_barra", "")).strip(),
                            str(datos_extraidos2.get("cod_mod7", "")).strip(),
                            str(datos_extraidos2.get("cor_minedu","")).strip(),
                            str(datos_extraidos2.get("anexo", "")).strip(),  # Convertir a string (aunque esté vacío)
                            str(datos_extraidos2.get("ie", "")).strip(),
                            str(datos_extraidos2.get("departamento", "")).strip(),
                            str(datos_extraidos2.get("provincia", "")).strip(),
                            str(datos_extraidos2.get("distrito", "")).strip(),
                            str(datos_extraidos2.get("seccion", "")).strip()
                        )

                        # Insertar los valores en la base de datos
                        try:
                            with connec.cursor() as cursor:
                                query = '''
                                INSERT INTO tb_validacion_A1 (
                                    fk_ficha, tipo_validacion, campo, Litho, cod_barra, cor_minedu, cod_mod7, anexo, ie, 
                                    departamento, provincia, distrito, seccion
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                '''
                                cursor.execute(query, valores_para_insertar)

                            connec.commit()
                            #print(f'{valores_para_insertar}')
                            print(f"Se insertó el reporte {parte_extraida} en la tb_validacion_A1 correctamente.")

                        except pyodbc.Error as e:
                            print(f"Error al insertar datos en la base de datos: {e}")
                            connec.rollback()
                        except Exception as e:
                            print(f"Error inesperado: {e}")
                            connec.rollback()

                
                        
                        time.sleep(2)
                        archivo_csv = ERROR_VALIDACION_SALIDA_FILE
                        # Verifica si el archivo CSV 
                        if os.path.exists(archivo_csv):
                            try:
                                df = pd.read_csv(archivo_csv, encoding='latin1', header=0, dtype={'cod_barra': str})
                                cursor = connec.cursor()
                                tabla_destino = 'tb_validacion_regex'

                                columnas = ', '.join(df.columns)
                                placeholders = ', '.join(['?' for _ in df.columns])

                                query = f"INSERT INTO {tabla_destino} ({columnas}) VALUES ({placeholders})"

                                datos = [tuple(row) for row in df.to_numpy()]
                                try:
                                    cursor.fast_executemany = True
                                except:
                                    pass  # Si no está disponible, ignora

                                # ✅ Insertar por lotes
                                batch_size = 3000
                                for i in range(0, len(datos), batch_size):
                                    batch = datos[i:i + batch_size]
                                    cursor.executemany(query, batch)
                                connec.commit()
                                print(f"Se insertó las validaciones regex que no cumplen con el regex_patterns en la {tabla_destino}.")
                                time.sleep(2)
                                os.remove(archivo_csv)
                                print("Archivo CSV eliminado correctamente.")
                                # except Exception as e:
                                #     print(f"⚠️ No se pudo eliminar el archivo: {e}")

                            except Exception as e:
                                # Si ocurre un error, hacer rollback
                                connec.rollback()
                                print(f"Error al insertar datos: {e}")

                        else:
                            print("Como no se encontró errores en la validacion regex, no se insertó datos en la tb_validacion_regex.")
                            print("Archivo eliminado correctamente.")  
                        


                        #insertando datos a la base de datos tb_validacion_a2
                        if len(diferencia9) > 0:
                            fk_ficha=parte_extraida
                            fk_campo2 = str('4')
                            inv_faltante = str(inventario_new_elements)
                            Cantidad_a2 = str(len(diferencia9))

                            # Insertar los datos en SQL Server
                            for fila in tabla_datos:
                                cursor.execute('''
                                INSERT INTO tb_validacion_A2 (
                                    fk_ficha, fk_campo, inv_faltante, cantidad
                                ) 
                                VALUES (?, ?, ?, ?)
                                ''', (
                                fk_ficha, fk_campo2, inv_faltante, Cantidad_a2
                            ))
                            print(f"Se insertó {Cantidad_a2} datos, en la columna inventario faltante según el report {fk_ficha} en una fila de la tb_validacion_A2.")
                        else:
                            print(f'Como no se encontró errores en la validación, no se insertó datos en la tb_validacion_A2.')



                        #insertando datos a la base de datos tb_validacion_a3

                        archivo_csv_dp = DUPLICATES_VALIDACION_FILE
                        column_names = ['fk_ficha','fk_campo','desc_campo','cantidad']

                        if os.path.exists(archivo_csv_dp):
                            try:
                                # Leer el archivo CSV completo sin cabecera
                                df1 = pd.read_csv(archivo_csv_dp, encoding='latin1', sep=',', header=None)
                                df1.columns = column_names  # asignar columnas manualmente

                                print(f"✅ Se leyeron {len(df1)} registros del archivo de duplicados")

                                with connec.cursor() as cursor:
                                    # Habilitar inserción masiva
                                    try:
                                        cursor.fast_executemany = True
                                    except:
                                        pass  # si no está disponible, continuar normal

                                    tabla_destino = 'tb_validacion_A3'
                                    columnas = ', '.join([f"[{col}]" for col in df1.columns])
                                    placeholders = ', '.join(['?' for _ in df1.columns])
                                    query = f"INSERT INTO {tabla_destino} ({columnas}) VALUES ({placeholders})"

                                    # Convertir DataFrame a tuplas
                                    datos = [tuple(row) for row in df1.to_numpy()]

                                    # Inserción por lotes (seguro en archivos grandes)
                                    batch_size = 5000
                                    for i in range(0, len(datos), batch_size):
                                        batch = datos[i:i + batch_size]
                                        cursor.executemany(query, batch)

                                    connec.commit()
                                    print(f"✅ Se insertaron {len(datos)} registros en {tabla_destino}")

                                # Eliminar archivo tras inserción correcta
                                os.remove(archivo_csv_dp)
                                print("✅ Archivo CSV eliminado")

                            except pd.errors.ParserError as e:
                                print(f"❌ Error al leer el CSV: {e}")
                            except pyodbc.Error as e:
                                print("❌ Error de base de datos:", e.args)
                                connec.rollback()
                            except Exception as e:
                                print(f"❌ Error inesperado: {e}")
                                connec.rollback()
                        else:
                            print("ℹ️ No se encontraron duplicados, no se insertó datos en tb_validacion_A3")

                        
                        # Inserción a la base de datos en la tb_validacion_a4

                        if len(diferencia) > 0:
                            fk_ficha=parte_extraida
                            fk_campo = str('3')
                            padron_faltante = str(registo_faltante_patron)
                            Cantidad_a4 = str(len(diferencia))


                            # Insertar los datos en SQL Server
                            for fila in tabla_datos:
                                cursor.execute('''
                                INSERT INTO tb_validacion_A4 (
                                    fk_ficha, fk_campo, padron_faltante, cantidad
                                ) 
                                VALUES (?, ?, ?, ?)
                                ''', (
                                fk_ficha, fk_campo, padron_faltante, Cantidad_a4
                            ))
                            print(f"Se insertó {Cantidad_a4} datos, en la columna padron faltante según el report {fk_ficha} en una fila de la tb_validacion_A4.")
                        else:
                            print(f'Como no se encontró errores en la validación, no se insertó datos en la tb_validacion_A4.')

                        connec.commit()

                        # Inserción a la base de datos en la tb_validacion_a5_a6
                        fk_ficha = parte_extraida
                        fk_campo = str('5')

                        # Asegurar tipos string
                        faltantes["cod_barra"] = faltantes["cod_barra"].astype(str)
                        faltantes["hoja"] = faltantes["hoja"].astype(str)
                        faltantes["dhoja"] = faltantes["dhoja"].astype(str)
                        faltantes["pos"] = faltantes["pos"].astype(str)

                        # Preparar los datos como lista de tuplas
                        data = [
                            (
                                fk_ficha,
                                fk_campo,
                                fila["cod_barra"],
                                fila["hoja"],
                                fila["dhoja"],
                                fila["pos"]
                            )
                            for _, fila in faltantes.iterrows()
                        ]

                        # Activar modo rápido
                        cursor.fast_executemany = True  

                        # Ejecutar inserción masiva
                        cursor.executemany('''
                            INSERT INTO tb_validacion_A5_A6 (
                                fk_ficha, fk_campo, cod_barra, hoja, dhoja, pos
                            ) 
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', data)

                        connec.commit()
                        print(f"Se insertaron {len(faltantes)} filas en tb_validacion_A5_A6 para el reporte {fk_ficha}.")

                        connec.commit()

                        #inserción a la tabla de cantidades de las validaciones iniciales

                        # validacion_a1
                        cursor.execute("""
                            INSERT INTO tb_validacion_cant (fk_ficha, tipo_validacion, cant_esp, cant_enc, cant_corres, cant_nocorr)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (fk_ficha, "validacion_a1", num_rows_dictionary1, num_col_base, num_rows_dictionary1 - cant_no_corr, cant_no_corr))

                        # validacion_a2
                        cursor.execute("""
                            INSERT INTO tb_validacion_cant (fk_ficha, tipo_validacion, cant_esp, cant_enc, cant_corres, cant_nocorr)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (fk_ficha, "validacion_a2", cant_inv, cant_base, cant_base - len(diferencia9), len(diferencia9)))

                        # validacion_a3
                        cursor.execute("""
                            INSERT INTO tb_validacion_cant (fk_ficha, tipo_validacion, cant_esp, cant_enc, cant_corres, cant_nocorr)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (fk_ficha, "validacion_a3", None, cant_base, cant_base - (gee['total_duplicates']), (gee['total_duplicates'])))

                        # validacion_a4
                        cursor.execute("""
                            INSERT INTO tb_validacion_cant (fk_ficha, tipo_validacion, cant_esp, cant_enc, cant_corres, cant_nocorr)
                            VALUES (?, ?, ?, ?, ?, ?)
                        """, (fk_ficha, "validacion_a4", None, cant_conjunto2, cant_base - len(diferencia), len(diferencia)))

                        # Confirmar cambios
                        connec.commit()

                        cod_ficha = parte_extraida

                        #rutas
                        ruta_muestra1 = r'output/muestra'
                        ruta_muestra2 = r'D:/shared/output/muestra'

                        cursor.execute("""
                            SELECT desc_ficha 
                            FROM tb_ficha_instrumento 
                            WHERE cod_ficha = ?
                        """, (cod_ficha,))
                        fila = cursor.fetchone()

                        if fila:
                                desc_ficha = fila[0].strip()
                                print(f'Encontrado job_id: {desc_ficha}')

                                carpeta_destino_muestra = os.path.join(ruta_muestra2, desc_ficha)
                                os.makedirs(carpeta_destino_muestra, exist_ok=True)

                                archivo_muestra = None
                                for archivo_muestra in os.listdir(ruta_muestra1):
                                    if cod_ficha in archivo_muestra and archivo_muestra.endswith(".csv"):
                                        archivo_muestra = os.path.join(ruta_muestra1, archivo_muestra)
                                        break
                                if archivo_muestra:
                                    destino_ruta = os.path.join(carpeta_destino_muestra, os.path.basename(archivo_muestra))
                                    shutil.copy(archivo_muestra, destino_ruta)
                                    print(f'Archivo copiado en: {destino_ruta}')

                                else: 
                                    print(f"No se encontró archivo CSV con el código {cod_ficha} en {ruta_muestra1}")
                        else:
                                print(f"No existe registro con cod_ficha = {cod_ficha}")

                        # Cerrar cursor y conexión
                        cursor.close()
                        connec.close()
                        update_task_file(task_file, progress=90)

            except pyodbc.Error as e:
                        print("Error al conectar con la base de datos:", e)
            logger.info("Datos insertados correctamente.")

            end_time = time.time()
            # Calcula la diferencia entre las dos fechas en segundos
            time_taken = end_time - start_time
            time.sleep(1)
            update_task_file(task_file, progress=90, status=3, estado_validacion=1)
            logger.info(f"Tiempo tomado: {time_taken:.2f} segundos")
            logger.info("Program ended")

        except Exception as e:
            # Error: actualizar JSON y relanzar excepción
            update_task_file(task_file, progress=100, status=3, estado_validacion=2, mensaje=str(e))
            logger.error(f"❌ Error en: {e}")
            raise

if __name__ == "__main__":
    # Rutas fijas para pruebas manuales
    base = "input/database/base.txt"
    diccionario = "input/diccionario/diccionario.xlsx"
    inventario = "input/inv/inventario.txt"
    padron = "input/padron/padron.xlsx"

    try:
        run_app(base, diccionario, tipo="Prueba Local", seccion="no")
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")

