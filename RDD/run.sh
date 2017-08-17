#!/bin/bash
path_delete=`pwd`/.u/

if [ $# -eq 6 ]; then
 es_search_host=$1
 search_index=$2
 es_load_host=$3
 load_index=$4
 nombre=$5
 criterio=$6
 path_script=`pwd`/.u/summarize
else
 rm -Rf $path_delete

 #Para los 6 parametros
 errorMessage="No se han introducido todos los argumentos requeridos o se han introducido de manera incorrecta"
 errArg1=" Arg 1: Cadena de conexion donde leer el documento. IP. Ej: 172.22.248.206:9229"
 errArg2=" Arg 2: Cadena de conexion donde guardar el resúmen. IP. Ej: 172.22.248.206:9229"
 errArg3=" Arg 3: Indice de donde se lee el documento"
 errArg4=" Arg 4: Indice de donde se guarda el resúmen del documento"
 errArg5=" Arg 5: Nombre del documento o identificador. Ej: 03_002-008_14.PDF"
 errArg6=" Arg 6: Nombre del criterio de busqueda, Ej: name, uuid o uri..."
#Descomentar para tratar el error de los 6 parametros
 echo "${errorMessage} " > ./error.log
 echo "${errArg1} " >> ./error.log
 echo "${errArg2} " >> ./error.log
 echo "${errArg3} " >> ./error.log
 echo "${errArg4} " >> ./error.log
 echo "${errArg5} " >> ./error.log
 echo "${errArg6} " >> ./error.log
 exit 1
fi 

spark-submit \
--master yarn-cluster \
--deploy-mode cluster \
--py-files $path_script/summarize.py $path_script/summarize.py \
#Comentar cuando vayamos a tratar 6 parametros
#$nombre $criterio
#Descomentar para tratar los 6 parametros
$es_search_host $search_index $es_load_host $load_index $nombre $criterio

rm -Rf $path_delete
exit 0
