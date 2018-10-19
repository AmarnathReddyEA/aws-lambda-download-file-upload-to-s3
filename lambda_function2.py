# My Lambda Function
import boto3
import re
import urllib.request
import zipfile
import tempfile
import os
import json
def lambda_handler():
    # Definition of function
    s3 = boto3.client('s3',
                      aws_access_key_id='AKIA',
                      aws_secret_access_key='SECRET')
    objs = s3.list_objects_v2(Bucket='bucketname')['Contents']
    listaXML = []

    for obj in objs:
        keyItem = obj['Key']
        if keyItem.endswith('xml'):
            listaXML.append(obj)
    listaXMLsomenteKeys = []
    for cadaXML in listaXML:
        listaXMLsomenteKeys.append(cadaXML['Key'])

    ultimoBaixado = listaXMLsomenteKeys[-1]
    ultimoBaixadoNumero = re.findall(r'\d+', ultimoBaixado)[0]
    print("# - Ultimo ID: " + ultimoBaixadoNumero)
    # Considerar o proximo
    ultimoBaixadoNumero = int(ultimoBaixadoNumero) + 1
    ultimoBaixadoProximoStr = str(ultimoBaixadoNumero)

    # 1 - Identificar qual foi o ultimo baixado
    # = ultimoBaixado gg:20
    # 2 - Pegar a lista de URLs no site oficial.
    # 3 - Identificar qual é o proximo a ser baixado
    # 4 - Executar o procedimento de download

    # Padrao de URL do XML
    urlParaBaixar = r'http://link'+ultimoBaixadoProximoStr+'.zip'
    print("# - Vai Baixar: " + urlParaBaixar)
    # Download and Create file in temp path
    temppath = tempfile.gettempdir()
    urllib.request.urlretrieve(urlParaBaixar, temppath+'/RM'+ultimoBaixadoProximoStr+'.zip')

    # Condider ZIP File
    print("# - Descompactar")
    zipdata = zipfile.ZipFile(temppath+'/RM'+ultimoBaixadoProximoStr+'.zip')
    zipdata.extractall(temppath+'/')
    zipdata.close()
    print("# - Descompactou")

    print("# - Processos AWS S3")
    print("# - Enviar ZIP para S3")
    s3.put_object(Bucket='bucketname', Key=ultimoBaixadoProximoStr+'.zip',
                  StorageClass='REDUCED_REDUNDANCY',
                  Body=open(temppath+'/'+ultimoBaixadoProximoStr+'.zip', 'rb'))
    print("# - Enviou ZIP para S3")

    print("# - Enviar TXT Descompactado para S3")
    s3.put_object(Bucket='bucketname', Key='' + ultimoBaixadoProximoStr + '.xml',
                  Body=open(temppath + '/' + ultimoBaixadoProximoStr + '.xml', 'rb'))
    print("# - Enviou TXT Descompactado para S3")

    return