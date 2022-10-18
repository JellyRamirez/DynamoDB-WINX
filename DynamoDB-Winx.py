'''
Equipo WINX
Andrea Jelena Ramírez García
Daniela Hernández Sánchez
Ernesto José López Urias
Luis Mario Ramírez Cardoso
'''
"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with Amazon DynamoDB to
create and use a table that stores data about movies.

1. Load the table with data from a JSON file
2. Perform basic operations like adding, getting, and updating data for individual movies.
3. Use conditional expressions to update movie data only when it meets certain criteria.
4. Query and scan the table to retrieve movie data that meets varying criteria.
"""

from decimal import Decimal
from email import generator
from io import BytesIO
import json
import logging
import os
from pprint import pprint
import requests
from zipfile import ZipFile
from question import Question
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError


logger = logging.getLogger(__name__)

class Animes:
    def __init__(self, dyn_resource):
        self.dyn_resource = dyn_resource
        self.table = None

    
    def exists(self, table_name):
       
        try:
            table = self.dyn_resource.Table(table_name)
            table.load()
            exists = True
        except ClientError as err:
            if err.response['Error']['Code'] == 'ResourceNotFoundException':
                exists = False
            else:
                logger.error(
                    "No encontramos que existiera %s. Porque: %s: %s",
                    table_name,
                    err.response['Error']['Code'], err.response['Error']['Message'])
                raise
        else:
            self.table = table
        return exists
        

    def create_table(self, table_name):
        
        try:
            self.table = self.dyn_resource.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'genero', 'KeyType': 'HASH'},  # Partition key
                    {'AttributeName': 'titulo', 'KeyType': 'RANGE'}  # Sort key
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'genero', 'AttributeType': 'S'},
                    {'AttributeName': 'titulo', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={'ReadCapacityUnits': 10, 'WriteCapacityUnits': 10})
            self.table.wait_until_exists()

        except ClientError as err:
            logger.error(
                "No se pudo crear la tzbla %s. Porque: %s: %s", table_name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return self.table

    def list_tables(self):
        
        try:
            tables = []
            for table in self.dyn_resource.tables.all():
                print(table.name)
                tables.append(table)
        except ClientError as err:
            logger.error(
                "No se puede listar la table. Porque: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return tables

    #Write a batch
    def write_batch(self, animes):
       
        try:
            with self.table.batch_writer() as writer:
                for anime in animes:
                    writer.put_item(Item=anime)
        except ClientError as err:
            logger.error(
                "No pude cargar los datos en la table %s. Porque: %s: %s", self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    

    #Write an item
    def add_anime(self, titulo, genero, año, rating):
        
        try:
            self.table.put_item(
                Item={
                    'genero': genero,
                    'titulo': titulo,
                    'info': {'año': año, 'rating': Decimal(str(rating))}})
        except ClientError as err:
            logger.error(
                "No se pudo agregar %s a la tabla %s. Porque: %s: %s",
                titulo, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
    
    #Read an item
    def get_anime(self, titulo, genero):
        
        try:
            response = self.table.get_item(Key={'genero': genero, 'titulo': titulo})
        except ClientError as err:
            logger.error(
                "No se pudo acceder a %s de la tabla %s. Porque: %s: %s",
                titulo, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Item']

    #Update an item
    def update_anime(self, titulo, genero, rating, año):
       
        try:
            response = self.table.update_item(
                Key={'genero': genero, 'titulo': titulo},
                UpdateExpression="set info.rating=:r, info.año=:a",
                ExpressionAttributeValues={
                    ':r': Decimal(str(rating)), ':a': año},
                ReturnValues="UPDATED_NEW")
        except ClientError as err:
            logger.error(
                "No se pudo actualizar los datos de %s en la tabla %s. Porque: %s: %s",
                titulo, self.table.name,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Attributes']
   
   #Query a table
    def query_animes(self, genero):
       
        try:
            response = self.table.query(KeyConditionExpression=Key('genero').eq(genero))
        except ClientError as err:
            logger.error(
                "No se pueden consultar los animes de el genero %s. Porque: %s: %s", genero,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
        else:
            return response['Items']
    
    #Scan a table
    def scan_animes(self, genero_range):
        
        animes = []
        scan_kwargs = {
            'FilterExpression': Key('genero').between(genero_range['first'], genero_range['second']),
            'ProjectionExpression': "gr, titulo, info.rating",
            'ExpressionAttributeNames': {"gr": "genero"}}
        try:
            done = False
            start_key = None
            while not done:
                if start_key:
                    scan_kwargs['ExclusiveStartKey'] = start_key
                response = self.table.scan(**scan_kwargs)
                animes.extend(response.get('Items', []))
                start_key = response.get('LastEvaluatedKey', None)
                done = start_key is None
        except ClientError as err:
            logger.error(
                "No se pudo escanear. Porque: %s: %s",
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise

        return animes
   
    #Delete an item
    def delete_movie(self, titulo, genero):
      
        try:
            self.table.delete_item(Key={'genero': genero, 'titulo': titulo})
        except ClientError as err:
            logger.error(
                "No se puede borrar %s. Porque: %s: %s", titulo,
                err.response['Error']['Code'], err.response['Error']['Message'])
            raise
   

#Get data
def get_anime_data(anime_file_name):
    
    if not os.path.isfile(anime_file_name):
        print(f"Descargando {anime_file_name}...")
        anime_content = requests.get(
            'https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/samples/moviedata.zip')
        anime_zip = ZipFile(BytesIO(anime_content.content))
        anime_zip.extractall()

    try:
        with open(anime_file_name) as anime_file:
            anime_data = json.load(anime_file, parse_float=Decimal)
    except FileNotFoundError:
        print(f"Archivo {anime_file_name} no encontrado. Primero sube el archivo "
              "para correr este demo.")
        raise
    else:
        return anime_data[:250]

def run_scenario(table_name, anime_file_name, dyn_resource):
    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    print('-'*88)
    print("Bienvenido a DynamoDB demo.")
    print('-'*88)

    animes = Animes(dyn_resource)
    animes_exists = animes.exists(table_name)
    if not animes_exists:
        print(f"\nCreating table {table_name}...")
        animes.create_table(table_name)
        print(f"\nCreated table {animes.table.name}.")

    my_anime = Question.ask_questions([
        Question('titulo', "Ingrese el titulo del anime que gustes añadir: "),
        Question('genero', "A que genero pertenece? ", Question.is_int),
        Question(
            'rating', "En una escala del 1 - 10 cuanto le asignas:",
            Question.is_float, Question.in_range(1, 10)),
        Question('año', "En que año se estreno: ")
    ])
    animes.add_anime(**my_anime)
    print(f"\nAdded '{my_anime['titulo']}' to '{animes.table.name}'.")
    print('-'*88)

    anime_update = Question.ask_questions([
        Question(
            'rating',
            f"\nActualicemos la informacion.\nLe diste un rating de {my_anime['rating']}, que nueva "
            f"calificacion le darias? ", Question.is_float, Question.in_range(1, 10))])

    my_anime.update(anime_update)
    updated = animes.update_anime(**my_anime)
    print(f"\nActualizando '{my_anime['titulo']}' con nuevo rating:")
    pprint(updated)
    print('-'*88)

    if not animes_exists:
        anime_data = get_anime_data(anime_file_name)
        print(f"\nLeeyendo data de '{anime_file_name}' a tu tabla.")
        animes.write_batch(anime_data)
        print(f"\nEscribiendo {len(anime_data)} animes en {animes.table.name}.")
    print('-'*88)

    titulo = "Jujutsu Kaisen"
    if Question.ask_question(
            f"Quieres acceder a la informacion del anime '{titulo}'? (s/n) ",
            Question.is_yesno):
        anime = animes.get_anime(titulo, 2018)
        print("\nEsto es lo que tenemos:")
        pprint(anime)
    print('-'*88)

    ask_for_year = True
    while ask_for_year:
        release_year = Question.ask_question(
            f"\nHagamos una lista de animes lanzados en un año especifico. Busquemos entre 2010 y 2020 ", Question.is_int, Question.in_range(2010, 2020))
        releases = animes.query_animes(release_year)
        if releases:
            print(f"Hubieron {len(releases)} animes lanzados en el año {release_year}:")
            for release in releases:
                print(f"\t{release['titulo']}")
            ask_for_year = False
        else:
            print(f"No conozco animes lanzados en {release_year}!")
            ask_for_year = Question.ask_question("Quieres buscar usar otro año (s/n) ", Question.is_yesno)
    print('-'*88)

    years = Question.ask_questions([
        Question(
            'first',
            f"\nAhora escaneemos los animes lanzados en un rango de años. Introduce un año: ",
            Question.is_int, Question.in_range(2010, 2020)),
        Question(
            'second', "Ingresa otro año: ",
            Question.is_int, Question.in_range(2010, 2020))])
    releases = animes.scan_animes(years)
    if releases:
        count = Question.ask_question(
            f"\nEncontre {len(releases)} animes. Cuantos quieres ver? ",
            Question.is_int, Question.in_range(1, len(releases)))
        print(f"\nAqui estan tus {count} animes:\n")
        print(releases[:count])
    else:
        print(f"No conozco animes lanzados entre el año {years['first']} "
              f"y  {years['second']}.")
    print('-'*88)

    if Question.ask_question(
            f"\nVamos a borrar un anime. Deseas borrar "
            f"'{my_anime['titulo']}'? (s/n)", Question.is_yesno):
        animes.delete_anime(my_anime['titulo'], my_anime['genero'])
        print(f"\nRemoviendo '{my_anime['titulo']}' de la tabla.")
    print('-'*88)

    if Question.ask_question(f"\nDeseas borrar la tabla? (s/n) ", Question.is_yesno):
        animes.delete_table()
        print(f"Deleted {table_name}.")
    else:
        print("No olvides borrar tu tabla, de lo contrario puedes sufrir"
              "cobros en tu cuenta de AWS.")
   

if __name__ == '__main__':
    try:
        run_scenario(
            'test_anime_table', 'anime_data.json', boto3.resource('dynamodb'))
    except Exception as e:
        print(f"Algo sucedio con el demo. Error: {e}")
