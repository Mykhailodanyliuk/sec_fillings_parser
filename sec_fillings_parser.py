import datetime
import json
import os
import shutil
import time
from zipfile import ZipFile

import pymongo
import requests

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0",
}


def get_collection_from_db(data_base, collection, client):
    db = client[data_base]
    return db[collection]


def download_file_requests(url, file_name):
    try:
        response = requests.get(url, headers=headers)
        open(file_name, "wb").write(response.content)
    except:
        time.sleep(60)
        print('problem')
        download_file_requests(url, file_name)
    print('File is downloaded')


def delete_directory(path_to_directory):
    if os.path.exists(path_to_directory):
        shutil.rmtree(path_to_directory)
    else:
        print("Directory does not exist")


def create_directory(path_to_dir, name):
    mypath = f'{path_to_dir}/{name}'
    if not os.path.isdir(mypath):
        os.makedirs(mypath)


def upload_sec_fillings_data():
    sec_data_collection = get_collection_from_db('db', 'sec_data', client)
    update_collection = get_collection_from_db('db', 'update_collection', client)
    current_directory = os.getcwd()
    directory_name = 'sec'
    path_to_directory = f'{current_directory}/{directory_name}'
    delete_directory(path_to_directory)
    create_directory(current_directory, directory_name)
    path_to_zip = f'{path_to_directory}/submissions.zip'
    download_file_requests('https://www.sec.gov/Archives/edgar/daily-index/bulkdata/submissions.zip',
                           path_to_zip)
    last_len_records = sec_data_collection.estimated_document_count()
    existed_ciks = [x.get('cik') for x in sec_data_collection.find({}, {'cik': 1, '_id': 0})]
    with ZipFile(path_to_zip, 'r') as zip:
        zip_files = zip.namelist()
        zip_files = [file[3:13] for file in zip_files if len(file) == 18]
        zip_files = [f'CIK{file}.json' for file in list(set(zip_files).difference(existed_ciks))]
        for file in zip_files:
            zip.extract(file, path=path_to_directory, pwd=None)
            with open(f'{path_to_directory}/{file}', 'r') as json_file:
                new_sec_company_data = json.load(json_file)
                try:
                    sec_data_collection.insert_one(
                        {'cik': new_sec_company_data.get('cik').zfill(10), 'ein': new_sec_company_data.get('ein'),
                         'sic': new_sec_company_data.get('sic'), 'name': new_sec_company_data.get('name'),
                         'upload_date': datetime.datetime.now(), 'data': new_sec_company_data,
                         'tickers': new_sec_company_data.get('tickers')})
                except pymongo.errors.DuplicateKeyError:
                    continue
    delete_directory(path_to_directory)
    total_records = sec_data_collection.estimated_document_count()
    update_query = {'name': 'sec_data', 'new_records': total_records - last_len_records, 'total_records': total_records,
                    'update_date': datetime.datetime.now()}
    if update_collection.find_one({'name': 'sec_data'}):
        update_collection.update_one({'name': 'sec_data'}, {"$set": update_query})
    else:
        update_collection.insert_one(update_query)
    npi_collection = get_collection_from_db('db', 'npi_data', client)
    if update_collection.find_one({'name': 'npi_data'}):
        update_collection.update_one({'name': 'npi_data'}, {
            "$set": {'name': 'npi_data', 'new_records': 0, 'total_records': npi_collection.estimated_document_count(),
                     'update_date': datetime.datetime.now()}})
    else:
        update_collection.insert_one(
            {'name': 'npi_data', 'new_records': 0, 'total_records': npi_collection.estimated_document_count(),
             'update_date': datetime.datetime.now()})


if __name__ == '__main__':
    while True:
        client = pymongo.MongoClient('mongodb://localhost:27017')
        upload_sec_fillings_data()
        client.close()
