import requests
import lxml.html as html
import pandas as pd
import numpy as np
from time import time
from datetime import datetime
import os
from unidecode import unidecode


def get_single_page_response(url):
    # Downloading data for a given status/page -> making sure the data is downladed (status_code == 200)
    status = True
    try_number = 1
    sleep_time = 1  # s
    while status:
        response = requests.get(url)
        if response.status_code != 200:
            if try_number <= 10:
                print(
                    f"Status code is {response.status_code} at try number {try_number}, entering sleep for {sleep_time} second(s)")
                try_number += 1
                time.sleep(sleep_time)
            else:
                print(
                    f"Oops!  Maximum number of tries was reached ({try_number}).  Run the code again...")
                break
        else:
            status = False
    return response


def single_page_projects(response):
    home = response.content.decode('utf-8')
    parsed = html.fromstring(home)
    # Project name
    projects_names = parsed.xpath('//div[@class="layer-content"]/a/text()')
    # Project url
    projects_url = parsed.xpath('//div[@class="layer-media"]/a/@href')
    # Project image
    projects_images = parsed.xpath('//div[@class="layer-media"]/a/img/@src')
    # Project Mandante + Arquitecto text
    projects_mandante_arquitecto = parsed.xpath(
        '//div[@class="layer-content"]/div/text()')
    # Project Mandante
    projects_mandante = projects_mandante_arquitecto[0::2]
    # Project Arquitecto
    projects_arquitecto = projects_mandante_arquitecto[1::2]

    projects_dict = {'name': projects_names,
                     'url': projects_url,
                     'image': projects_images,
                     'mandante': projects_mandante,
                     'arquitecto': projects_arquitecto}
    projects_df = pd.DataFrame.from_dict(data=projects_dict)
    return projects_df


def get_state_url(state):
    # States
    state_dict = {'en-proceso': 'en-proceso/',
                  'pre-certificacion': 'pre-certificacion/',
                  'certificacion': 'certificacion/',
                  'sello-plus': 'sello-plus/'}
    state_url = state_dict[state]
    return state_url


def get_number_of_pages(state):
    state_url = get_state_url(state)
    url = HOME_URL + state_url
    response = get_single_page_response(url)
    home = response.content.decode('utf-8')
    parsed = html.fromstring(home)
    # Number of pages
    number_of_pages = parsed.xpath(
        '//div[@class="paginate"]/a[@class="page-numbers"]/text()')
    number_of_pages = [int(x) for x in number_of_pages]
    if number_of_pages:
        last_page = max(number_of_pages)
    else:
        last_page = 1
    return last_page


def get_project_page_data(state_df, state):

    # Request processing
    main_df = pd.DataFrame()
    project_number = 1
    for url in state_df['url']:
        response = get_single_page_response(url)
        home = response.content.decode('utf-8')
        parsed = html.fromstring(home)

        # Project Name
        project_name = parsed.xpath('//h1[@class="entry-title"]/text()')
        project_name = project_name[0].strip() if project_name else None

        # Project Image
        project_image_url = parsed.xpath(
            '//figure[@class="wp-block-image size-large"]/img/@src')
        # project_image_url = parsed.xpath('//figure/img/@src')
        project_image_url = project_image_url[0].strip().replace(
            'http:', 'https:') if project_image_url else None
        # project_image_url = project_image_url if project_image_url.startswith(
        #     'https://www.certificacionsustentable.cl') else 'https://www.certificacionsustentable.cl' + project_image_url

        # Entry date
        project_entry_date = parsed.xpath(
            '//time[@class="entry-date published"]/@datetime')
        project_entry_date = project_entry_date[0][:10].strip(
        ) if project_entry_date else None

        project_dict = {'project_name': project_name,
                        'project_image_url': project_image_url,
                        'project_entry_date': project_entry_date}

        # Project details
        # keys
        project_details_keys = parsed.xpath(
            '//div[@class="entry-content"]//li/b/text()')
        # Step 1: Remove initial and trailing spaces and colons
        project_details_keys = [item.strip().replace(
            ':', '').rstrip(':') for item in project_details_keys]
        # Step 2: Remove accents using unidecode
        project_details_keys = [unidecode(item)
                                for item in project_details_keys]

        # values
        project_details_values = parsed.xpath(
            '//div[@class="entry-content"]//li/text()')
        project_details_values = [item.strip().replace(
            ':', '').rstrip(':') for item in project_details_values]
        # dictionary
        project_details_dict = dict(
            zip(project_details_keys, project_details_values))

        project_details_keys = ['Mandante',
                                'Arquitecto',
                                'Unidad tecnica',
                                'Asesor',
                                'Entidad Evaluadora',
                                'Region',
                                'Comuna',
                                'Version de certificacion',
                                'Nivel obtenido',
                                'Fecha de logro obtenido',
                                'Puntaje obtenido',
                                'Asesor precertificacion',
                                'Entidad evaluadora precertificacion',
                                'Asesor certificacion',
                                'Entidad evaluadora certificacion']

        for key in project_details_keys:
            project_dict[key] = project_details_dict.get(key, None)

        print(
            f'  * {project_name} ({project_number} out of {state_df.shape[0]})')

        project_df = pd.DataFrame.from_dict(
            data=project_dict, orient='index').T
        column_name_dict = {'project_name': 'project_name',
                            'project_image_url': 'project_image_url',
                            'project_entry_date': 'project_entry_date',
                            'Mandante': 'mandante',
                            'Arquitecto': 'arquitecto',
                            'Unidad tecnica': 'unidad_tecnica',
                            'Asesor': 'asesor',
                            'Entidad Evaluadora': 'entidad_evaluadora',
                            'Region': 'region',
                            'Comuna': 'comuna',
                            'Version de certificacion': 'version_certificacion',
                            'Nivel obtenido': 'nivel_obtenido',
                            'Fecha de logro obtenido': 'fecha_logro_obtenido',
                            'Puntaje obtenido': 'puntaje_obtenido',
                            'Asesor precertificacion': 'asesor_precertificacion',
                            'Entidad evaluadora precertificacion': 'entidad_evaluadora_precertificacion',
                            'Asesor certificacion': 'asesor_certificacion',
                            'Entidad evaluadora certificacion': 'entidad_evaluadora_certificacion'}

        # Rename columns using the dictionary
        project_df = project_df.rename(columns=column_name_dict)

        # Creating dataframes
        main_df = pd.concat([main_df, project_df])
        project_number += 1

    # Cleaning
    # Removing duplicates
    main_df.drop_duplicates(inplace=True)
    # Reseting indexes
    main_df.reset_index(drop=True, inplace=True)
    # Formating
    to_save_df = main_df.copy()
    # Saving data into a local folder or database
    path = './data/raw/'
    if not os.path.exists(path):
        os.makedirs(path)

    filename = f'{datetime.now().strftime("%Y_%m_%d")}' + \
        '-' + state + '-2.csv'
    print(f'Saving {filename} file')
    to_save_df.to_csv(path+filename, index=False, encoding="utf-8-sig")


def all_pages_projects(state):

    # get state url
    state_url = get_state_url(state)

    # Get number of pages per state
    number_of_pages = get_number_of_pages(state)

    # ---
    pagination = True
    initial_round = True

    # -------
    # Request processing
    main_df = pd.DataFrame()
    while pagination:
        try:
            if initial_round:
                page_number = 1
                url = HOME_URL + state_url
                response = get_single_page_response(url)
                # home = response.content.decode('utf-8')
                # parsed = html.fromstring(home)
                initial_round = False

            else:
                page_number += 1
                if page_number <= number_of_pages:
                    url = HOME_URL + state_url + \
                        'page/' + str(page_number) + '/'
                    response = get_single_page_response(url)
                else:
                    break

            # Creating dataframes
            print(f'- Page {page_number} out of {number_of_pages}')
            projects_df = single_page_projects(response)
            main_df = pd.concat([main_df, projects_df])

        except:
            pagination = False
            print('Failed !!!')
            path = './data/raw/'
            if not os.path.exists(path):
                os.makedirs(path)

            filename = state + '_failed.txt'
            f = open(path+filename, "w+")
            f.write(
                f'The procces stopped at {page_number} out of {number_of_pages}')
            f.close()

    # Cleaning
    # Removing duplicates
    main_df.drop_duplicates(inplace=True)
    # Reseting indexes
    main_df.reset_index(drop=True, inplace=True)
    # Formating
    to_save_df = main_df.copy()
    # Saving data into a local folder or database
    path = './data/raw/'
    if not os.path.exists(path):
        os.makedirs(path)

    filename = f'{datetime.now().strftime("%Y_%m_%d")}' + \
        '-' + state + '-1.csv'
    print(f'Saving {filename} file')
    to_save_df.to_csv(path+filename, index=False, encoding="utf-8-sig")
    # Going into each single project page
    print(
        f'\nGetting inside the {state} process: {main_df.shape[0]} projects to download...')
    print('-------------------------------------------------------------------------------\n')
    get_project_page_data(main_df, state)


def main(states):
    start_time = time()
    for state in states:
        print('\n----------------------------')
        print(f'Starting {state} process')
        print('----------------------------\n')
        all_pages_projects(state)
    elapsed_time = time() - start_time
    print("\nElapsed time: %0.2f seconds." % elapsed_time)


if __name__ == '__main__':
    states = ['en-proceso', 'pre-certificacion', 'certificacion', 'sello-plus']
    # states = ['sello-plus']
    # state = 'certificacion'
    HOME_URL = 'https://www.certificacionsustentable.cl/'
    main(states)
