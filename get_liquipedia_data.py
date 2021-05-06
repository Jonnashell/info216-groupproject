import json
import os
import numpy as np
import pandas as pd
import pathlib
import re
import requests
import time
from datetime import datetime
from math import ceil

# IMPORTANT:
# API Terms of use: https://liquipedia.net/api-terms-of-use
# - Rate limit all HTTP requests to no more than 1 request per 2 seconds.

# Check if we already gathered data from API in the last X hours
hours_trigger = 36
use_cache = False

try:
    f_name = pathlib.Path('player_results.json') # 'team_results.json'
    m_time = datetime.fromtimestamp(f_name.stat().st_mtime)

    time_diff = (datetime.now() - m_time).total_seconds() / 60 / 60
    if time_diff < hours_trigger:
        use_cache = True
    else:
        print('Cache is outdated (at least {} hours old). Retrieving data from Liquipedia API'.format(hours_trigger))
except FileNotFoundError:
    print("file 'team_results.json' does not exist. Retrieving data from Liquipedia API")


if not use_cache:
    def get_entity_data(entity_list, printouts, sort_property):
        """
        :param entity_list: list of all unique entities you want to query in Liquipedia API
        :param printouts: properties the API shall return for each entity
        :param sort_property: property used for sorting entities
        :return: dictionary, key: name of entity, value: dictionary of properties and values of entities
        """
        # distinct between team and player lookup_properties
        base_uri = 'https://liquipedia.net/overwatch/api.php?'
        # Limit the amount of entities queried to 15 at a time. More than this is not possible in the API.
        amount = ceil(len(entity_list) / 15) + 1
        # Loop over groups of entities splitted into "amount" and execute API requests
        last = 0
        all_requests = []
        for i in range(1, amount):
            entities = entity_list[last:15*i]
            if sort_property == 'Has_id_sort':
                lookup_property = 'Has_id_sort'
                query = '||'.join(entities).lower()
            else:
                lookup_property = 'Has_id'
                query = '||'.join(entities)

            last = 15*i
            params = {
                'action': 'ask',
                'query': '[[{}::{}]]|{}|sort={}'.format(lookup_property, query, printouts, sort_property),
                'format': 'json'
                # 'api_version': 3
            }
            headers = {
                'User-Agent': 'UniversityOfBergen-INFO216-Group1',
                'Content-Type': 'application/x-www-form-urlencoded'
            }

            try:
                r = requests.get(base_uri, params=params, headers=headers)
                all_requests.append(r)
            except:
                print('failed reee:', query)

            # sleep for 2 seconds to follow the API terms of use
            time.sleep(2)

        # loop over all entities and retrieve only necessary info
        results = {}
        for request in all_requests:
            for entity, data in request.json()['query']['results'].items():
                if 'Has id' in data['printouts'].keys() and entity.lower() != str(data['printouts']['Has id'][0]).lower():
                    entity = data['printouts']['Has id'][0]
                results[entity.lower()] = {}
                for prop, value in data['printouts'].items():
                    if len(value) > 0:
                        value = value[0]
                        if prop == 'Has ids':
                            value = value['fulltext']
                        elif prop == 'Has birth day' or prop == 'Modification date':
                            value = value['raw']
                        elif prop == 'Has sponsor':
                            value = re.sub('|\[|\]', '', value).split('<br>')
                            value = [(''.join(y[1:]), y[0]) for y in [x.split() for x in value]]
                        elif prop == 'Is active':
                            if value == 't':
                                value = True
                            else:
                                value = False
                        results[entity.lower()].update({prop: value})
        return results

    # import datasets to one big df
    all_dfs = []
    phs_dir = os.getcwd() + r'\phs_data'
    for sub_dir in os.listdir(phs_dir):
        # define full path to the sub_dir
        full_path = os.path.join(phs_dir, sub_dir)
        # if sub_dir is not a directory, it's the map stats dataset
        if os.path.isfile(full_path):
            map_stats = pd.read_csv(full_path)
            continue
        # get new dfs
        df = pd.concat([pd.read_csv(os.path.join(full_path, x)) for x in os.listdir(full_path)])
        #
        # if dataset is 2020
        if sub_dir == 'phs_2020' or sub_dir == 'phs_2021':
            df.rename(columns={'esports_match_id': 'match_id', 'tournament_title': 'stage',
                               'team_name': 'team', 'player_name': 'player',
                               'hero_name': 'hero'}, inplace=True)
        # add df_new to df
        all_dfs.append(df)
    dfs = pd.concat(all_dfs)

    # collect entity lists
    player_list = list(dfs.player.unique())
    team_list = list(dfs.team.unique())

    # players
    printouts = '?Has name|?Has birth day|?Has age|?Has nationality|?Has id|?Has role|?Modification date'
    player_results = get_entity_data(player_list, printouts, 'Has_id_sort')

    # teams
    printouts = '?Has name|?Has region|?Has location|?Has site|?Has twitter|?Has instagram profile|?Was created|?Is active|?Is tier|?Has sponsor|?Modification date'
    team_results = get_entity_data(team_list, printouts, 'Has_name')

    # output results from API to .json files as cache
    with open('team_results.json', 'w', encoding='utf-8') as f:
        json.dump(team_results, f)
    with open('player_results.json', 'w', encoding='utf-8') as f:
        json.dump(player_results, f)


# Get data from cache
with open('player_results.json', 'r', encoding='utf-8') as f:
    player_results = json.load(f)
with open('team_results.json', 'r', encoding='utf-8') as f:
    team_results = json.load(f)
with open('map_results.json', 'r', encoding='utf-8') as f:
    map_results = json.load(f)


# Some players have changed their id's after playing an OWL game
liquipedia_players = set(player_results.keys())
dataset_players = set(dfs.player.unique())

for player in (dataset_players - liquipedia_players):
    pass
    # add each player with their info to 'player_results' dictionary