import pandas
import re
import requests
import time
from math import ceil

## IMPORTANT: 
# API Terms of use: https://liquipedia.net/api-terms-of-use
# - Rate limit all HTTP requests to no more than 1 request per 2 seconds.

def get_entity_data(column_name, printouts, sort_property):
    '''
    :param column_name: name of column in DataFrame that we want data about
    :param printouts: properties the API shall return for each entity
    :param sort_property: property used for sorting entities
    :return: dictionary, key: name of entity, value: dictionary of properties and values of entities
    '''
    global df
    base_uri = 'https://liquipedia.net/overwatch/api.php?'
    # Limit the amount of entities queried to 15 at a time. More than this is not possible in the API.
    amount = ceil(df.get(column_name).unique().size / 15) + 1
    # Loop over groups of entities splitted into "amount" and execute API requests
    last = 0
    all_requests = []
    for i in range(1, amount):
        entities = [df.get(column_name).unique()[last:15*i]][0]
        query = '||'.join(entities)
        last = 15*i
        #
        params = {
            'action': 'ask',
            'query': '[[Has_id::{}]]|{}|sort={}'.format(query, printouts, sort_property),
            'format': 'json'
            #'api_version': 3
        }
        headers = {
            'User-Agent': 'UniversityOfBergen-INFO216-Group1',
            'Content-Type': 'application/x-www-form-urlencoded'
        }
        #
        try:
            r = requests.get(base_uri, params=params, headers=headers)
            all_requests.append(r)
        except:
            print('failed reee:', query)
        #
        # sleep for 2 seconds to follow the API terms of use
        time.sleep(2)
    #
    #
    # loop over all entities and retrieve only necessary info
    results = {}
    for request in all_requests:
        for entity, data in request.json()['query']['results'].items():
            results[entity] = {}
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
                    results[entity].update({prop: value})
    return results

# load csv file / dataset
df = pandas.read_csv(r'phs_2020\phs_2020_1.csv')

# players
printouts = '?Has name|?Has birth day|?Has age|?Has nationality|?Has ids|?Has role|?Modification date'
player_results = get_entity_data('player_name', printouts, 'Has_id_sort')

# teams
printouts = '?Has name|?Has region|?Has location|?Has site|?Has twitter|?Has instagram profile|?Was created|?Is active|?Is tier|?Has sponsor|?Modification date'
team_results = get_entity_data('team_name', printouts, 'Has_name')

# maps
map_results = {"Hanamura": {"Has name": "Hanamura", "Has location": "Japan", "Has type": "Assault", "Has release date": "2015-10-27"},
               "Horizon": {"Has name": "Horizon Lunar Colony", "Has location": "Moon", "Has type": "Assault", "Has release date": "2017-06-20"},
               "Paris": {"Has name": "Paris", "Has location": "France", "Has type": "Assault", "Has release date": "2016-05-24"},
               "Temple of Anubis": {"Has name": "Temple of Anubis", "Has location": "Egypt", "Has type": "Assault", "Has release date": "2015-10-27"},
               "Volskaya": {"Has name": "Volskaya Industries", "Has location": "Russia", "Has type": "Assault", "Has release date": "2015-10-27"},
               "Dorado": {"Has name": "Dorado", "Has location": "Mexico", "Has type": "Esscort", "Has release date": "2015-10-27"},
               "Havana": {"Has name": "Havana", "Has location": "Cuba", "Has type": "Escort", "Has release date": "2019-05-07"},
               "Junkertown": {"Has name": "Junkertown", "Has location": "Australia", "Has type": "Escort", "Has release date": "2017-09-19"},
               "Rialto": {"Has name": "Rialto", "Has location": "Italy", "Has type": "Escort", "Has release date": "2018-05-03"},
               "Route 66": {"Has name": "Route 66", "Has location": "United States", "Has type": "Escort", "Has release date": "2016-03-22"},
               "Watchpoint: Gibraltar": {"Has name": "Watchpoint: Gibraltar", "Has location": "United Kingdom", "Has type": "Escort", "Has release date": "2015-10-27"},
               "Busan": {"Has name": "Busan", "Has location": "South Korea", "Has type": "Control", "Has release date": "2016-05-24"},
               "Ilios": {"Has name": "Ilios", "Has location": "Greece", "Has type": "Control", "Has release date": "2016-03-08"},
               "Lijang Tower": {"Has name": "Lijang Tower", "Has location": "China", "Has type": "Control", "Has release date": "2016-02-09"},
               "Nepal": {"Has name": "Nepal", "Has location": "Nepal", "Has type": "Control", "Has release date": "2016-02-09"},
               "Oasis": {"Has name": "Oasis", "Has location": "Iraq", "Has type": "Control", "Has release date": "2017-01-03"},
               "Blizzard World": {"Has name": "Blizzard World", "Has location": "United States", "Has type": "Hybrid", "Has release date": "2018-01-23"},
               "Eichenwalde": {"Has name": "Eichenwalde", "Has location": "Germany", "Has type": "Hybrid", "Has release date": "2016-09-06"},
               "Hollywood": {"Has name": "Hollywood", "Has location": "United States", "Has type": "Hybrid", "Has release date": "2015-11-10"},
               "King's Row": {"Has name": "King's Row", "Has location": "", "Has type": "Hybrid", "Has release date": "2015-10-27"},
               "Numbani": {"Has name": "Numbani", "Has location": "Africa", "Has type": "Hybrid", "Has release date": "2015-10-27"}
              }