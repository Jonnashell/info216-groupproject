import rdflib
import os
import pandas as pd
import re
import spotlight
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.collection import Collection
from rdflib.namespace import RDF, RDFS, XSD, FOAF
from get_liquipedia_data import team_results, player_results, map_results

# Note: importing team_results, and player_results may takes a bit of time.
#       This is due to the API terms of use described in get_liquipedia_data.py

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
    if sub_dir == 'phs_2020':
        df.rename(columns={'esports_match_id': 'match_id', 'tournament_title': 'stage',
                           'team_name': 'team', 'player_name': 'player',
                           'hero_name': 'hero'}, inplace=True)
    # add df_new to df
    all_dfs.append(df)

dfs = pd.concat(all_dfs)
# make all player names lowercase, because of difference in Liquipedia and dataset
dfs.player = dfs.player.apply(lambda x: x.lower())

# Instantiate graph
g = Graph()

# Namespaces
ex = Namespace('http://example.org/')
dbp = Namespace('http://dbpedia.org/resource/')
schema = Namespace('https://schema.org/')
dbp_o = Namespace('http://dbpedia.org/ontology/')
wd = Namespace('http://www.wikidata.org/entity/')

# bind namespaces
g.bind('FOAF', FOAF)
g.bind('ex', ex)
g.bind('dbp', dbp)
g.bind('Schema', schema)
g.bind('DBpedia', dbp_o)
g.bind('Wikidata', wd)

SERVER = "https://api.dbpedia-spotlight.org/en/annotate"

# global list of already queried DBpedia resources
queried_resources = set()
all_resources = {}


def get_dbpedia_resources(resources):
    '''
    Requests DBpedia's API with the spotlight module to gather data about resources
    :param resources: list of resources to query (cannot contain list of lists)
    :return list of dicts: response from DBpedia's API
    '''
    global queried_resources
    server = "https://api.dbpedia-spotlight.org/en/annotate"
    # make sure resource is not already queried, to avoid unnecessary API requests
    resources = [str(x) for x in resources if x not in queried_resources]
    # verify that we still have some resources after filtering out already queried ones
    if resources == []:
        print('All resources were already queried')
        return None
    # add resources to queried_resources
    queried_resources.update(resources)
    # convert list of resources to comma-separated string for API request
    text = ','.join(resources)
    # perform API request
    try:
        response = spotlight.annotate(server, text)
        # add this response to the global variable all_resources
        [all_resources.update({x['surfaceForm']: x}) for x in response]
    except Exception as e:
        print('REEEE', e)


def connect_dbpedia_resources(team_data, keys):
    '''
    Connect the resources gathered from DBpedia's API with the resources from our dataset.
    '''
    global all_resources

    # make sure all_resources is not empty
    if all_resources == {}:
        return None

    # MANUALLY define exceptions in blacklist
    blacklist = ['Has sponsor']

    # get all values in team_data on keys defined in keys
    values = dict(zip(keys, map(team_data.get, keys)))
    # get response from DBpedia that matches the key/value pairs of team_data
    result = {k: all_resources[v] for k,v in values.items() if k not in blacklist and v in all_resources}

    # MANUALLY add exceptions defined in blacklist below
    if 'Has sponsor' in keys:
        sponsors = [x[0] for x in team_data['Has sponsor'] if x[0] in all_resources]
        if sponsors != []:
            sponsor_dict = {'Has sponsor': sponsors}
            result.update(sponsor_dict)
    return result

# Add team triples to graph
for team, team_data in team_results.items():
    team_name = team.replace(' ', '_')
    # add all resources to one list for later query
    try:
        sponsors = [x[0] for x in team_data['Has sponsor']]
        keys = ['Has name', 'Has region', 'Has location', 'Has sponsor']
    except KeyError:
        sponsors = []
        keys = ['Has name', 'Has region', 'Has location']

    # get resources
    resources = [team_data[x] for x in ['Has name', 'Has region', 'Has location'] if x in team_data.keys()] + sponsors
    # query resources (no putput is returned, but global variable 'all_resources' is updated)
    get_dbpedia_resources(resources)
    # connect resources with team_data
    resources = connect_dbpedia_resources(team_data, keys)

    # define team_entity and types
    try:
        team_entity = URIRef(resources['Has name']['URI'])
    except KeyError:
        # this means the team is not a resource in DBpedia.
        team_entity = ex.term(team_name)
        # add type
        g.add((team_entity, RDF.type, schema.SportsTeam))
        
    # add team name
    g.add((team_entity, FOAF.name, Literal(team)))

    # loop over all resources connected to team_data
    for key, response in resources.items():
        try:
            resource_obj = URIRef(response['URI'])
        except TypeError:
            continue
        
        # add resources to team_entity
        # don't add if key is 'Has name' because we use FOAF.name for that earlier.
        if key != 'Has name':
            key = key.split()
            predicate = ex.term(key[0].lower() + key[1].title())
            g.add((team_entity, predicate, resource_obj))

        # add types for all resources
        for ns, value in [t.split(':') for t in response['types'].split(',')]:
            if ns == 'Wikidata':
                g.add((resource_obj, RDF.type, wd.term(value)))
            elif ns == 'Schema':
                g.add((resource_obj, RDF.type, schema.term(value)))
            elif ns == 'DBpedia':
                g.add((resource_obj, RDF.type, dbp.term(value)))


    ### DATASET STUFF (veldig temp) ###
    # properties needed:
    # 'match_winner', 'map_winner', 'map_loser', 'map_name', 'team_one_name', 'team_two_name'
    # notes:
    # get all rows from map_stats where match_id == match_id from dfs
    df = dfs.loc[dfs['team'] == team]
    df = df.drop_duplicates(subset=['start_time','match_id','stage','map_type','map_name','player','team'])

    loop over rows
    for index, row in df.iterrows():    
        # team, has id, id
        pass

# # Add player triples to graph (veldig veldig temp, kun basic info)
player_data = dfs[['player', 'team']].drop_duplicates()
for row in zip(player_data['player'], player_data['team']):
    # player_entity = URIRef(f"https://liquipedia.net/overwatch/{row[0]}") # placeholder URI?
    player_entity = ex.term(row[0])
    g.add((player_entity, ex.playerID, Literal(row[0], datatype=XSD.string)))
    g.add((player_entity, RDF.type, ex.Player))
    g.add((player_entity, ex.playsFor,  dbp.term(row[1].replace(' ', "_"))))

    # Get player role from Liquipedia data
    try:
        player_role = player_results[row[0]]['Has role']
        g.add((player_entity, ex.role, ex.term(player_role)))
    except KeyError:
        continue

    # Get player name from Liquipedia data
    try:
        player_name = player_results[row[0]]['Has name']
        g.add((player_entity, FOAF.name, Literal(player_name, datatype=XSD.string)))
    except KeyError:
        g.add((player_entity, FOAF.name, ex.term('Unknown')))

    # Get player nationality from Liquipedia data
    try:
        player_nationality = player_results[row[0]]['Has nationality']
        country_annotation = spotlight.annotate(SERVER, player_nationality)
        country_URIref = URIRef(country_annotation[0]['URI'])
        g.add((player_entity, dbp_o.term('country'), country_URIref))
    except KeyError:
        continue


# Print the graph to terminal
g.serialize(destination='graph.ttl', format='ttl')
print(g.serialize(format='ttl').decode('utf-8'))