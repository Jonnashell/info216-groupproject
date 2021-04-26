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
    full_path = os.path.join(phs_dir, sub_dir)
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

# Instantiate graph
g = Graph()

# Namespaces
ex = Namespace("http://example.org/")
dbp = Namespace("http://dbpedia.org/resource/")
schema = Namespace("https://schema.org/")
dbp_o = Namespace('http://dbpedia.org/ontology/')

# bind namespaces
g.bind('FOAF', FOAF)
g.bind("ex", ex)
g.bind("dbp", dbp)
g.bind("Schema", schema)
g.bind("DBpedia", dbp_o)

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
        [all_resources.update({x['surfaceForm']: response}) for x in response]
        return response
    except Exception as e:
        print('REEEE', e)


def connect_dbpedia_resources(response, team_data, types):
    '''
    Connect the resources gathered from DBpedia's API with the resources from our dataset.
    '''
    if response is None:
        return None
    # check if the queried resource string == the value of the resource in team_data
    result = {t: r for r in response for t in types if r['surfaceForm'] == team_data[t]}
    # MANUALLY add exceptions like 'Has sponsor'
    if 'Has sponsor' in types:
        sponsors = [x[0] for x in team_data['Has sponsor']]
        sponsors = [r for r in response if r['surfaceForm'] in sponsors]
        if sponsors != []:
            sponsor_dict = {'Has sponsor': sponsors}
            result.update(sponsor_dict)
    return result


for team, team_data in team_results.items():
    # try to get resource from DBpedia first
    whitelist = ['Has name', 'Has region', 'Has location']
    # add all resources to one list for later query
    try:
        sponsors = [x[0] for x in team_data['Has sponsor']]
    except KeyError:
        sponsors = []
    resources = [team_data[x] for x in whitelist if x in team_data.keys()] + sponsors
    # query resources
    response = get_dbpedia_resources(resources)
    # connect resources with team_data
    resources = connect_dbpedia_resources(response, team_data, whitelist)
    # 
    team = team.replace(' ', '_')

    ### DATASET STUFF (veldig temp) ###
    #df = dfs.loc[dfs['team'] == team]
    #df = df.drop_duplicates(subset=['start_time','match_id','stage','map_type','map_name','player','team'])

    # loop over rows
    # for index, row in df.iterrows():    
    #     # team, has id, id
    #     pass

# Add player triples to graph (veldig veldig temp, kun basic info)
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