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

# static variable
SERVER = "https://api.dbpedia-spotlight.org/en/annotate"

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

# list of already queried DBpedia resources
queried_resources = set()

for team, team_data in team_results.items():
    # try to get resource from DBpedia first
    whitelist = ['Has name', 'Has region', 'Has location', 'Has sponsor']
    text = [team_data[x][0][0] if x == 'Has sponsor' else team_data[x] for x in whitelist if x in team_data.keys()]
    text = ','.join(text)
    #
    annotations = spotlight.annotate(SERVER, text)
    #
    team = team.replace(' ', '_')
    for annotation in annotations:
        # see which of the team_data values it matched
        if team_data.get('Has name'):
            has_name = re.fullmatch(annotation['surfaceForm'],team_data['Has name'])
        if team_data.get('Has region'):
            has_region = re.fullmatch(annotation['surfaceForm'],team_data['Has region'])
        if team_data.get('Has location'):
            has_location = re.fullmatch(annotation['surfaceForm'],team_data['Has location'])
        if team_data.get('Has sponsor'):
            has_sponsor = re.fullmatch(annotation['surfaceForm'],team_data['Has sponsor'][0][0])
        #
        if has_name is not None:
            # team_entity, FOAF.name, team
            team_entity = URIRef(annotation['URI'])
            g.add((team_entity, FOAF.name, Literal(team_data['Has name'])))
        elif has_region is not None:
            # team_entity, dbp_o:region, region
            region = URIRef(annotation['URI'])
            g.add((team_entity, dbp_o.term('region'), region))
        elif has_location is not None:
            # team_entity, dbp_o:location, location
            location = URIRef(annotation['URI'])
            g.add((team_entity, dbp_o.term('location'), location))
        elif has_sponsor is not None:
            # team_entity, dbp_o:sponsor, sponsor
            sponsor = URIRef(annotation['URI'])
            g.add((team_entity, dbp.term('sponsor'), sponsor))
            # what to do with types
    #
    ### DATASET STUFF (veldig temp) ###
    #df = dfs.loc[dfs['team'] == team]
    #df = df.drop_duplicates(subset=['start_time','match_id','stage','map_type','map_name','player','team'])

    # loop over rows
    # for index, row in df.iterrows():    
    #     # team, has id, id
    #     pass

# Add player triples to graph
# for player, player_data in player_results.items():
#     # try to get resource from DBpedia first
#     #text = ''
#     annotations = spotlight.annotate(SERVER, text)

#     for annotation in annotations:
#         pass

g.serialize(destination='graph.ttl',format='ttl')
print(g.serialize(format='ttl').decode('utf-8'))




##### TEST STUFF


# Add team triples to graph
# for team, team_data in team_results.items():
#     ### DBpedia ###
#     # try to get resource from DBpedia first
#     whitelist = ['Has name', 'Has region', 'Has location', 'Has sponsor']
#     resources = [team_data[x][0][0] if x == 'Has sponsor' else team_data[x] for x in whitelist if x in team_data.keys()]
    
#     # if we already queried the resource, don't do it again
#     queried_resources.update(resources)
#     text = ','.join(resources)
#     #
#     annotations = spotlight.annotate(SERVER, text)
#     #
#     team = team.replace(' ', '_')
#     for annotation in annotations:
#         #
#         for prop in whitelist:
#             # if team_data[prop] exists
#             if team_data.get(prop):
#                 resource_name = re.fullmatch(annotation['surfaceForm'],team_data[prop])
#                 # if we found a resource in DBpedia for this resource, and the name property name is 'Has name'
#                 if resource_name is not None and prop == 'Has name':
#                     team_entity = URIRef(annotation['URI'])
#                     g.add((team_entity, FOAF.name, Literal(team_data['Has name'])))
#                 # if we found a resource in DBpedia for this resource, do some stuff
#                 elif resource_name is not None:
#                     resource_uri = URIRef(annotation['URI'])
#                     g.add((team_entity, dbp_o.term(prop), resource_uri))