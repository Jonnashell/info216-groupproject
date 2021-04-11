import rdflib
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

# Add team triples to graph
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
            g.add((team_entity, FOAF.name, Literal(team)))
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

# Add player triples to graph
# for player, player_data in player_results.items():
#     # try to get resource from DBpedia first
#     #text = ''
#     annotations = spotlight.annotate(SERVER, text)

#     for annotation in annotations:
#         pass

print(g.serialize(format='ttl').decode('utf-8'))

## SPØR OM TYPES
# https://www.dbpedia-spotlight.org/api
# text: atlanta reign,North America,United States,SecretLab

# simple event model ontology
# hasActors, hasActorType


## Spør om OWL (subClasses, osv.)