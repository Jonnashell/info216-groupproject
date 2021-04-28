import rdflib
import os
import pandas as pd
import re
import spotlight
import owlrl
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.collection import Collection
from rdflib.namespace import RDF, RDFS, XSD, FOAF, OWL
from get_liquipedia_data import team_results, player_results, map_results
from numpy import nan

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
g.bind('owl', OWL)

# Classes and Class Properties. Kan legges inn i egen ontologi-fil kanskje??
# RDFS Player-related class properties
g.add((ex.Player, RDF.type, OWL.Class))
g.add((ex.Player, RDFS.subClassOf, FOAF.Person))
g.add((FOAF.name, RDFS.domain, ex.Player))
g.add((FOAF.name, RDF.type, OWL.DatatypeProperty))
g.add((FOAF.Person, RDFS.subClassOf, dbp.Agent))
g.add((ex.PlayerID, RDF.type, OWL.DatatypeProperty))
g.add((ex.PlayerID, RDFS.domain, ex.Player))
g.add((ex.PlayerID, RDFS.range, RDFS.Literal))
g.add((ex.playsFor, RDFS.domain, ex.Player))
g.add((ex.playsFor, RDFS.range, dbp.SportsTeam))
g.add((ex.playedAgainst, RDFS.subClassOf, FOAF.knows))
g.add((ex.playedAgainst, RDF.type, OWL.ReflexiveProperty))
g.add((ex.playedAgainst, RDF.type, OWL.SymmetricProperty))
g.add((ex.playedAgainst, RDFS.domain, ex.Player))
g.add((ex.playedAgainst, RDFS.range, ex.Player))

# RDFS Team-related class properties
g.add((dbp.Organisation, RDFS.subClassOf, dbp.Agent))
g.add((dbp.SportsTeam, RDFS.subClassOf, dbp.Organisation))
g.add((dbp.SportsTeam, OWL.sameAs, schema.SportsTeam))
g.add((FOAF.name, RDFS.domain, schema.SportsTeam))

# RDFS Match-related class properties
g.add((ex.Match, RDF.type, OWL.Class))
g.add((ex.matchID, RDF.type, OWL.DatatypeProperty))
g.add((ex.matchID, RDFS.domain, ex.Match))
g.add((ex.matchID, RDFS.range, RDFS.Literal))
g.add((ex.matchMap, RDFS.domain, ex.Match))
g.add((ex.matchMap, RDFS.range, ex.Map))
g.add((ex.matchWinner, RDFS.domain, ex.Match))
g.add((ex.matchWinner, RDFS.range, dbp.SportsTeam))
g.add((ex.matchLoser, RDFS.domain, ex.Match))
g.add((ex.matchLoser, RDFS.range, dbp.SportsTeam))
g.add((ex.matchTeamOne, RDFS.domain, ex.Match))
g.add((ex.matchTeamOne, RDFS.range, dbp.SportsTeam))
g.add((ex.matchTeamTwo, RDFS.domain, ex.Match))
g.add((ex.matchTeamTwo, RDFS.range, dbp.SportsTeam))
g.add((ex.matchStartTime, RDFS.domain, ex.Match))

# RDFS Map-related class properties
g.add((ex.Map, RDF.type, OWL.Class))
g.add((ex.hasLocation, RDFS.domain, ex.Map))
g.add((ex.hasLocation, RDFS.domain, dbp.SportsTeam))
g.add((ex.hasLocation, RDFS.range, dbp.Country))
g.add((FOAF.name, RDFS.domain, ex.Map))

# RDFS Tournament-related class properties
g.add((ex.Tournament, RDF.type, OWL.Class))
g.add((FOAF.name, RDFS.domain, ex.Tournament))
g.add((ex.tournamentMatches, RDFS.domain, ex.matchID))  # usikker her, trenger å akseptere mange matchID's i array e.l

# Other class properties
g.add((dbp.Country, OWL.sameAs, schema.Country))
g.add((dbp.Place, OWL.sameAs, schema.Place))

print("Inital Classes and Class properties added to graph. ")


# Spotlight server address
SERVER = "https://api.dbpedia-spotlight.org/en/annotate"

# global list of already queried DBpedia resources
queried_resources = set()
all_resources = {}


def get_dbpedia_resources(resources):
    """
    Requests DBpedia's API with the spotlight module to gather data about resources
    :param resources: list of resources to query (cannot contain list of lists)
    :return list of dicts: response from DBpedia's API
    """
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
    """
    Connect the resources gathered from DBpedia's API with the resources from our dataset.
    """
    global all_resources

    # make sure all_resources is not empty
    if all_resources == {}:
        return None

    # MANUALLY define exceptions in blacklist
    blacklist = ['Has sponsor']

    # get all values in team_data on keys defined in keys
    values = dict(zip(keys, map(team_data.get, keys)))
    # get response from DBpedia that matches the key/value pairs of team_data
    result = {k: all_resources[v] for k, v in values.items() if k not in blacklist and v in all_resources}

    # MANUALLY add exceptions defined in blacklist below
    if 'Has sponsor' in keys:
        sponsors = [x[0] for x in team_data['Has sponsor'] if x[0] in all_resources]
        if sponsors:
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

    # DATASET STUFF (veldig temp)
    # properties needed:
    # 'match_winner', 'map_winner', 'map_loser', 'map_name', 'team_one_name', 'team_two_name'
    # notes:
    # get all rows from map_stats where match_id == match_id from dfs
    df = dfs.loc[dfs['team'] == team]
    df = df.drop_duplicates(subset=['start_time', 'match_id', 'stage', 'map_type', 'map_name', 'player', 'team'])

    # loop over rows
    for index, row in df.iterrows():    
        # team, has id, id
        pass

print("Team triples added to graph.")

# Add player triples to graph
player_data = dfs[['player', 'team']].drop_duplicates()
for row in zip(player_data['player'], player_data['team']):
    # player_entity = URIRef(f"https://liquipedia.net/overwatch/{row[0]}") # placeholder URI?
    player_entity = ex.term(row[0])
    g.add((player_entity, ex.PlayerID, Literal(row[0], datatype=XSD.string)))
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

print("Player triples added to graph.")


# Creating a new dataframe with Overwatch League match data from Statslab
match_df = pd.read_csv(r'phs_data\match_map_stats.csv')
match_df = match_df[["round_start_time", "match_id", "map_name", "team_one_name",
                     "team_two_name", "match_winner"]]
match_df["match_start_date"] = match_df['round_start_time'].str.extract(r'(^\d{4}-\d{2}-\d{2})')
match_df.drop('round_start_time', axis=1, inplace=True)
match_df["tournament"] = "Overwatch League " + match_df['match_start_date'].str.extract(r'(^\d{4})')
match_df.drop_duplicates(subset=["match_id"], keep="first", ignore_index=True, inplace=True)


# Query DBPedia Spotlight for map location resources
all_map_locations = set([map_name['Has location'] for map_name in map_results.values()])
get_dbpedia_resources([all_map_locations])


# Adding match, tournament and map triples to graph
for (index, match_id, map_name, team_one_name, team_two_name, match_winner, match_start_time, tournament) in match_df.itertuples():
    # Create a term for the Match instance subject
    match_entity = ex.term(str(match_id))

    # Remove spaces from terms
    team_one_name = team_one_name.replace(' ', '_')
    team_two_name = team_two_name.replace(' ', '_')
    match_winner = match_winner.replace(' ', '_')
    map_entity_name = map_name.replace(' ', '_')
    map_entity_name = map_entity_name.replace("'", "")

    # Add Match instances with properties to graph
    g.add((match_entity, RDF.type, ex.Match))
    g.add((match_entity, ex.matchID, Literal(match_id, datatype=XSD.string)))

    # Adding Map instances with properties to graph
    if (ex.term(map_entity_name), RDF.type, ex.Map) not in g:
        map_entity = ex.term(map_entity_name)
        map_location = map_results[map_name]['Has location']
        g.add((map_entity, RDF.type, ex.Map))
        g.add((map_entity, FOAF.name, Literal(map_name, datatype=XSD.string)))

        # Add map entity location with DBPedia resource
        map_resource_obj = URIRef(all_resources[map_location]['URI'])
        g.add((map_entity, ex.hasLocation, map_resource_obj))

        # Add types to DBPedia map resource object
        g.add((map_resource_obj, FOAF.name, Literal(map_location, datatype=XSD.string)))
        for namespace, value in [t.split(':') for t in all_resources[map_location]['types'].split(',')]:
            if namespace == 'Wikidata':
                g.add((map_resource_obj, RDF.type, wd.term(value)))
            elif namespace == 'Schema':
                g.add((map_resource_obj, RDF.type, schema.term(value)))
            elif namespace == 'DBpedia':
                g.add((map_resource_obj, RDF.type, dbp.term(value)))

    # Add more Match instance properties
    g.add((match_entity, ex.matchMap, ex.term(map_entity_name)))
    g.add((match_entity, ex.matchTeamOne, ex.term(team_one_name)))
    g.add((match_entity, ex.matchTeamTwo, ex.term(team_two_name)))
    g.add((match_entity, ex.matchWinner, ex.term(match_winner)))
    g.add((match_entity, ex.matchStartTime, Literal(match_start_time, datatype=XSD.string)))

    # Add Tournament instances with properties to graph
    if tournament is not nan:
        tournament_entity_name = tournament.replace(" ", "_")
        tournament_entity = ex.term(tournament_entity_name)
        if (tournament_entity, RDF.type, ex.Tournament) not in g:
            g.add((tournament_entity, RDF.type, ex.Tournament))
            g.add((tournament_entity, FOAF.name, Literal(tournament, datatype=XSD.string)))
        g.add((tournament_entity, ex.tournamentMatches, ex.term(match_id)))

print("Match, tournament and map triples added to graph")


# Noen inferred triples fucker opp shitten til WebVowl, aner ikke hvorfor.
# Kan ha noe med at den oppretter sånn 5000 tripler tho...
# Add inferred triples to the graph
# owl = owlrl.CombinedClosure.RDFS_OWLRL_Semantics(g, False, False, False)
# owl.closure()
# owl.flush_stored_triples()


# Print the graph to terminal
g.serialize(destination='graph.ttl', format='ttl')
print(g.serialize(format='ttl').decode('utf-8'))
