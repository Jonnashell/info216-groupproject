import timeit
import rdflib
import os
import pandas as pd
import re
import spotlight
import owlrl
from datetime import datetime
from rdflib import Graph, Namespace, URIRef, Literal, BNode
from rdflib.collection import Collection
from rdflib.namespace import RDF, RDFS, XSD, FOAF, OWL
from get_liquipedia_data import team_results, player_results, map_results
from numpy import nan

# Note: importing team_results, and player_results may takes a bit of time.
#       This is due to the API terms of use described in get_liquipedia_data.py

# import datasets and merge to a single Pandas DataFrame
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
# make all player names lowercase, because of difference in Liquipedia and dataset
dfs.player = dfs.player.apply(lambda x: str(x).lower())


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
g.bind('DBpedia', dbp)
g.bind('Schema', schema)
g.bind('DBpediaOntology', dbp_o)
g.bind('Wikidata', wd)
g.bind('OWL', OWL)


# Classes and Class Properties for esports
# RDFS Player-related class properties
g.add((FOAF.Person, RDFS.subClassOf, dbp.Agent))
g.add((ex.Player, RDFS.subClassOf, FOAF.Person))
g.add((ex.Player, RDF.type, OWL.Class))
g.add((FOAF.name, RDFS.domain, ex.Player))
g.add((FOAF.name, RDF.type, OWL.DatatypeProperty))
g.add((ex.PlayerID, RDF.type, OWL.DatatypeProperty))
g.add((ex.PlayerID, RDFS.domain, ex.Player))
g.add((ex.PlayerID, RDFS.range, RDFS.Literal))
g.add((FOAF.age, RDFS.domain, ex.Player))
g.add((FOAF.age, RDFS.range, RDFS.Literal))
g.add((ex.birthday, RDFS.domain, ex.Player))
g.add((ex.birthday, RDFS.range, RDFS.Literal))
g.add((ex.playedHeroes, RDFS.domain, ex.Player))
g.add((ex.playedHeroes, RDFS.range, RDFS.Literal))
g.add((ex.role, RDFS.domain, ex.Player))
g.add((ex.role, RDFS.range, RDFS.Literal))
g.add((ex.playsFor, RDFS.domain, ex.Player))
g.add((ex.playsFor, RDFS.range, dbp.SportsTeam))
g.add((ex.playedAgainst, RDFS.subClassOf, FOAF.knows))
g.add((ex.playedAgainst, RDF.type, OWL.SymmetricProperty))
g.add((ex.playedAgainst, RDFS.domain, ex.Player))
g.add((ex.playedAgainst, RDFS.range, ex.Player))
g.add((ex.playedWith, RDFS.subClassOf, FOAF.knows))
g.add((ex.playedWith, RDF.type, OWL.SymmetricProperty))
g.add((ex.playedWith, RDFS.domain, ex.Player))
g.add((ex.playedWith, RDFS.range, ex.Player))
g.add((ex.playedMatches, RDFS.domain, ex.Player))
g.add((ex.playedMatches, RDFS.range, ex.Match))
g.add((schema.nationality, RDFS.domain, ex.Player))
g.add((schema.nationality, RDFS.range, schema.Country))
g.add((schema.nationality, RDFS.range, dbp.Country))

# RDFS Team-related class properties
g.add((dbp.Organisation, RDFS.subClassOf, dbp.Agent))
g.add((dbp.SportsTeam, RDFS.subClassOf, dbp.Organisation))
g.add((dbp.SportsTeam, OWL.sameAs, schema.SportsTeam))
g.add((FOAF.name, RDFS.domain, schema.SportsTeam))
g.add((ex.playedAgainst, RDFS.domain, dbp.SportsTeam))
g.add((ex.playedAgainst, RDFS.range, dbp.SportsTeam))
g.add((ex.playedMatches, RDFS.domain, dbp.SportsTeam))
g.add((ex.playedMatches, RDFS.range, ex.matchID))
g.add((ex.hasRegion, RDFS.domain, dbp.SportsTeam))
g.add((ex.hasRegion, RDFS.range, dbp.Continent))
g.add((ex.hasRegion, RDFS.range, schema.Continent))
g.add((ex.hasRegion, RDFS.range, schema.Location))
g.add((ex.hasRegion, RDFS.range, dbp.Location))

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
g.add((ex.matchStartTime, RDFS.range, RDFS.Literal))

# RDFS Map-related class properties
g.add((ex.Map, RDF.type, OWL.Class))
g.add((ex.hasLocation, RDFS.domain, ex.Map))
g.add((ex.hasLocation, RDFS.domain, dbp.SportsTeam))
g.add((ex.hasLocation, RDFS.domain, schema.SportsTeam))
g.add((ex.hasLocation, RDFS.range, dbp.Country))
g.add((ex.hasLocation, RDFS.range, schema.Country))
g.add((FOAF.name, RDFS.domain, ex.Map))

# RDFS Tournament-related class properties
g.add((ex.Tournament, RDF.type, OWL.Class))
g.add((FOAF.name, RDFS.domain, ex.Tournament))
g.add((ex.tournamentMatches, RDFS.domain, ex.matchID))
g.add((ex.tournamentWinner, RDFS.domain, ex.Tournament))
g.add((ex.tournamentWinner, RDFS.range, dbp.SportsTeam))
g.add((ex.tournamentWinner, RDFS.range, schema.SportsTeam))
g.add((ex.tournamentPrizePool, RDFS.domain, ex.Tournament))
g.add((ex.tournamentPrizePool, RDFS.range, RDFS.Literal))

# Other class properties
g.add((dbp.Country, OWL.sameAs, schema.Country))
g.add((dbp.Place, OWL.sameAs, schema.Place))
g.add((dbp.Country, ex.hasRegion, dbp.Continent))
g.add((schema.Country, ex.hasRegion, schema.Continent))

print("Inital Classes and Class properties added to graph. ")


# Spotlight server address
SERVER = "https://api.dbpedia-spotlight.org/en/annotate"

# global list of already queried DBpedia resources
queried_resources = set()
all_resources = {}


def get_dbpedia_resources(resources):
    """
    Requests DBpedia's API with the spotlight module to gather data about resources
    :param resources: iterable object of resources to query (cannot contain list of lists)
    """
    global queried_resources
    server = "https://api.dbpedia-spotlight.org/en/annotate"
    # make sure resource is not already queried, to avoid unnecessary API requests
    resources = [str(x) for x in resources if x not in queried_resources]
    # verify that we still have some resources after filtering out already queried ones
    if not resources:
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
        [all_resources.update({x['surfaceForm']: x}) for x in response if x['similarityScore'] > 0.9]
    except Exception as e:
        print('An exception occurred: ', e)


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
start = timeit.default_timer()
for team, team_data in team_results.items():
    team = team.title()
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
    # query resources (no output is returned, but global variable 'all_resources' is updated)
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
        g.add((team_entity, RDF.type, dbp.SportsTeam))
        
    # add team name
    g.add((team_entity, FOAF.name, Literal(team, datatype=XSD.string)))

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
    
    # get all unique games a team has played
    team_games_df = dfs[dfs.team == team][['match_id', 'team']].drop_duplicates()
    # add all games a team has played to a collection with a blank node
    b = BNode()
    team_games = [ex.term(str(match)) for match in team_games_df['match_id'].to_list()]
    Collection(g, b, team_games)
    # add blank node to team_entity's played matches
    g.add((team_entity, ex.playedMatches, b))

print("Team triples added to graph.")
stop = timeit.default_timer()
print('Time: ', stop - start)

# query player nationalities in DBpedia
nationalities = set([player['Has nationality'] for player in player_results.values()])
[get_dbpedia_resources([n]) for n in nationalities]

start = timeit.default_timer()
# Add player triples to graph
for player, player_data in player_results.items():
    # define player_entity
    player_entity = ex.term(player.replace(' ', '_'))

    # define nationality
    try:
        player_nationality = URIRef(all_resources[player_data['Has nationality']]['URI'])
    except KeyError:
        # this means the nationality does not exist in DBpedia
        player_nationality = ex.term(player_data['Has nationality'].replace(' ', '_'))

    # Add birthday to player entity
    try:
        birthday = '/'.join(player_data['Has birth day'].split('/')[1:4])
        birthday = datetime.strptime(birthday, '%Y/%m/%d')
        g.add((player_entity, ex.birthday, Literal(birthday, datatype=XSD.date)))
    except KeyError:
        # this means there is no data on the player's birthday in DBpedia
        pass

    # Add age to graph
    try:
        g.add((player_entity, FOAF.age, Literal(player_data['Has age'], datatype=XSD.integer)))
    except KeyError:
        # this means there is no data on the player's age in DBpedia
        pass
    
    # Add role to graph
    try:
        g.add((player_entity, ex.role, Literal(str(player_data['Has role'].replace(' ', '_')), datatype=XSD.string)))
    except KeyError:
        # this means there is no data on the player's main role in DBpedia
        pass

    # Add type, id, name, and nationality to graph
    g.add((player_entity, RDF.type, ex.Player))
    g.add((player_entity, ex.PlayerID, Literal(player, datatype=XSD.string)))
    g.add((player_entity, FOAF.name, Literal(player_data['Has name'], datatype=XSD.string)))
    g.add((player_entity, dbp_o.term('nationality'), player_nationality))

    # get all unique games a player has participated in + team name and heroes played
    player_games_df = dfs[(dfs.player == player) & (dfs.hero != 'All Heroes')][
        ['match_id', 'team', 'hero']].drop_duplicates()

    # add all unique games a player has participated in to a blank node
    player_matches = [ex.term(str(match)) for match in player_games_df['match_id'].unique()]
    b = BNode()
    Collection(g, b, player_matches)

    # add the blank node to player_entity's played matches
    g.add((player_entity, ex.playedMatches, b))

    # Add team to player entity (using existing team_entity in graph)
    try:
        player_team = player_games_df.iloc[-1, 1]  # team most recently played with
        team_entity = [s for s in g.subjects(predicate=FOAF.name, object=Literal(player_team, datatype=XSD.string))]
        g.add((player_entity, ex.playsFor, team_entity[0]))
    except IndexError:
        print(f"Could not find team for player {player}")

    # Add player's hero pick rates to a blank node
    total_nr_picked_heroes = player_games_df.hero.value_counts().sum()
    hero_pick_rates = [Literal(f"{hero} pick rate: {round((count / total_nr_picked_heroes) * 100, 5)}%",datatype=XSD.string)
                       for hero, count in player_games_df.hero.value_counts().iteritems()]
    b = BNode()
    Collection(g, b, hero_pick_rates)

    # Add the blank node pwith hero ratios to player_entity
    g.add((player_entity, ex.playedHeroes, b))


print("Player triples added to graph.")
stop = timeit.default_timer()
print('Time: ', stop - start)

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

tournament_matches = {}

start = timeit.default_timer()
# Adding match, tournament and map triples to graph
for (index, match_id, map_name, team_one_name, team_two_name,
     match_winner, match_start_time, tournament) in match_df.itertuples():
    # Create a term for the Match instance subject
    match_entity = ex.term(str(match_id))

    # get team_entities before we rename terms
    try:
        team_one_entity = [s for s in g.subjects(predicate=FOAF.name, object=Literal(team_one_name, datatype=XSD.string))][0]
        team_two_entity = [s for s in g.subjects(predicate=FOAF.name, object=Literal(team_two_name, datatype=XSD.string))][0]
    except IndexError as e:
        print('Could not find team_entity for a team: {}'.format(e))
    except Exception as e:
        print('Exception: Could not find team_entity for a team: {}'.format(e))

    # create team_one_df and team_two_df before we rename terms
    temp = dfs.drop_duplicates(subset=['player', 'match_id', 'team'])
    team_one_df = temp[(temp.match_id == match_id) & (temp.team.str.lower() == team_one_name.lower())]

    temp = dfs.drop_duplicates(subset=['player', 'match_id', 'team'])
    team_two_df = temp[(temp.match_id == match_id) & (temp.team.str.lower() == team_two_name.lower())]

    # Remove spaces from terms
    team_one_name = team_one_name.replace(' ', '_')
    team_two_name = team_two_name.replace(' ', '_')
    match_winner = match_winner.replace(' ', '_')
    map_entity_name = map_name.replace(' ', '_')
    map_entity_name = map_entity_name.replace("'", "")

    # Add Match instances with properties to graph
    g.add((match_entity, RDF.type, ex.Match))
    g.add((match_entity, ex.matchID, Literal(match_id, datatype=XSD.string)))

    # Add players pr. team
    player_entities1 = []
    for player in team_one_df.player.to_list():
        _ = [player_entities1.append(s) for s in g.subjects(predicate=ex.PlayerID, object=Literal(player, datatype=XSD.string))]

    b1 = BNode()
    Collection(g, b1, player_entities1)
    g.add((match_entity, ex.matchTeamOnePlayers, b1))

    player_entities2 = []
    for player in team_two_df.player.to_list():
        _ = [player_entities2.append(s) for s in g.subjects(predicate=ex.PlayerID, object=Literal(player, datatype=XSD.string))]

    b2 = BNode()
    Collection(g, b2, player_entities2)
    g.add((match_entity, ex.matchTeamTwoPlayers, b2))

    # Adding Map instances with properties to graph
    if (ex.term(map_entity_name), RDF.type, ex.Map) not in g:
        map_entity = ex.term(map_entity_name)
        map_location = map_results[map_name]['Has location']
        g.add((map_entity, RDF.type, ex.Map))
        g.add((map_entity, FOAF.name, Literal(map_name, datatype=XSD.string)))

        # Add map entity location with DBPedia resource
        try:
            map_resource_obj = URIRef(all_resources[map_location]['URI'])
        except KeyError:
            map_resource_obj = ex.term(map_location)
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
    g.add((match_entity, ex.matchTeamOne, team_one_entity))
    g.add((match_entity, ex.matchTeamTwo, team_two_entity))
    g.add((match_entity, ex.matchWinner, ex.term(match_winner)))
    g.add((match_entity, ex.matchStartTime, Literal(match_start_time, datatype=XSD.string)))

    # Add Tournament instances with properties to graph
    if tournament is not nan:
        tournament_entity_name = tournament.replace(" ", "_")
        tournament_entity = ex.term(tournament_entity_name)
        if (tournament_entity, RDF.type, ex.Tournament) not in g:
            tournament_matches[tournament_entity] = []
            g.add((tournament_entity, RDF.type, ex.Tournament))
            g.add((tournament_entity, FOAF.name, Literal(tournament, datatype=XSD.string)))

        tournament_matches[tournament_entity].append(match_id)


# Add match_ids to tournament entities
for value in tournament_matches.keys():
    # add all match_ids for matches played in a tournament to a collection with a blank node
    b = BNode()
    t_matches = [ex.term(str(match)) for match in tournament_matches[value]]
    Collection(g, b, t_matches)
    # add blank node to team_entity's played matches
    g.add((value, ex.tournamentMatches, b))

for tournament_entity in tournament_matches.keys():
    # add all match_ids for matches played in a tournament to a collection with a blank node
    b = BNode()
    t_matches = [ex.term(str(match)) for match in tournament_matches[tournament_entity]]
    Collection(g, b, t_matches)
    # add blank node to team_entity's played matches
    g.add((tournament_entity, ex.tournamentMatches, b))


print("Match, tournament and map triples added to graph")
stop = timeit.default_timer()
print('Time: ', stop - start)


# Add inferred triples to the graph (NB! OWL2 is not compatible with WebVOWL)
# owl = owlrl.CombinedClosure.RDFS_OWLRL_Semantics(g, False, False, False)
# owl.closure()
# owl.flush_stored_triples()


# Print the graph to terminal
g.serialize(destination='graph.ttl', format='ttl')
# leave commented until we deliver assignment
# print(g.serialize(format='ttl').decode('utf-8'))
