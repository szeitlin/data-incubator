__author__ = 'szeitlin'

#identify relationships and write out rels.csv for import to neo4j

import itertools
import pandas
from py2neo import node, rel
from py2neo import neo4j


def define_cols(referrals):
    '''
    if there's a referrer code in column 2, to points to the user in column 1

    start       end     referred    counter:int
    1           1       REFERRED     1

    '''

    usercol = referrals['user_code']
    refcol = referrals['referrer_code']
    referrals['has_ref'] = refcol.notnull()
    rels = referrals['has_ref']

    return usercol, refcol, rels

def batch_insert_nodes(usercol):
    '''
    this approach creates nodes for both users and referrers, but doesn't create relationships
    '''
    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data")
    batch = neo4j.WriteBatch(graph_db)

    #have to use enumerate to get the index
    for i, user in enumerate(usercol):
        batch.get_or_create_indexed_node("Users", "index", i {
            "index":i, "name": user)
        })

    for i, ref in enumerate(refcol):
        batch.get_or_create_indexed_node("Referrers", "index", i {
            "index": i, "name": ref)
        })

    nodes = batch.submit()


def change_flag_and_count(usercol, rels):
    '''
    identify users and refs that are paired
    '''
    i = 1
    counter = 0             #i=1 to skip first row

    countlist, start, end = [], [], []

    for i in range(len(usercol)):
        if rels.ix[i]==True:
            rels.ix[i] = "REFERRED"
            counter +=1
            countlist.append(counter)

        else:
            rels.ix[i] = ""
            counter = 0
            countlist.append(counter)
            continue

    referrals['counter'] = countlist

    return rels, countlist


def create_db(usercol, refcol, start, end):
    '''
    try to generate relationships using a different example from the fundamentals page

    '''

    graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

    rowlist = []

    #nodes first
    for i in range(11, 100):
        rowlist.append (node(user=usercol[i]))
        rowlist.append (node(ref = refcol[i]))

    #relationships second
    for i in range(11, 100):
        rowlist.append(rel(start[i], "RECOMMENDED", end[i]))

    incubate = graph_db.create(*rowlist) #asterisk expands the list & might work better?

    #gives an error Incomplete Read if you try to do the whole thing at once, but
    #looks like you can do this in pieces in order to get the whole thing (?)

    #not sure if this is really necessary, should try +/- the format=pretty part
    neo4j._add_header('X-Stream', 'true;format=pretty')


def main():

    referrals = pandas.read_csv("user_codes.txt")

    usercol, refcol, rels = define_cols(referrals)
    users, refs = primaries(usercol, refcol, referrals)
    users, refs, start, end = find_arrows(users, refs)
    create_db(users, refs, start, end)

if __name__ == "__main__":
    main()



































