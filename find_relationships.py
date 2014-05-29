__author__ = 'szeitlin'

#identify relationships and write out rels.csv for import to neo4j

#ex. node file where names are nodes, age and works_on are properties of the nodes
# name    age works_on
# Michael 37  neo4j
# Selina  14
# Rana    6
# Selma   4

#ex. rels.csv where start and end refer to implicit indexing in the nodes file, 
#FATHER_OF is a type of relationship, and since is a property of that relationship

# start   end type        since   counter:int
# 1     2   FATHER_OF 1998-07-10  1
# 1     3   FATHER_OF 2007-09-15  2
# 1     4   FATHER_OF 2008-05-03  3
# 3     4   SISTER_OF 2008-05-03  5
# 2     3   SISTER_OF 2007-09-15  7

import itertools
import pandas
from py2neo import node, rel

from py2neo import neo4j
graph_db = neo4j.GraphDatabaseService("http://localhost:7474/db/data/")

referrals = pandas.read_csv("user_codes.txt")

#        user_code referrer_code
# 0   f97c6c37eeca  e5b4a1bb0e5e
# 1   52879656bb35           NaN
# 2   2b633b15919b  840c5cb659b3
# 3   b362c0da9eec           NaN


def melt_users(referrals):
    '''
    convert referrals to a single column list of nodes for import
    drop NAs
    save as df
    write to csv with no index column
    '''

#supposedly concat will by default combine vertically if colnames are the same

#change colnames to be the same

    referrals.columns=["users", "users"]

#concat together all the rows for the first two columns into one long column

    nodes = pandas.concat([referrals.ix[:,0],referrals.ix[:,1]])

    nodes.dropna(inplace=True)

#remove duplicate rows to get unique nodes

    nodes.drop_duplicates()

#write out file for neo4j batch-importer

    nodes.to_csv("nodes.csv", index=False)


def link_primary(referrals):
    '''
    if there's a referrer code in column 2, to points to the user in column 1

    start       end     referred    counter:int
    1           1       REFERRED     1

    '''

#graph_db.create(node({"name": "Alice"}), node({"name": "Bob"}), rel(0, "KNOWS", 1)),)

#print referrals.head(10)

#note that dataframes in pandas look like tables but they're actually based on dicts
#try just getting the columns by treating the column name as a key

usercol = referrals['user_code']

refcol = referrals['referrer_code']

#        graph_db.create(node({"user": "f97c6c37eeca"}), node({"referrer": "e5b4a1bb0e5e"}), rel(start, "recommended", end))


    #str/NaN series --> add new bool column to referrals dataframe

referrals['has_ref'] = refcol.notnull()

def primaries(usercol, refcol, referrals):
    '''
    identify users and refs that are paired
    '''
    i, counter = 1, 0             #i=1 to skip first row
    users, refs = [], []

    for i in range(len(usercol)):
        if referrals['has_ref'].ix[i] == True:
            counter +=1
            users.append(usercol[i])
            refs.append(refcol[i])
        else:
            counter = 0
            continue

    return users, refs


def find_arrows(users, refs):
    '''
    starting just from the primary nodes, identify numbering for start and end
    NOTE: this is just a placeholder that ignores further branching for now
    '''
    start, end= [], []
    i = 0

    #convert to series to use index
    users = pandas.Series(users)
    refs = pandas.Series(refs)

    for i in range(len(users)):
        start.append(i)

    for i in range(len(refs)):
        end.append(i)

    #adjust endpoints
    end = [(x+2) for x in start)] #and this isn't even working the way I want it to?

    return users, refs, start, end

def for_batch_import(referrals, start, end, countlist):
    '''
    generate the relationship file by concatenating the appropriate columns as lists into a new df
    -> return as tab-delimited csv file (doublecheck this)

#ex. rels.csv where start and end refer to implicit indexing in the nodes file,
#FATHER_OF is a type of relationship, and since is a property of that relationship

# start   end type        since   counter:int
# 1     2   FATHER_OF 1998-07-10  1
# 1     3   FATHER_OF 2007-09-15  2
# 1     4   FATHER_OF 2008-05-03  3
# 3     4   SISTER_OF 2008-05-03  5
# 2     3   SISTER_OF 2007-09-15  7
    '''

    start=pandas.Series(start)
    end = pandas.Series(end) #have to convert these to use concat
    df.columns=['start', 'end']
    df = pandas.concat([start, end], axis = 1)


# def creating(users, refs, start, end):
#     '''
#     generate the big create command
#     note that this is just starting from ones that have 1 connection each
#     not sure what to do about ones that have more than that, maybe can find those by implicit connections (?)
#
#     '''
#     nodes1 = [i for i in itertools.repeat("user", times=len(users))]
#     nodes2 = [i for i in itertools.repeat("referrer", times=len(refs))]
#
#     rowlist = []
#
#     for i in range(1, len(users)):
#         rowlist.append(node({nodes1[i]: users[i]}))
#         rowlist.append(node({nodes2[i]: refs[i]}))
#         rowlist.append(rel(start[i], 'recommended',end[i]))
#
#     incubate = graph_db.create(rowlist)

def parens(users, refs, start, end):
    '''
    try to generate using a different example from the fundamentals page
    can see something on the browser but can't view the data wtf.
    '''

    rowlist = []

    #nodes first
    for i in range(11, 100):
        rowlist.append (node(user=users[i]))
        rowlist.append (node(ref = refs[i]))

    #relationships second
    for i in range(11, 100):
        rowlist.append(rel(start[i], "RECOMMENDED", end[i]))

    incubate = graph_db.create(*rowlist) #supposedly the start expands the list & might work better?

    #gives an error Incomplete Read if you try to do the whole thing at once, but
    #looks like you can do this in pieces in order to get the whole thing (?)

    #not sure if this is really necessary, should try +/- the format=pretty part
    neo4j._add_header('X-Stream', 'true;format=pretty')






































