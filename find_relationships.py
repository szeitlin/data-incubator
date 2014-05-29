__author__ = 'szeitlin'

#identify relationships for import to neo4j

import pandas

referrals = pandas.read_csv("user_codes.txt")

#try melt to make users the columns and the index/rownames are codes for people they referred

def user_melt():
    '''
    2 columns (user, referrers) --> 1 column per user, 1 row per referrer

    ex.
    user    ref
    Bob     Andy
    Nancy   Andy
    Jim     Bob
    Harry   Jim

    returns:
    user: Bob Jim   Andy
    refs: Jim Harry Bob
                    Nancy

    '''
    melted = referrals.pivot(columns = 'referrer_code', values = 'user_code')

    head(melted)

user_melt()
