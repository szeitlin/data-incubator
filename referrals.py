__author__ = 'szeitlin'

from collections import Counter
import pandas
import pandasql

##read in user_codes.txt (csv) and fellows.csv(spaces/newlines)

referrals = pandas.read_csv("user_codes.txt")

fellows = pandas.read_csv("fellows.csv", sep = "\n") #check syntax for this

## find fraction of successful fellows referred by another user (doublecheck the question for exact wording!)

#count n of users (total_users = nrows = 2086)
print referrals.count()

# count returns nrows for both columns

#tried checking if any referrers were also users, looks like the answer is no (referral codes don't overlap with user codes)

#this matches fellows with the identical user code in the other file (59)
   # q = """SELECT fellows, user_code
   #        FROM fellows join referrals
   #        ON fellows = user_code;"""

# of the successful fellows, check if any of those codes have referral codes (54)
q = """SELECT fellows, user_code, referrer_code
        FROM fellows join referrals
        WHERE (fellows = user_code)
        GROUP BY fellows, referrer_code
        HAVING (referrer_code != "NaN");"""

# Execute your SQL command against the pandas frame
fraction = pandasql.sqldf(q, locals())

print fraction


def each_viral_coeff():
    """ want the viral coefficient for each user,
    i.e. if a user refers another user (ideally including if that user refers another user...
    not sure how to do this without making a graph database, and waiting to hear back from neo4j, so...

    """

# start with this: for each referrer code, how many users have that same code?
#returns a dict of referrer_code: count

refs = Counter(referrals['referrer_code'])

print refs

def avg_each_viral_coeff():
    """
     average the individual viral coeffs (6)
    """
    for value in refs.values():
        total += value
        entries +=1

    print "# entries is " + str(entries) #344

#the rest of the users were not referred, but they still count in the denominator

    others = 2086-entries

    avg_coeff = float(total/(entries + others))

    print avg_coeff

def avg_user_refs():
    """
    want the number of users the average user ultimately refers (Including refs of refs)
    """

def reward_totals():
    """
    want the number of dollars paid to users for referrals.

    this will need to total, for each user, the number of referrals and then calculate based on the reward table.

    """