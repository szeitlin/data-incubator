__author__ = 'szeitlin'


def make_blah(num):

    """blah = sum of square of digits mod 59
    and

    4 digit positive int -> int

    >>>blah(6789)
    (6**2 + 7 **2 + 8 **2 + 9 **2) mod 59
    53
    """
    #would be nice to do this with a list comprehension or map reduce fcns
    a = 0

    for i in str(num):
        a += (int(i)**2)

    #print a

    blah = (a % 59)

    return blah

#make_blah(6789)

def des_4dig(num):
    """
    helper function to determine if a number is descending, i.e. if digits decrease left to right

    4 digit positive int -> bool

    >>>des_4dig(6789)
    False
    >>>des_4dig(5432)
    True
    """
    numo = list(str(num))

    b = 0

    for i in range(len(numo)-1):
        flag = False
        if numo[b+1] >= numo[b] :
            break
            return flag
        elif numo[b+1] < numo[b]:
            b +=1
            flag = True

    return flag

# print des_4dig(6789)
# print des_4dig(5432)
# print des_4dig(9999)
# print des_4dig(9998)
# print des_4dig(1018)

def algrabbra():
    """
    returns all possible descending 4-digit integers whose blah is 7.
    as a comma-separated sorted list

    """
    ### start with 9999 (max 4-digit number)
    ###first descending from there would be 9876
    ### could write a separate thing just to come up with these, but I think this is easier?

    unsorted = []

    for num in range(1000, 9999):
        if (des_4dig(num) == True) and (make_blah(num) ==7):
            unsorted.append(num)

    print unsorted

algrabbra()

