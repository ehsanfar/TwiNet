import pymysql.cursors
from collections import defaultdict, Counter
from os.path import dirname, abspath, isfile
import json
import random
import time, datetime
import re
import csv
import statistics

from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem.porter import *
# stemmer = PorterStemmer()

from itertools import islice

# Connect to the database
d = dirname(dirname(abspath(__file__))) + '/dataset/'

conn1 = pymysql.connect(host='localhost',
                             user='root',
                             password='1574',
                             db='ehsan',
                             charset='utf8mb4',
                             cursorclass=pymysql.cursors.DictCursor)

conn2 = pymysql.connect(host='localhost',
                           user='root',
                           password='1574',
                           db='twitternetwork',
                           charset='utf8mb4',
                           cursorclass=pymysql.cursors.DictCursor)

class PorterCastleStemmer(PorterStemmer):
    """ A wrapper around porter stemmer with a reverse lookip table """

    def __init__(self, *args, **kwargs):
        super(self.__class__, self).__init__(*args, **kwargs)
        self._stem_memory = defaultdict(set)
        # switch stem and memstem
        self._stem = self.stem
        self.stem = self.memstem

    def memstem(self, word):
        """ Wrapper around stem that remembers """
        stemmed_word = self._stem(word)
        self._stem_memory[stemmed_word].add(word)
        return stemmed_word

    def unstem(self, stemmed_word):
        """ Reverse lookup """
        return sorted(self._stem_memory[stemmed_word], key=len)

    def saveJson(self):
        global d
        # print(self._stem_memory)
        tempstemmemory = {w: list(s) for w,s in self._stem_memory.items()}
        with open(d + 'stem_memory.json', 'w') as outfile:
            json.dump(tempstemmemory, outfile)


stemmer = PorterCastleStemmer()

interval = (datetime.datetime.now() - datetime.datetime(2017, 7, 1, 0, 0)).days
def saveStatusRetweeterJSON():
    # d = dirname(dirname(abspath(__file__))) + '/dataset/'
    global d

    with open(d+ 'status_retweeter_dict.json', 'r') as infile: 
        status_retweeter_dict = json.load(infile)

    allretweetscount = sum([len(l) for l in status_retweeter_dict.values()])
    print('length of retweeters:', allretweetscount)
        
    conn1 = pymysql.connect(host='localhost',
                                 user='root',
                                 password='1574',
                                 db='ehsan_08_07',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        # with conn1.cursor() as cursor:
        #     # Create a new record
        #     sql = "INSERT INTO `users` (`email`, `password`) VALUES (%s, %s)"
        #     cursor.execute(sql, ('webmaster@python.org', 'very-secret'))
        #
        # # conn1 is not autocommit by default. So you must commit to save
        # # your changes.
        # conn1.commit()
        
        with conn1.cursor() as cursor:
            # Read a single record
            # sql = "SELECT `retweeter_id`, `main_user_id` FROM `retweeters` WHERE `id`<12000"
            sql = "SELECT * FROM `retweeters`"
            #('webmaster@python.org',)
            cursor.execute(sql)
            result = cursor.fetchall()
            status_retweeter_dict = defaultdict(list)
            for r in result:
                print(r)
                print(r['status_id'], r['retweeter_id'])
                status_retweeter_dict[int(r['status_id'])].append(int(r['retweeter_id']))
            
            retweeterlength = []
            for status in status_retweeter_dict:  
                # print(len(status_retweeter_dict[status]))
                retweeterlength.append(len(status_retweeter_dict[status]))
            
            print("average retweeter length:", sum(retweeterlength)/len(retweeterlength))
            with open('status_retweeter_dict.json', 'w') as outfile:
                json.dump(status_retweeter_dict, outfile)



    finally:
        conn1.close()
        with open(d+ 'status_retweeter_dict.json', 'r') as infile: 
            status_retweeter_dict = json.load(infile)

        allretweetscount = sum([len(l) for l in status_retweeter_dict.values()])
        print('length of retweeters:', allretweetscount)

def saveStatusUserJSON(): 
    d = dirname(dirname(abspath(__file__))) + '/dataset/'
  
    conn1 = pymysql.connect(host='localhost',
                                 user='root',
                                 password='1574',
                                 db='ehsan_08_07',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        
        with conn1.cursor() as cursor:
            # Read a single record
            # sql = "SELECT `retweeter_id`, `main_user_id` FROM `retweeters` WHERE `id`<12000"
            sql = "SELECT id, user_id FROM `statuses`"
            #('webmaster@python.org',)
            cursor.execute(sql)
            result = cursor.fetchall()
            status_user_dict = defaultdict(int)
            for r in result:
                # if random.random()<0.1:
                # if random.random()< 0.01:
                print(r['id'], r['user_id'])    
                # if r['user_id'].isdigit():
                status_user_dict[r['id']] = int(r['user_id'])
            
            print("average retweeter length:", len(status_user_dict))
            with open(d + 'status_user_dict.json', 'w') as outfile:
                json.dump(status_user_dict, outfile)

    finally:
        conn1.close()


def saveUserRetweeterJSON(): 
    global d 
    with open(d + 'status_user_dict.json', 'r') as infile:
        status_user_dict = json.load(infile)
    
    with open(d + 'status_retweeter_dict.json', 'r') as infile: 
        status_retweeter_dict = json.load(infile)    
    
    user_retweeter_dict = defaultdict(list)
    for s, r in status_retweeter_dict.items(): 
        user_retweeter_dict[status_user_dict[s]].extend(r)
    
    with open(d + 'user_retweeter_dict.json', 'w') as outfile:
        json.dump(user_retweeter_dict, outfile)


def saveStatusTimeJson(): 
    d = dirname(dirname(abspath(__file__))) + '/dataset/'
  
    conn1 = pymysql.connect(host='localhost',
                                 user='root',
                                 password='1574',
                                 db='ehsan_08_07',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)

    try:
        
        with conn1.cursor() as cursor:
            # Read a single record
            # sql = "SELECT `retweeter_id`, `main_user_id` FROM `retweeters` WHERE `id`<12000"
            sql = "SELECT id, created_at FROM `statuses`"
            #('webmaster@python.org',)
            cursor.execute(sql)
            result = cursor.fetchall()
            status_time_dict = defaultdict(int)
            for r in result:
                # if random.random()<0.1:
                # if random.random()< 0.01:
                # print(r['id'], r['created_at'])    
                # if r['user_id'].isdigit():
                time_seconds = int(time.mktime(r['created_at'].timetuple()))
                # print(time_seconds)
                status_time_dict[int(r['id'])] = time_seconds
            
            print("length of status time dict:", len(status_time_dict))
            with open(d + 'status_time_dict.json', 'w') as outfile:
                json.dump(status_time_dict, outfile)

    finally:
        conn1.close()


def saveStatusRetTimeJSON():
    # d = dirname(dirname(abspath(__file__))) + '/dataset/'
    global d
    status_rettime_dict = defaultdict(list)
    status_retcount_dict = defaultdict(int)
    with open(d + 'retweet_data_bulk.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # print(row['original_tweet_id'], row['retweet_time'])
            a = re.search(r'\w+\s(?P<month>\w+)\s(?P<day>\d+)\s(?P<hour>\d+):(?P<minute>\d+):.+\s(?P<year>\d+)$', row['retweet_time'])
            newt = "%s %s %s:%s %s"%(a.group('month'), a.group('day'), a.group('hour'), a.group('minute'), a.group('year'))
            dt = datetime.datetime.strptime(newt, '%b %d %H:%M %Y')
            time_seconds = int(time.mktime(dt.timetuple()))
            # print(dt, time_seconds)
            status_rettime_dict[int(row['original_tweet_id'])].append(time_seconds)
            status_retcount_dict[int(row['original_tweet_id'])] = int(row['original_tweet_retweet_count'])

    for status in status_rettime_dict:
        print(status_retcount_dict[status], len(status_rettime_dict[status]))

    with open(d + 'status_retweettime_dict.json', 'w') as outfile:
        json.dump(status_rettime_dict, outfile)

    with open(d + 'status_retweetcount_dict.json', 'w') as outfile:
        json.dump(status_retcount_dict, outfile)


def countSources():
    global d, conn1
    # try:

    with conn1.cursor() as cursor:
        # Read a single record
        # sql = "SELECT `retweeter_id`, `main_user_id` FROM `retweeters` WHERE `id`<12000"
        sql = "SELECT text FROM `statuses`"
        # ('webmaster@python.org',)
        cursor.execute(sql)
        result = cursor.fetchall()
        status_user_dict = defaultdict(int)
        textlist = []
        for r in result:
            # if random.random()<0.1:
            # if random.random()< 0.01:
            print(r['text'][:20])
            textlist.append(r['text'][:30])
            # if r['user_id'].isdigit():
            # status_user_dict[r['id']] = int(r['user_id'])

        # print("average retweeter length:", len(status_user_dict))
        with open(d + 'textlist.json', 'w') as outfile:
            json.dump(textlist, outfile)

    # finally:
    #     conn1.close()

    with open(d + 'textlist.json', 'r') as outfile:
        textlist = json.load(outfile)

    print("length of json:", len(textlist))
    print("length of retweets:", len([e for e in textlist if 'RT ' in e]))
    print("length of replies:", len([e for e in textlist if 'Reply' in e]))

    return
    sourcenames = []
    for text in textlist:
        res = re.search(r'RT (@[\d ^ \w]+)\b.+', text)
        if res:
            sn = res.group(1)
            sourcenames.append(sn[1:])

    countersources = Counter(sourcenames)
    sortedtuples = sorted(countersources.items(), key = lambda x: x[1], reverse= True)

    print(list(sortedtuples)[:200])
    mostcounted = [e[0] for e in sortedtuples]

    all_list = []
    with open(d + 'All_sources.txt', 'r') as infile:
        for line in infile:
            all_list.append(line.strip())

    all_list = [e[1:] for e in all_list if len(e)>1]
    all_list_1000 = list(set(all_list))
    all_list_2000 = list(set(all_list))
    # print(all_list)

    print(len(mostcounted))
    for newname in mostcounted:
        if len(newname)<=1:
            continue

        if len(all_list_2000)>2000:
            break

        if newname not in all_list and len(all_list_1000)<1000:
            all_list_1000.append(newname)

        if newname not in all_list and len(all_list_2000)<2000:
            all_list_2000.append(newname)


    print(len(all_list_1000),len(all_list_2000))

    all_list_1000 = list(set(all_list_1000))
    all_list_2000 = list(set(all_list_2000))

    with open(d + 'allaccounts_1000.json', 'w') as outfile:
        json.dump(all_list_1000, outfile)

    with open(d + 'allaccounts_2000.json', 'w') as outfile:
        json.dump(all_list_2000, outfile)

    print(len(all_list))



def tokenizeText(text):
    # print(text)
    # text = re.sub(r'(.*)https?:\/\/.*[\r\n]*.*', r'\1', text, flags=re.MULTILINE)
    text = re.sub(r'https?:\/\/[^\s]+', r'', text, flags=re.MULTILINE)
    text = re.sub(r'rt @[^\s]*', r'', text, flags=re.MULTILINE)
    text = re.sub(r'@[^\s]*', r'', text, flags=re.MULTILINE)
    # text = text.replace("'", '')
    stopset = set(stopwords.words('english'))
    tokens = word_tokenize(str(text))
    # print(tokens)
    tokens = [x.strip('\' \" ` () * & ^ % $ < . ; , - ! ? : “ ’ ” …').replace("'", '') for x in tokens]
    # print(tokens)
    tokens = [stemmer.stem(e) for e in tokens if e]
    # if tokens and tokens[0] == '@':
    #     tokens = tokens[1:]
    #     tokens[0] = "@"+tokens[0]
    tokens = [w for w in tokens if not w in stopset]
    tokenstr =  ' '.join(tokens)
    tokenstr = tokenstr.replace('@ ', '@').replace('# ', '#')
    return tokenstr

def createStatusTokenTable():
    global conn1, db, conn2


    sql_drop = "DROP TABLE IF EXISTS statustokens"

    # conn1.execute(sql)
    sql_cr = "CREATE TABLE %s (%s, %s, %s, %s, %s, %s)"%('statustokens',
                                                     'created_at datetime',
                                                     'id bigint(20)',
                                                     'user_id bigint(20)',
                                                     'tokens varchar(500)',
                                                        'textlength int',
                                                     'PRIMARY KEY (id)',
                                                        )
    sql_read = "Select created_at, id, user_id, text from statuses where created_at> now()- interval %d day"%interval
    sql_read_ids = "select id from statustokens"
    # value = [61245423, 5123412343124, 'this that']
    # sql_insert = "INSERT INTO statustokens values (%(id)d, %(user_id)d, '%(tokens)s');"

    add_token = ("INSERT INTO statustokens "
                    "(created_at, id, user_id, tokens, textlength)"
                    "VALUES (%s, %s, %s, %s, %s)"
                 )

    row_exists = "SELECT COUNT(*) FROM statustokens WHERE id = %s"
    # add_value = {'db_name': 'statustokens', 'id': 61245423, 'user_id': 5123412343124, 'tokens': 'this that'}
    # add_value = (str(61245423), str(5123412343124), 'this that')

    # print(sql_cr)
    # print(add_token, add_value)

    with conn1.cursor() as cursor1, conn2.cursor() as cursor2:
        # cursor2.execute(sql_drop)
        # cursor2.execute(sql_cr)
        # conn2.commit()
        cursor2.execute(sql_read_ids)
        result = cursor2.fetchall()
        ids = set([])
        for r in result:
            ids.add(r['id'])

        cursor1.execute(sql_read)
        result = cursor1.fetchall()
        for i, r in enumerate(result):
        # for i, r in islice(enumerate(result), 1000):
            # if text.startswith('RT @'):
            #     continue
            # print(text)
            # cursor.execute(sql)
            # cursor.execute(sql_cr)
            # drop a row
            # sql_delrow = "DELETE FROM statustokens WHERE id = %s" %str(r['id'])
            # cursor2.execute(sql_delrow)
            # # print(sql_rowexists)
            if r['id'] in ids:
                print("Already exists: ", i, r['id'])
                continue

            text = r['text'].lower()
            add_row = (r['created_at'], r['id'], r['user_id'], tokenizeText(text), len(text))
            if add_row[3]:
                cursor2.execute(add_token, add_row)
                # print(i, add_row)
                # conn2.commit()
            #
            if i %100 == 0:
                conn2.commit()
                # print(text)
                print(i, add_row)


        conn2.commit()
        stemmer.saveJson()
        # conn1.close()


def createUserActivityTable():
    global conn1, conn2, interval

    sql_drop = "DROP TABLE IF EXISTS useractivity2000"

    # conn1.execute(sql)
    sql_createuser = "CREATE TABLE %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"%('useractivity2000',
                                                         'user_id bigint(20)',
                                                        'screen_name varchar(50)',
                                                        'followers_count int',
                                                        'followees_count int',
                                                     'firsttweettime datetime',
                                                     'statuses_count int',
                                                             'retweets_count int',
                                                             'replies_count int',
                                                                         'links_count int',
                                                                             'text_median_length int',
                                                                             'url boolean',
                                                                                     'PRIMARY KEY (user_id)'
                                                        )
    sql_read_statuses = "Select created_at, id, user_id, text from statuses where created_at> now()- interval %d day"%(interval)
    sql_read_users = "Select screen_name, id, followers_count, friends_count, statuses_count, favourites_count, url from users"

    # value = [61245423, 5123412343124, 'this that']
    # sql_insert = "INSERT INTO statustokens values (%(id)d, %(user_id)d, '%(tokens)s');"

    add_user_query = ("INSERT INTO useractivity2000"
                    "(user_id, screen_name, followers_count, followees_count, firsttweettime, statuses_count, retweets_count, replies_count, links_count, text_median_length, url)"
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
                 )
    # add_value = {'db_name': 'statustokens', 'id': 61245423, 'user_id': 5123412343124, 'tokens': 'this that'}
    # add_value = (str(61245423), str(5123412343124), 'this that')

    # print(sql_cr)
    # print(add_token, add_value)
    userid_retcount_dict = defaultdict(int)
    userid_repcount_dict = defaultdict(int)
    userid_lengthlist_dict = defaultdict(list)
    userid_fisttweettime_dict = {}
    userid_linkcount_dict = defaultdict(int)
    userid_followerscount_dict = defaultdict(int)
    userid_followeescount_dict = defaultdict(int)
    userid_screenname_dict = defaultdict(str)
    userid_statusescount_dict = defaultdict(int)
    userid_url_dict = defaultdict(bool)
    userid_favoritecount_dict = defaultdict(int)

    with conn1.cursor() as cursor1:
        conn2.commit()
        cursor1.execute(sql_read_statuses)
        result1 = cursor1.fetchall()
        for i, r in enumerate(result1):
        # for i, r in islice(enumerate(result1), 2000):
            text = r['text']
            userid_statusescount_dict[r['user_id']] += 1
            if r['user_id'] not in userid_fisttweettime_dict:
                userid_fisttweettime_dict[r['user_id']] = r['created_at']
            elif r['created_at']<userid_fisttweettime_dict[r['user_id']]:
                userid_fisttweettime_dict[r['user_id']] = r['created_at']

            if re.search(r'(.*)https?:\/\/.*[\r\n]*', text):
                userid_linkcount_dict[r['user_id']] += 1

            if re.search(r'RT (@[\d ^ \w]+)\b.+', text):
                userid_retcount_dict[r['user_id']] += 1
            else:
                text_nolink = re.sub(r'(.*)https?:\/\/.*[\r\n]*', r'\1', text, flags=re.MULTILINE)
                userid_lengthlist_dict[r['user_id']].append(len(text_nolink))

            if re.search(r'@[\d ^ \w]+\b.+', text):
                userid_repcount_dict[r['user_id']] += 1

            print('read status: ', i, r['user_id'])#, userid_fisttweettime_dict[r['user_id']], userid_retcount_dict[r['user_id']], userid_repcount_dict[r['user_id']], len(userid_lengthlist_dict[r['user_id']]))
            # if text.startswith('RT @'):
            #     continue
            # print(text)
        print("length of userid first tweet vs length list:", len(userid_fisttweettime_dict), len(userid_lengthlist_dict))
        userid_medianlength_dict = {userid: statistics.median(l) if l else 0 for userid, l in userid_lengthlist_dict.items()}
        userid_medianlength_dict = defaultdict(int, userid_medianlength_dict.items())

    with conn1.cursor() as cursor2, conn2.cursor() as cursor3:
        cursor3.execute(sql_drop)
        cursor3.execute(sql_createuser)
        cursor2.execute(sql_read_users)
        result2 = cursor2.fetchall()
        for j, r2 in enumerate(result2):
            userid_screenname_dict[r2['id']] = r2['screen_name']
            userid_followerscount_dict[r2['id']] = r2['followers_count']
            userid_followeescount_dict[r2['id']] = r2['friends_count']
            # userid_statusescount_dict[r2['id']] = r2['statuses_count']
            # userid_favoritecount_dict[r2['id']] = r2['favourites_count']
            if r2['url']:
                userid_url_dict[r2['id']] = True

        for uid in userid_fisttweettime_dict:
            add_row = (uid,
                       userid_screenname_dict[uid], userid_followerscount_dict[uid],
                       userid_followeescount_dict[uid], userid_fisttweettime_dict[uid],
                       userid_statusescount_dict[uid], userid_retcount_dict[uid],
                       userid_repcount_dict[uid], userid_linkcount_dict[uid],
                       userid_medianlength_dict[uid],
                       userid_url_dict[uid])
            # cursor.execute(sql)
            # cursor.execute(sql_cr)
            # drop a row
            # sql_delrow = "DELETE FROM statustokens WHERE id = %s" %str(r['id'])
            # cursor2.execute(sql_delrow)
            cursor3.execute(add_user_query, add_row)
                # print(i, add_row)
            conn2.commit()
            print(add_row)


def saveUSERCSV():
    global conn2, d
    import csv
    c = conn2.cursor()
    def execute(c, command):
        c.execute(command)
        return c.fetchall()

    cols = []
    for item in execute(c, "show columns from useractivity2000"):
        print(item)
        cols.append(item['Field'])

    toCSV = []
    sql_read_user = "SELECT * from useractivity2000"
    data = execute(c, sql_read_user)
    for row in data:
        print(type(row))
        toCSV.append(row)

    keys = list(toCSV[0].keys())
    print(keys)
    with open(d + 'useractivity2000.csv', 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, keys)
        dict_writer.writeheader()
        dict_writer.writerows(toCSV)

# conn1.cursor().execute("DROP database IF EXISTS ehsan_new_updated")
# saveStatusUserJSON()
# saveStatusTimeJson()
# saveStatusRetTimeJSON()
# countSources()

# example = "@EsotericCD Though I'm still a snob who can't really handle regional accents. It's what happens when you partially grew up in Surrey."
# print(example)
# print(tokenizeText(example))#'RT @OurFamilyWorld: Leasing the car of your dreams is literally as easy as 1-2-3 with @HonckerCars! Check out these top 5 reasons why! #ad)
# createStatusTokenTable()
# createUserActivityTable()

# saveUSERCSV()

conn1.close()
conn2.close()
