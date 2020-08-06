import sqlite3
import ssl
from urllib.request import urlopen
from bs4 import BeautifulSoup
from datetime import datetime

DATE = str(datetime.now().strftime('%H:%M:%S %d-%m-%Y'))

# Ignore SSL certificate errors
ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.verify_mode = ssl.CERT_NONE

conn = sqlite3.connect('mlluck.sqlite')
cur = conn.cursor()

cur.execute('''CREATE TABLE IF NOT EXISTS Pages
    (id INTEGER PRIMARY KEY, url TEXT UNIQUE, html TEXT,
     error INTEGER, old_date REAL, new_date REAL)''')

cur.execute('''CREATE TABLE IF NOT EXISTS Links
    (from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))''')

cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')

# Check to see if we are already in progress...
cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = cur.fetchone()

if row is not None:
    print("Restarting existing crawl.  Remove mlluck.sqlite to start a fresh crawl.")
else:
    starturl = input('Enter web url or enter: ')
    if (len(starturl) < 1):
        starturl = 'https://www.mobilluck.com.ua/'
    if (starturl.endswith('/')):
        starturl = starturl[:-1]

    web = starturl

    if (starturl.endswith('.htm') or starturl.endswith('.html')):
        pos = starturl.rfind('/')
        web = starturl[:pos]

    if (len(web) > 1):
        cur.execute('INSERT OR IGNORE INTO Webs (url) VALUES ( ? )', (web,))
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_date) VALUES ( ?, NULL, ? )', (starturl, DATE))
        conn.commit()

try:
    document = urlopen(starturl, context=ctx)
    html = document.read()

    if document.getcode() != 200:
        print("Error on page: ", document.getcode())
    if 'text/html' != document.info().get_content_type():
        print("Ignore non text/html page")

    soup = BeautifulSoup(html, 'html.parser')
except KeyboardInterrupt:
    print('')
    print('Program interrupted by user...')



new_url = set()

# Find second categories of the catalog
menu_sec_categ = soup.find_all('li', {'class': 'sub_category active_menu_second'})
count = 0
for i in menu_sec_categ:
    item = i.find('a').get('href')
    if item[0:2] == '//':
        item = 'https:' + item
    new_url.add(item)
    cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_date) VALUES ( ?, NULL, ? )', (item, DATE))
    count += 1
conn.commit()
    # print('Second category', item)
# print('COUNT:', count)


# Find third category of the catalog
menu_third_categ = soup.find_all('div', {'class': 'menu_third_categories'})
count = 0
for i in menu_third_categ:
    items = i.find_all('li')

    for item in items:
        row = item
        item = item.a.get('href')
        title = row.find('span').contents[0]
        if item[0:2] == '//':
            item = 'https:' + item
        new_url.add(item)
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_date) VALUES ( ?, NULL, ? )', (item, DATE))
        count += 1
conn.commit()
        # print(item, title)
# print('COUNT:', count)


# count = 0
# for i in new_url:
#     cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_date) VALUES ( ?, NULL, ? )', (i, DATE))
#     if count % 100 == 0:
#         conn.commit()
#     count += 1
    # print('SET ITEM', i)
# print(new_url, len(new_url), sep='\n')


setcount = len(new_url)-504
print('setcount', setcount)
while True:
    if (setcount <= 1):
        break
    setcount -= 1
    print("==============While")

    cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
    try:
        row = cur.fetchone()
        print(row)
        fromid = row[0]
        url = row[1]
    except:
        print('No unretrieved HTML pages found')
        many = 0
        break

    # If we are retrieving this page, there should be no links from it
    # cur.execute('DELETE from Links WHERE from_id=?', (fromid,))

    try:
        document = urlopen(url, context=ctx)
        html = document.read()

        if document.getcode() != 200:
            print("Error on page: ", document.getcode())
        if 'text/html' != document.info().get_content_type():
            print("Ignore non text/html page")

        soup = BeautifulSoup(html, 'html.parser')
    except KeyboardInterrupt:
        print('')
        print('Program interrupted by user...')

    pagination = soup.find_all('a', {'class': 'a-text'})
    print("PAGE PAGINATION", pagination)
    if pagination != []:
        try:
            lnpgn = int(pagination[-2].get_text())
        except IndexError as err:
            print("Page without pagination, just part of tree. Error:", err)
            continue
        frst = soup.find('a', {'class': 'a-text'}).get('href')
        for i in range(1, lnpgn + 1):
            pagurl = 'http:' + frst + f'pages_{i}_15.html'
            cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_date) VALUES ( ?, NULL, ? )', (pagurl, DATE))
            conn.commit()

            cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', (pagurl,))
            try:
                row = cur.fetchone()
                toid = row[0]
            except:
                print('Could not retrieve id')
                continue
            # print fromid, toid
            cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', (fromid, toid))
            conn.commit()
            print(pagurl)

    # print(soup.prettify())
    items_url = soup.find_all('div', {'class': 'ccitem2'})
    count = 0
    for item in items_url:
        itemu = item.a.get('href')
        if itemu[0:2] == '//':
            itemu = 'https:' + itemu
        print(itemu)
        cur.execute('INSERT OR IGNORE INTO Pages (url, html, new_date) VALUES ( ?, NULL, ? )', (itemu, DATE))
        conn.commit()

        cur.execute('SELECT id FROM Pages WHERE url=? LIMIT 1', (itemu,))
        try:
            row = cur.fetchone()
            toid = row[0]
        except:
            print('Could not retrieve id')
            continue
        # print fromid, toid
        cur.execute('INSERT OR IGNORE INTO Links (from_id, to_id) VALUES ( ?, ? )', (fromid, toid))
        conn.commit()
        count += 1
        # print(fromid, toid)
    print("==============While end", setcount,'/', len(new_url))

conn.commit()




cur.close()













