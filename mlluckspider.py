import sqlite3
import urllib.error
import ssl
from urllib.parse import urljoin
from urllib.parse import urlparse
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

# cur.execute('''CREATE TABLE IF NOT EXISTS Links
#     (from_id INTEGER, to_id INTEGER, UNIQUE(from_id, to_id))''')

cur.execute('''CREATE TABLE IF NOT EXISTS Webs (url TEXT UNIQUE)''')

# Check to see if we are already in progress...
cur.execute('SELECT id,url FROM Pages WHERE html is NULL and error is NULL ORDER BY RANDOM() LIMIT 1')
row = cur.fetchone()
if row is not None:
    print("Restarting existing crawl.  Remove spider.sqlite to start a fresh crawl.")
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


# Find url's of the main catalog
menu_fst_categ = soup.find_all('li', {'class': 'sub_category onhoverclass'})
count = 0
for i in menu_fst_categ:
    item = i.find('a').get('href')
    if item[0:2] == '//':
        item = 'https:' + item
    count += 1
    print('Main catalog', item)
print('COUNT:', count)

# Find second categories of the catalog
menu_sec_categ = soup.find_all('li', {'class': 'sub_category active_menu_second'})
count = 0
for i in menu_sec_categ:
    item = i.find('a').get('href')
    if item[0:2] == '//':
        item = 'https:' + item
    count += 1
    print('Second category', item)
print('COUNT:', count)


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
        count += 1
        print(item, title)
print('COUNT:', count)

cur.close()



