import os, codecs
from urllib import request
from bs4 import BeautifulSoup as bs
import pandas as pd
link = 'https://www.theamas.com/winners-database/?winnerKeyword=&winnerYear='


d = {}
d['year'] = []
d['name'] = []
d['category'] = []
d['award_for'] = []

for year in range(1974, 2019):
    header = {}
    header['User-Agent'] = 'Mozilla/5.0 (Windows 10) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.166  Safari/535.19'
    req = request.Request(url = link+str(year), headers = header)
    response = request.urlopen(req)
    html = response.read().decode('utf8', 'ignore')
    # print (html)
    soup = bs(html, 'lxml')
    result_table = soup.find_all(id='resultsTable')[0]
    trs = result_table.find_all('tr')[1:]
    # print (trs[0])
    for tr in trs:
        cks = tr.find_all('td')
        # print (type(cks[0]))
        y = int(cks[0].text.strip())
        c = cks[1].text.strip()
        natuple = cks[2].find('p').text.strip()
        if ' “' in natuple :
            natuple = natuple.split(' “')
            name = natuple[0]
            award_for = natuple[1][:-1]
        elif '“' in natuple:
            award_for = natuple.replace('“', '').replace('”', '')
            name = 'null'
        else:
            name = natuple
            award_for = natuple
        d['year'].append(y)
        d['category'].append(c)
        d['name'].append(name)
        d['award_for'].append(award_for)
    # print (d)
    # break

df = pd.DataFrame.from_dict(d)
df.to_csv('..\\data\\extracted\\ama.csv')