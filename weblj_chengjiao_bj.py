#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2016 Hongkun Yu <staryhk@gmail.com>
#
# @AUTHOR:      Hongkun Yu
# @MAIL:        staryhk@gmail.com
# @VERSION:     2016-11-01
#

import os
import re
import urllib2  
#import urllib  
import socket
import sqlite3
import random
import threading
from bs4 import BeautifulSoup
import json
import time
#import socket

import sys
reload(sys)
sys.setdefaultencoding("utf-8")


#Some User Agents
hds=[{'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/45.0.2454.101 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.2) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.12 Safari/535.11'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; Trident/6.0)'},\
    {'User-Agent':'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:34.0) Gecko/20100101 Firefox/34.0'},\
    {'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/44.0.2403.89 Chrome/44.0.2403.89 Safari/537.36'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50'},\
    {'User-Agent':'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1'},\
    {'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11'},\
    {'User-Agent':'Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11'},\
    {'User-Agent':'Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11'}\
    ]

#proxys=[]

storename = 'chengjiao_beijing_' + time.strftime("%Y_%m_%d_%H_%M_%S", time.localtime())

class SQLiteWraper(object):
    
    def __init__(self,path,*args,**kwargs):  
        self.lock = threading.RLock()   
        self.path = path 
        
        self.conn = sqlite3.connect(self.path)
        self.conn.text_factory = str
        self.cu = self.conn.cursor()
      
    def close(self):  
        self.conn.close()  

    def execute(self,command):  
        cu = self.cu
        self.lock.acquire()
        try:
            if isinstance(command, (str, unicode)):
                cu.execute(command)
            elif len(command) == 2:
                cu.execute(command[0],command[1])
            else:
                raise ValueError("Invalid command")
            self.conn.commit()
        finally:
            self.lock.release()
    
#def getProxyIp():
#    
#    proxys=[]
#    while len(proxys) == 0:
#        url = 'http://www.xicidaili.com/nn/'
#        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
#        res = urllib2.urlopen(req).read()
#        soup = BeautifulSoup(res)
#        ips = soup.findAll('tr')
#    
#        url = "http://ip.chinaz.com/getip.aspx"  #用来测试IP是否可用的url  
#        socket.setdefaulttimeout(1)
#        for x in range(1,len(ips)):
#            try:  
#                ip = ips[x]
#                tds = ip.findAll("td")
#                proxy_host = "http://"+tds[1].contents[0]+":"+tds[2].contents[0]
#                proxy_temp = {"http":proxy_host}
#                res = urllib.urlopen(url,proxies=proxy_temp).read()
#                print proxy_temp
#                proxys.append(proxy_temp)
#            except Exception, e:  
#                print e
#                continue
#        time.sleep(10)
#
#    print 'found %d proxies' % len(proxys)  

def gen_chengjiao_insert_command(info_dict):
    
    info_list = [u'href',
                u'area',
                u'buildyear',
                u'dealdate',
                u'decoration',
                u'elevator',
                u'floor',
                u'name',
                u'orientation',
                u'fangben',
                u'source',
                u'tag',
                u'title',
                u'totalprice',
                u'unitprice',
                u'xiaoqu',
                u'guapai',
                u'cjcycle'
                ]

    t = []
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t = tuple(t)
    command = (r"insert into chengjiao values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command


def chengjiao_page_search(db_cj, url):
    trytimes = 0
#    tryblocktimes = 0
    while 1:
        try:
#            proxy_s = urllib2.ProxyHandler(proxys[random.randint(0, len(proxys)-1)])
#            opener = urllib2.build_opener(proxy_s)
#            urllib2.install_opener(opener) 
            req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
            source_code = urllib2.urlopen(req,timeout=10).read()
            plain_text=unicode(source_code)#,errors='ignore')   
            soup = BeautifulSoup(plain_text)
        except socket.timeout as e:
            if trytimes < 5:
                time.sleep(3)
                trytimes += 1
                continue
            else:
                print e
                exception_write(e, 'chengjiao_page_search', url)
                return 
        except (urllib2.HTTPError, urllib2.URLError) as e:
            print e
            exception_write(e, 'chengjiao_page_search', url)
            return 
        except Exception as e:
            print e
            exception_write(e, 'chengjiao_page_search', url)
            return
        
        human = soup.find('div',{'class':'human'})
        
        if not human:
            break
        else:
            print "block"
            time.sleep(random.randint(900, 1200))
            trytimes = 0
#            if tryblocktimes < 5:
#                tryblocktimes += 1
#                continue
#            else:
#                print "block"
#                getProxyIp();
#                trytimes = 0
#                tryblocktimes = 0

    thispagelist = soup.find('ul',{'class':'listContent'}).findAll('li')
   
    j = 0
    for j in range(len(thispagelist)):
        cjitem = thispagelist[j]
        print j
        try:
            info_dict = {}
            
            title = cjitem.find('div',{'class':'title'})
            if not title.a:
                href = ""
                tmpl = list(title.children)
                longname = tmpl[0]
            else:
                href = unicode(title.a['href'])
                longname = title.text
            nameinfos = longname.split(' ')
                
            info_dict[u'href'] = href
            info_dict[u'name'] = longname
            if len(nameinfos) == 3:
                info_dict[u'xiaoqu'] = nameinfos[0]
                info_dict[u'title'] = nameinfos[1]
                info_dict[u'area'] = nameinfos[2]
            else:
                print "Unhealth record!"
                info_dict[u'xiaoqu'] = ""
                info_dict[u'title'] = ""
                info_dict[u'area'] = ""
    
            houseinfo = cjitem.find('div',{'class':'houseInfo'})
            hi = houseinfo.text.replace(' ', '').split('|')
            dealdate = cjitem.find('div',{'class':'dealDate'})
            totalprice = cjitem.find('div',{'class':'totalPrice'})
           
            if len(hi) == 3:
                info_dict[u'orientation'] = hi[0]
                info_dict[u'decoration'] = hi[1]
                info_dict[u'elevator'] = hi[2]
            else:
                if len(hi) == 2:
                    print "Unhealth record!"
                    info_dict[u'orientation'] = hi[0] 
                    info_dict[u'decoration'] = hi[1]
                    info_dict[u'elevator'] = ""
                else:
                    print "Unhealth record!"
                    info_dict[u'orientation'] = houseinfo.text
                    info_dict[u'decoration'] = ""
                    info_dict[u'elevator'] = ""
            info_dict[u'dealdate'] = dealdate.text
            info_dict[u'totalprice'] = totalprice.text
            
            positioninfo = cjitem.find('div',{'class':'positionInfo'})
            pi = positioninfo.text.split(' ')
            source = cjitem.find('div', {'class': 'source'})
            unitprice = cjitem.find('div',{'class':'unitPrice'})
            
            if len(pi) == 2:
                info_dict[u'floor'] = pi[0]
                info_dict[u'buildyear'] = pi[1]
            else:
                print "Unhealth record!"
                info_dict[u'floor'] = positioninfo.text
                info_dict[u'buildyear'] = "" 
            info_dict[u'source'] = source.text 
            info_dict[u'unitprice'] = unitprice.text
   

            dealhouseinfo = cjitem.find('div', {'class':'dealHouseInfo'}).find('span', {'class': 'dealHouseTxt'})
            if not dealhouseinfo:
                dealhousetxts = {}
                dealhouseplaintxt = ""
            else:
                dealhousetxts = dealhouseinfo.findAll('span')
                dealhouseplaintxt = dealhouseinfo.text

            if len(dealhousetxts) == 2:
                info_dict[u'fangben'] = dealhousetxts[0].text
                info_dict[u'tag'] = dealhousetxts[1].text
            else:
                info_dict[u'fangben'] = ""
                info_dict[u'tag'] = dealhouseplaintxt
            
            dealCycleitem = cjitem.find('div', {'class':'dealCycleeInfo'})
            if not dealCycleitem:
                dealCycletxts = {}
                dealCycleplaintxt = ""
            else:
                dealCycleeinfo = dealCycleitem.find('span', {'class': 'dealCycleTxt'})
                if not dealCycleeinfo:
                    dealCycletxts = {}
                    dealCycleplaintxt = ""
                else:
                    dealCycletxts = dealCycleeinfo.findAll('span')
                    dealCycleplaintxt = dealCycleeinfo.text

            if len(dealCycletxts) == 2:
                info_dict[u'guapai'] = dealCycletxts[0].text
                info_dict[u'cjcycle'] = dealCycletxts[1].text
            else:
                info_dict[u'cjcycle'] = ""
                info_dict[u'guapai'] = dealCycleplaintxt

        except Exception as e:
            print e
            exception_write(e, 'chengjiao_item_page_list', str(j))
            continue
##############################################3
        #try:
        #    moreinfo = chengjiao_item_page(info_dict[u'href'])
        #except Exception as e:
        #    print e
        #    exception_write(e, 'chengjiao_item_page', info_dict[u'href'])
        #    continue
        
        #info_dict.update(moreinfo)
################################################
        try:
            command = gen_chengjiao_insert_command(info_dict)
            db_cj.execute(command)
        except Exception as e:
            print e
            exception_write(e, 'chengjiao_item_page_db', str(j))
            continue


def chengjiao_item_page(url):

    info_dict = {}

    while 1:
        
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text)
        
        human = soup.find('div',{'class':'human'})
        
        if not human:
            break
        else:
            print "block && wait"
            time.sleep(600)

    info_fr = soup.find('div', {'class': 'info fr'})

    msg = info_fr.find('div', {'class':'msg'})

    sp1 = list(msg.find('span', {'class':'sp01'}).children)
    sp2 = list(msg.find('span', {'class':'sp02'}).children)
    sp3 = list(msg.find('span', {'class':'sp03'}).children)

    info_dict[u'title'] = sp1[0].text
    info_dict[u'floor'] = sp1[1]
    info_dict[u'orientation'] = sp2[0].text
    info_dict[u'nnnn'] = sp2[1]
    info_dict[u'area'] = sp3[0].text
    info_dict[u'buildyear'] = sp3[1]

    tag = info_fr.find('div', {'class':'tag'})
    info_dict[u'tag'] =  tag.text

    nowp = info_fr.findAll('p')
    site = list(nowp[0].children)
    onsale = nowp[1]

    info_dict[u'xiaoqu'] = site[0].text
    info_dict[u'region'] = site[2].text
    info_dict[u'region2'] = site[3].text
    info_dict[u'onsale'] = onsale.text

    return info_dict


def xiaoqu_chengjiao_spider(db_cj,xq_name=u"京师园"):
    
    trytimes = 0
#    tryblocktimes = 0
    url=u"http://bj.lianjia.com/chengjiao/rs"+urllib2.quote(xq_name)+"/"
    while 1:
        try:
#            proxy_s = urllib2.ProxyHandler(proxys[random.randint(0, len(proxys)-1)])
#            opener = urllib2.build_opener(proxy_s)
#            urllib2.install_opener(opener) 
            req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
            source_code = urllib2.urlopen(req,timeout=10).read()
            plain_text=unicode(source_code)#,errors='ignore')   
            soup = BeautifulSoup(plain_text)
        except socket.timeout as e:
            if trytimes < 5:
                #time.sleep(5)
                trytimes += 1
                continue
            else:
                print e
                exception_write(e, 'xiaoqu_chengjiao_spider', xq_name)
                return
        
        except (urllib2.HTTPError, urllib2.URLError) as e:
            print e
            exception_write(e, 'xiaoqu_chengjiao_spider',xq_name)
            return 
        except Exception as e:
            print e
            exception_write(e, 'xiaoqu_chengjiao_spider',xq_name)
            return
        
        human = soup.find('div',{'class':'human'})
        
        if not human:
            break
        else:
            print "block"
            time.sleep(random.randint(900, 1200))
            trytimes = 0
#            if tryblocktimes < 5:
#                tryblocktimes += 1
#                continue
#            else:
#                print "block"
#                getProxyIp();
#                trytimes = 0
#                tryblocktimes = 0

    pagebox = soup.find('div',{'class':'page-box house-lst-page-box'})
    if not pagebox:
        print "no chengjiao record"
        return
    
    l = pagebox['page-data'].find(':')
    r = pagebox['page-data'].find(',')
    pagenum = int(pagebox['page-data'][l+1:r])

    print u"开始爬 %s 区全部的信息" % xq_name
    print u"total number of pages is " + str(pagenum)

    for j in range(pagenum):
        url_page = u"http://bj.lianjia.com/chengjiao/pg%drs%s/" % (j + 1, xq_name)
        chengjiao_page_search(db_cj, url_page)    
        
        #time.sleep(random.randint(1,2))
        print xq_name + "  " + str(j) + "th page have been done"
        
    
    print u"爬下了 %s 区全部的信息" % xq_name



  

def exception_write(e, fun_name,url):
    f = open(storename + '_log.txt','a')
    line = "%s\t%s\t%s\n" % (e, fun_name, url)
    f.write(line)
    f.close()

def excepthandle(db_cj):
    xcs = [
'三间房东路',
'三间房南里4号院',
'三间房南里5号院',
'三间房南里7号院',
'上东三角洲',
'上东上层',
'上地南路6号院',
'上地南路8号院',
'上地国际创业园',
'上地西里',
'上坡佳园',
'上奥世纪中心',
'上庄家园东区',
'上庄家园西区',
'上林世家',
'上林苑',
'泰福苑D2区',
'老虎庙'
]

    cps = [
u'http://bj.lianjia.com/chengjiao/pg1rs三里河路5号院/',
u'http://bj.lianjia.com/chengjiao/pg2rs三里河路5号院/',
u'http://bj.lianjia.com/chengjiao/pg1rs上林溪/',
u'http://bj.lianjia.com/chengjiao/pg2rs上林溪/',
u'http://bj.lianjia.com/chengjiao/pg3rs上林溪/',
u'http://bj.lianjia.com/chengjiao/pg4rs上林溪/',
u'http://bj.lianjia.com/chengjiao/pg1rs上海建筑/',
u'http://bj.lianjia.com/chengjiao/pg2rs上海建筑/',
u'http://bj.lianjia.com/chengjiao/pg3rs上海建筑/',
u'http://bj.lianjia.com/chengjiao/pg4rs上海建筑/',
u'http://bj.lianjia.com/chengjiao/pg5rs上海建筑/'
]

    for xq in xcs:
        xiaoqu_chengjiao_spider(db_cj, xq)

    for cp in cps:
        chengjiao_page_search(db_cj, cp)




if __name__=="__main__":
    
    db_cj=SQLiteWraper(storename + '.db')
    #db_cj=SQLiteWraper('chengjiao_2016_11_06_23:46:14.db')

    create_command = """create table if not exists chengjiao 
                (href TEXT, 
                area TEXT,
                buildyear TEXT,
                dealdate TEXT,
                decoration TEXT,
                elevator TEXT,
                floor TEXT,
                name TEXT,
                orientation TEXT,
                fangben TEXT,
                source TEXT,
                tag TEXT,
                title TEXT,
                totalprice TEXT,
                unitprice TEXT,
                xiaoqu TEXT,
                guapai TEXT,
                cjcycle TEXT)"""


    db_cj.execute(create_command)
   
    xq_list=[]
    xq = open("xiaoqu_2016_11_06_23_01_24_xiaoqu_district_list.txt.bak", "r")
    #xq = open("xq_trade_list_11_1_from_dong.txt", "r")
    for line in xq:
        xq_list.append(line.strip('\r\n')) 
    xq.close()
    total = len(xq_list)
    
    #print xq_list[0]
    print u"total number of xiaoqu is %d" % total

    #excepthandle(db_cj)
    #print 'Exception handle done'

    #getProxyIp()

    for xq in xq_list:
        print u'begin spidering xiaoqu %s' % xq
        xiaoqu_chengjiao_spider(db_cj, xq)
        #time.sleep(random.randint(5,10))

    db_cj.close()

    print u'all done'

    
