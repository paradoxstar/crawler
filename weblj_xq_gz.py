##! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2016 Hongkun Yu <staryhk@gmail.com>
#
# @AUTHOR:      Hongkun Yu
# @MAIL:        staryhk@gmail.com
# @VERSION:     2016-8-25
#
import os
import re
import socket
import urllib2  
import sqlite3
import socket
import random
import threading
from bs4 import BeautifulSoup
import json
import time

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


storename = 'xiaoqu_guangzhou'# + time.strftime("%Y_%m_%d_%X", time.localtime())


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
    


def gen_xiaoqu_insert_command(info_dict):
    
    info_list = [u'href',
                u'name',
                u'chengjiao',
                u'chuzu',
                u'district',
                u'bizcircle',
                u'style',
                u'buildyear',
                u'tag',
                u'totalprice',
                u'sellcount'
                ]
    
    t = []
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t = tuple(t)
    command = (r"replace into xiaoqu values(?,?,?,?,?,?,?,?,?,?,?)",t)
    return command


def xiaoqu_page_search(db_xq,url_page=u"https://gz.lianjia.com/xiaoqu/tianhe/pg1/"):

    trytimes = 0
    while 1:
        try:
            req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
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
                exception_write(e, 'xiaoqu_page_search', url_page)
                return 
        except (urllib2.HTTPError, urllib2.URLError) as e:
            print e
            exception_write(e, 'xiaoqu_page_search', url_page)
            return 
        except Exception as e:
            print e
            exception_write(e, 'xiaoqu_page_search', url_page)
            return
        
        human = soup.find('div',{'class':'human'})
        
        if not human:
            break
        else:
            print "block && wait"
            time.sleep(600)
            trytimes = 0
    
    xiaoqu_list = soup.findAll('li',{'class':'clear xiaoquListItem'})
    
    j = 0
    for j in range(len(xiaoqu_list)):
        xq = xiaoqu_list[j]
        try:
            info_dict = {}

            title = xq.find('div',{'class':'title'})
            href = unicode(title.a['href'])
            longname = title.text
                
            info_dict[u'href'] = href
            info_dict[u'name'] = longname.strip()
            
            houseinfo = xq.find('div',{'class':'houseInfo'})
            hi = houseinfo.text.replace(' ', '').strip().split('|')
           
            if len(hi) == 2:
                info_dict[u'chengjiao'] = hi[0].strip()
                info_dict[u'chuzu'] = hi[1].strip()
            elif len(hi) == 3:
                print "more than chengjiao && chuzu"
                info_dict[u'chengjiao'] = hi[1].strip()
                info_dict[u'chuzu'] = hi[2].strip()
            else:
                print "miss chengjiao && chuzu!"
                info_dict[u'chengjiao'] = houseinfo.text.replace(' ', '').strip()
                info_dict[u'chuzu'] = ""

            positioninfo = xq.find('div',{'class':'positionInfo'})
            district = positioninfo.find('a', {'class': 'district'})
            bizcircle = positioninfo.find('a', {'class': 'bizcircle'})
            posinfos = list(positioninfo.children)
            morinfo = posinfos[-1].strip().strip('/').split('\n')
            
            info_dict[u'district'] = district.text.strip()
            info_dict[u'bizcircle'] = bizcircle.text.strip()
            if len(morinfo) == 2:
                info_dict[u'style'] = morinfo[0].strip().strip('/').strip()
                info_dict[u'buildyear'] = (morinfo[1].strip().strip('/').strip())[:4]
            else:
                #print "miss style && build year!"
                info_dict[u'style'] = ""
                info_dict[u'buildyear'] =morinfo[0].strip()

            tag = xq.find('div', {'class': 'tagList'})
            info_dict[u'tag'] = tag.text.strip()

            totalprice = xq.find('div', {'class': 'totalPrice'})
            sellcount = xq.find('a', {'class': 'totalSellCount'})
            info_dict[u'totalprice'] = totalprice.text[:-4]
            info_dict[u'sellcount'] = sellcount.text[:-1]

        except Exception as e:
            print e
            exception_write(e, 'xiaoqu_page_search', str(j))
            continue
        
        print info_dict[u'name'] 
        try:
            command = gen_xiaoqu_insert_command(info_dict)
            db_xq.execute(command)
        except Exception as e:
            print e
            exception_write(e, 'xiaoqu_page_search_db', str(j))
            continue


    
def area_xiaoqu_search(db_xq,region=u"tianhe"):
   
    trytimes = 0
    url=u"https://gz.lianjia.com/xiaoqu/"+region+"/"
    while 1:
        try:
            req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
            source_code = urllib2.urlopen(req,timeout=5).read()
            plain_text=unicode(source_code)#,errors='ignore')   
            soup = BeautifulSoup(plain_text)
        except socket.timeout as e:
            if trytimes < 5:
                time.sleep(2)
                trytimes += 1
                continue
            else:
                print e
                exception_write(e, 'area_xiaoqu_search', region)
                return

        except (urllib2.HTTPError, urllib2.URLError), e:
            print e
            exception_write(e, 'area_xiaoqu_search', region)
            return
        except Exception,e:
            print e
            exception_write(e, 'area_xiaoqu_search', region)
            return
        
        human = soup.find('div',{'class':'human'})
        
        if not human:
            break
        else:
            print "block! && wait"
            time.sleep(600)
            trytimes = 0

    xqtotal_num = int(soup.find('h2',{'class':'total fl'}).span.text.strip())

    print u"开始爬 %s 区全部的小区信息" % region
    print "total number of xiaoqu is " + str(xqtotal_num)

    d = "d=" + soup.find('div',{'class':'page-box house-lst-page-box'}).get('page-data')
    exec(d)
    total_pages = d['totalPage']
    
    print u"total number of pages is " + str(total_pages)
    
    for i in range(total_pages):
        url_page = u"https://gz.lianjia.com/xiaoqu/%s/pg%d/" % (region, i + 1)
        xiaoqu_page_search(db_xq, url_page)
        
        print region + "  " + str(i) + "th page have been done"

    print u"爬下了 %s 区全部的小区信息" % region



def exception_write(e, fun_name,url):
    f = open(storename + '_log.txt','a')
    line = "%s\t%s\t%s\n" % (e, fun_name, url)
    f.write(line)
    f.close()
    

if __name__=="__main__":
    #广州区域列表
    regions=[u"tianhe",
            u"yuexiu",
            u"liwan",
            u"haizhu",
            u"panyu",
            u"baiyun",
            u"huangpugz",
            u"conghua",
            u"zengcheng",
            u"huadou",
            u"luogang",
            u"nansha"]

    db_xq=SQLiteWraper(storename + '.db')
    
    create_command = """create table if not exists xiaoqu 
                (href TEXT primary key UNIQUE, 
                name TEXT, 
                chengjiao TEXT, 
                chuzu TEXT, 
                district TEXT, 
                bizcircle TEXT, 
                style TEXT, 
                buildyear TEXT, 
                tag TEXT, 
                totalprice TEXT,
                sellcount TEXT)"""
    

    db_xq.execute(create_command)

    #spider xiaoqu info
    for region in regions:
        area_xiaoqu_search(db_xq,region)

    db_xq.close()

    print 'all done'
