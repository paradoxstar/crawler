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


storename = 'xiaoqu_shanghai'#+ time.strftime("%Y_%m_%d_%X", time.localtime())


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
                u'idx',
                u'idy',
                u'district',
                u'bizcircle',
                u'buildyear',
                u'chanquan',
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
    command = (r"replace into xiaoqu values(?,?,?,?,?,?,?,?,?,?)",t)
    return command


def xiaoqu_page_search(db_xq,url=u"http://sh.lianjia.com/xiaoqu/beicai/d1/"):

    trytimes = 0
    while 1:
        try:
            req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
            opener = urllib2.build_opener(urllib2.HTTPCookieProcessor)
            source_code = opener.open(req,timeout=5).read()
            plain_text=unicode(source_code)#,errors='ignore')
            soup = BeautifulSoup(plain_text)
        except socket.timeout as e:
            if trytimes < 5:
                time.sleep(3)
                trytimes += 1
                continue
            else:
                print e
                exception_write(e, 'xiaoqu_page_search', url)
                return 0
        except (urllib2.HTTPError, urllib2.URLError) as e:
            print e
            exception_write(e, 'xiaoqu_page_search', url)
            return 0
        except Exception as e:
            print e
            exception_write(e, 'xiaoqu_page_search', url)
            return 0
        
        human = soup.find('div',{'class':'human'})
        
        if not human:
            break
        else:
            print "block && wait"
            time.sleep(600)
            trytimes = 0
    
    xiaoqu_list = soup.find('div',{'class':'list-wrap'}).findAll('li')
    
    j = 0
    for j in range(len(xiaoqu_list)):
        xq = xiaoqu_list[j]
        try:
            info_dict = {}
            info_panel = xq.find('div',{'class':'info-panel'})

            h2 = info_panel.find('h2')
            longname = h2.a['title']
            href = h2.a['href']
            info_dict[u'href'] = href
            info_dict[u'name'] = longname.strip()
            
            col1 = info_panel.find('div',{'class':'col-1'})
            where = col1.find('div',{'class':'where'})
            other = col1.find('div',{'class':'other'})
            chanquan = col1.find('div',{'class':'chanquan'})
            
            whereinfo = where.a['xiaoqu'].strip('[]').split(',')
            idx = float(whereinfo[0])
            idy = float(whereinfo[1])
            info_dict[u'idx'] = idx
            info_dict[u'idy'] = idy

            otherinfo = other.find('div',{'class':'con'}).findAll('a')
            totalother = list(other.find('div',{'class':'con'}).children)
            if len(totalother) == 2:
                buildyear = ''
            else:
                buildyear = totalother[-1].strip()
            info_dict[u'district'] = otherinfo[0].text
            info_dict[u'bizcircle'] = otherinfo[1].text
            info_dict[u'buildyear'] = buildyear

            chanquaninfo = chanquan.text
            info_dict[u'chanquan'] = chanquaninfo.strip('\n\t ')

            col3 = info_panel.find('div',{'class':'col-3'})
            price = col3.find('div',{'class':'price'})
            info_dict[u'totalprice'] = price.span.text.strip()

            col2 = info_panel.find('div',{'class':'col-2'})
            sellcount = col2.find('div',{'class':'square'}).find('div')
            info_dict[u'sellcount'] = sellcount.a.span.text.strip()

        except Exception as e:
            print e
            exception_write(e, 'xiaoqu_page_search', str(j))
            continue
        
        print u"----------" + str(j)
        try:
            command = gen_xiaoqu_insert_command(info_dict)
            db_xq.execute(command)
        except Exception as e:
            print e
            exception_write(e, 'xiaoqu_page_search_db', str(j))
            continue


def bizcircle_xiaoqu_search(db_xq,bizcircle=u"/xiaoqu/beicai/", name=u"北蔡"):

    trytimes = 0
    url=u"http://sh.lianjia.com"+bizcircle
    while 1:
        try:
            req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
            opener = urllib2.build_opener(urllib2.HTTPCookieProcessor)
            source_code = opener.open(req,timeout=5).read()
            plain_text=unicode(source_code)#,errors='ignore')
            soup = BeautifulSoup(plain_text)
        except socket.timeout as e:
            if trytimes < 5:
                time.sleep(2)
                trytimes += 1
                continue
            else:
                print e
                exception_write(e, 'bizcircle_xiaoqu_search', bizcircle)
                return
    
        except (urllib2.HTTPError, urllib2.URLError), e:
            print e
            exception_write(e, 'bizcircle_xiaoqu_search', bizcircle)
            return
        except Exception,e:
            print e
            exception_write(e, 'bizcircle_xiaoqu_search', bizcircle)
            return
        
        human = soup.find('div',{'class':'human'})
        
        if not human:
            break
        else:
            print "block! && wait"
            time.sleep(600)
            trytimes = 0

    pagebox = soup.find('div',{'class':'page-box house-lst-page-box'})
    if not pagebox:
        print "---total number of page is 0"
        return
    tpage = pagebox.find('a',{'gahref':'results_totalpage'})
    npage = pagebox.find('a',{'gahref':'results_next_page'})
    allpage = pagebox.findAll('a')
    if tpage:
        totalpage = int(tpage['href'].split('/d')[-1])
    else:
        if npage:
            totalpage = int(allpage[-2]['href'].split('/d')[-1])
        else:
            totalpage = 1;

    print "---total number of page is " + str(totalpage)

    for i in range(totalpage):
        url_page = u"http://sh.lianjia.com%sd%d/" % (bizcircle, i + 1)
        xiaoqu_page_search(db_xq, url_page)
        print u"------" + name + "  " + str(i) + "th page have been done"

    print u"---爬下了 %s 商圈全部的小区信息" % name



def area_xiaoqu_search(db_xq,region=u"pudongxinqu"):
   
    trytimes = 0
    url=u"http://sh.lianjia.com/xiaoqu/"+region+"/"
    while 1:
        try:
            req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
            opener = urllib2.build_opener(urllib2.HTTPCookieProcessor)
            source_code = opener.open(req,timeout=5).read()
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

    
    bizcircles = soup.find('dl',{'class':'dl-lst clear'}).find('div',{'class':'option-list sub-option-list gio_plate'}).findAll('a')

    print u" %s 区共有 %d 个商圈" % (region, len(bizcircles)-1)
    bizcircles_href=[]
    bizcircles_name=[]
    for i in range(len(bizcircles) - 1):
        bizcircles_href.append(bizcircles[i + 1]['href'])
        bizcircles_name.append(bizcircles[i + 1]['title'])
    #print bizcircles_name

    for i in range(len(bizcircles_href)):
        print u"%d 开始爬 %s 商圈的小区" % (i,bizcircles_name[i])
        bizcircle_xiaoqu_search(db_xq, bizcircles_href[i],bizcircles_name[i])

    print u"爬下了 %s 区全部的小区信息" % region


def exception_write(e, fun_name,url):
    f = open(storename + '_log.txt','a')
    line = "%s\t%s\t%s\n" % (e, fun_name, url)
    f.write(line)
    f.close()
    

if __name__=="__main__":
    #上海区域列表
    regions=[u"pudongxinqu",
            u"minhang",
            u"baoshan",
            u"xuhui",
            u"putuo",
            u"yangpu",
            u"changning",
            u"songjiang",
            u"jiading",
            u"huangpu",
            u"jingan",
            u"zhabei",
            u"hongkou",
            u"qingpu",
            u"fengxian",
            u"jinshan",
            u"chongming",
            u"shanghaizhoubian"]

    db_xq=SQLiteWraper(storename + '.db')
    
    create_command = """create table if not exists xiaoqu 
                (href TEXT primary key UNIQUE, 
                name TEXT,
                idx TEXT,
                idy TEXT,
                district TEXT, 
                bizcircle TEXT,
                buildyear TEXT, 
                chanquan TEXT,
                totalprice TEXT,
                sellcount TEXT)"""
    

    db_xq.execute(create_command)

    #spider xiaoqu info
    for region in regions:
        area_xiaoqu_search(db_xq,region)

    db_xq.close()

    print 'all done'
