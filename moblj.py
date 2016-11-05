#! /usr/bin/env python
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
import urllib2  
import sqlite3
import random
import threading
from bs4 import BeautifulSoup
import json
import time

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

import LianJiaLogIn

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

 
#北京区域列表
regions=[u"东城",u"西城",u"朝阳",u"海淀",u"丰台",u"石景山",u"通州",u"昌平",u"大兴",u"亦庄开发区",u"顺义",u"房山",u"门头沟",u"平谷",u"怀柔",u"密云",u"延庆",u"燕郊"]

lock = threading.Lock()

class SQLiteWraper(object):
    """
    数据库的一个小封装，更好的处理多线程写入
    """
    def __init__(self,path,command='',*args,**kwargs):  
        self.lock = threading.RLock() #锁  
        self.path = path #数据库连接参数  
        
        if command!='':
            conn=self.get_conn()
            cu=conn.cursor()
            cu.execute(command)
    
    def get_conn(self):  
        conn = sqlite3.connect(self.path)#,check_same_thread=False)  
        conn.text_factory=str
        return conn   
      
    def conn_close(self,conn=None):  
        conn.close()  
    
    def conn_trans(func):  
        def connection(self,*args,**kwargs):  
            self.lock.acquire()  
            conn = self.get_conn()  
            kwargs['conn'] = conn  
            rs = func(self,*args,**kwargs)  
            self.conn_close(conn)
            self.lock.release()  
            return rs  
        return connection  
    
    @conn_trans    
    def execute(self,command,method_flag=0,conn=None):  
        cu = conn.cursor()
        try:
            if not method_flag:
                cu.execute(command)
            else:
                cu.execute(command[0],command[1])
            conn.commit()
        except sqlite3.IntegrityError,e:
            #print e
            return -1
        except Exception, e:
            print e
            return -2
        return 0
    
    @conn_trans
    def fetchall(self,command="select name from xiaoqu",conn=None):
        cu=conn.cursor()
        lists=[]
        try:
            cu.execute(command)
            lists=cu.fetchall()
        except Exception,e:
            print e
            pass
        return lists


def gen_xiaoqu_insert_command(info_dict):
    """
    生成小区数据库插入命令
    """
    info_list=[u'小区名称',u'大区域',u'小区域',u'小区户型',u'建造时间']
    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into xiaoqu values(?,?,?,?,?)",t)
    return command

def gen_zufang_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list=[u'成交网页',u'小区名称',u'户型',u'租住面积',u'朝向',u'楼层',u'小区ID',u'签约日期',u'总层数',u'成交价',u'装修情况',u'数据来源']

    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into zufang values(?,?,?,?,?,?,?,?,?,?,?,?)",t)
    return command


def gen_mzufang_insert_command(info_dict):
    """
    生成成交记录数据库插入命令
    """
    info_list=[u'小区ID', u'小区名称',u'户型',u'签约日期'u'成交价']

    t=[]
    for il in info_list:
        if il in info_dict:
            t.append(info_dict[il])
        else:
            t.append('')
    t=tuple(t)
    command=(r"insert into zufang values(?,?,?,?,?)",t)
    return command



def xiaoqu_page_search(db_xq,url_page=u"http://bj.lianjia.com/xiaoqu/pg1rs%E6%98%8C%E5%B9%B3/"):

    numworkdone = 0
    try:
        req = urllib2.Request(url_page,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exit(-1)
    except Exception,e:
        print e
        exit(-1)
    
    xiaoqu_list=soup.findAll('li',{'class':'clear'})
    for xq in xiaoqu_list:
        info_dict={}
        info_dict.update({u'小区名称':xq.find('div', {'class':'title'}).text})
        content = unicode(xq.find('div',{'class':'positionInfo'}).renderContents().strip())
        info=re.match(r".+>(.+)</a>\xa0<.+>(.+)</a>\xa0(.+)\xa0(.+)",content)
        info = info.groups()
        info_dict.update({u'大区域':info[0]})
        info_dict.update({u'小区域':info[1]})
        info_dict.update({u'小区户型':info[2]})
        info_dict.update({u'建造时间':info[3][:4]})
        command=gen_xiaoqu_insert_command(info_dict)
        db_xq.execute(command,1)
        numworkdone += 1

    print "%s finished " % url_page + "%d works" % numworkdone

    
def area_xiaoqu_search(db_xq,region=u"昌平"):
    
    url=u"http://bj.lianjia.com/xiaoqu/rs"+region+"/"
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=5).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        return
    except Exception,e:
        print e
        return

    xqtotal_num = int(soup.find('h2',{'class':'total fl'}).span.text.strip())

    print u"开始爬 %s 区全部的小区信息" % region
    print "total number of xiaoqu is " + str(xqtotal_num)

    d="d="+soup.find('div',{'class':'page-box house-lst-page-box'}).get('page-data')
    exec(d)
    total_pages=d['totalPage']
    
    threads=[]
    for i in range(total_pages):
        url_page=u"http://bj.lianjia.com/xiaoqu/pg%drs%s/" % (i+1,region)
        t=threading.Thread(target=xiaoqu_page_search,args=(db_xq,url_page))
        threads.append(t)
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print u"爬下了 %s 区全部的小区信息" % region


def xiaoqu_mzufang_spider(db_mzf,xq_name=u"京师园",done=0):
   
    url=u"http://m.lianjia.com/zufang/rs"+urllib2.quote(xq_name)+"/"
    try:
        req = urllib2.Request(url,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('xiaoqu_zufang_spider',xq_name)
        return
    except Exception,e:
        print e
        exception_write('xiaoqu_zufang_spider',xq_name)
        return
    
    human = soup.find('div',{'class':'human'})
    record = soup.find('li',{'class':'pictext'})
    if human:
        print "block!"
        with open('mlast', 'w') as f:
            f.write(str(done))
        exit(-1)

    if not record:
        print "no zufang record"
        return

    recordurl = u"http://m.lianjia.com" + unicode(record.a['href'])
    #houseid = recordurl[29:-5]
    try:
        req = urllib2.Request(recordurl,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('xiaoqu_zufang_spider',xq_name)
        return
    except Exception,e:
        print e
        exception_write('xiaoqu_zufang_spider',xq_name)
        return

    human = soup.find('div',{'class':'human'})
    zufang_chengjiao = soup.find('div', {'class': 'mod_box house_lists'})

    if human:
        print "block!"
        with open('mlast', 'w') as f:
            f.write(str(done))
        exit(-1)
    if not zufang_chengjiao:
        print "page has no zufang_box"
        return

    zufang_list = zufang_chengjiao.find('div',{'class':'detail_more'})

    if not zufang_list:
        print "page has no zufang_list"
        return

    #xiaoquid = unicode(soup.find('div',{'class':'zf-room'}).findAll('p')[5].a['href'][8:-1])
    #houseid = soup.find('div',{'class':'houseRecord'}).span.text[5:]
    zfurl = u"http://m.lianjia.com" + unicode(zufang_list.a['href'])

    try:
        req = urllib2.Request(zfurl,headers=hds[random.randint(0,len(hds)-1)])
        source_code = urllib2.urlopen(req,timeout=10).read()
        plain_text=unicode(source_code)#,errors='ignore')   
        soup = BeautifulSoup(plain_text)
    except (urllib2.HTTPError, urllib2.URLError), e:
        print e
        exception_write('xiaoqu_zufang_spider',xq_name)
        return
    except Exception,e:
        print e
        exception_write('xiaoqu_zufang_spider',xq_name)
        return

    human = soup.find('div',{'class':'human'})
    li = soup.find('ul', {'class': 'lists'})
    xiaoquid = zfurl[31:-8]
    if human:
        print "block!"
        with open('mlast', 'w') as f:
            f.write(str(done))
        exit(-1)

    total = int(li['data-info'][6:])

    ls = li.findAll('li')
    
    for l in ls:
        info_dict={}
        info_dict.update({u'小区名称': xq_name})
        info_dict.update({u'小区ID': xiaoquid})
        #info_dict.update({u'成交网页': zfrecord['viewUrl']})
        info_dict.update({u'签约日期': l.find('span',{'class': 'deal_time'}).string[5:]})
        #info_dict.update({u'总层数': zfrecord['totalFloor']})
        info_dict.update({u'户型': l.find('div',{'class': 'item_minor'}).string})
        #info_dict.update({u'数据来源': zfrecord['signSource']})
        info_dict.update({u'成交价': l.find('span',{'class': 'price_total'}).string[:-3]})
        #info_dict.update({u'朝向': zfrecord['orientation']})
        #info_dict.update({u'楼层': zfrecord['floor']})
        #info_dict.update({u'租住面积': zfrecord['area']})
        #info_dict.update({u'装修情况': zfrecord['decoration']})
        #print info_dict
        command=gen_zufang_insert_command(info_dict)
        #print command
        db_mzf.execute(command,1)

    if total <= 10:
        print xq_name + "has done!"
        return

    refreshtimes = int ((total - 10) / 50) + 1

    for i in range(refreshtimes):
        newlists = zfurl + "?page_size=" + unicode(50) + "&_t=1&offset=" + unicode(10 + i * 50)
        try:
            req = urllib2.Request(newlists,headers=hds[random.randint(0,len(hds)-1)])
            source_code = urllib2.urlopen(req,timeout=10).read()
            plain_text=unicode(source_code)#,errors='ignore')   
            soup = BeautifulSoup(plain_text)
        except (urllib2.HTTPError, urllib2.URLError), e:
            print e
            exception_write('xiaoqu_zufang_spider',xq_name)
            return
        except Exception,e:
            print e
            exception_write('xiaoqu_zufang_spider',xq_name)
            return


        human = soup.find('div',{'class':'human'})
        li = soup.find('ul', {'class': 'lists'})

        if human:
            print "block!"
            with open('mlast', 'w') as f:
                f.write(str(done))
            exit(-1)

        ls = li.findAll('li')
    
        for l in ls:
            info_dict={}
            info_dict.update({u'小区名称': xq_name})
            info_dict.update({u'小区ID': xiaoquid})
            #info_dict.update({u'成交网页': zfrecord['viewUrl']})
            info_dict.update({u'签约日期': l.find('span',{'class': 'deal_time'}).string[5:]})
            #info_dict.update({u'总层数': zfrecord['totalFloor']})
            info_dict.update({u'户型': l.find('div',{'class': 'item_minor'}).string})
            #info_dict.update({u'数据来源': zfrecord['signSource']})
            info_dict.update({u'成交价': l.find('span',{'class': 'price_total'}).string[:-3]})
            #info_dict.update({u'朝向': zfrecord['orientation']})
            #info_dict.update({u'楼层': zfrecord['floor']})
            #info_dict.update({u'租住面积': zfrecord['area']})
            #info_dict.update({u'装修情况': zfrecord['decoration']})
            #print info_dict
            command=gen_zufang_insert_command(info_dict)
            #print command
            db_mzf.execute(command,1)

    print xq_name + "has done!"



  

def exception_write(fun_name,url):
    """
    写入异常信息到日志
    """
    lock.acquire()
    f = open('log.txt','a')
    line="%s %s\n" % (fun_name,url)
    f.write(line)
    f.close()
    lock.release()


def exception_read():
    """
    从日志中读取异常信息
    """
    lock.acquire()
    f=open('log.txt','r')
    lines=f.readlines()
    f.close()
    f=open('log.txt','w')
    f.truncate()
    f.close()
    lock.release()
    return lines


def exception_spider(db_cj):
    """
    重新爬取爬取异常的链接
    """
    count=0
    excep_list=exception_read()
    while excep_list:
        for excep in excep_list:
            excep=excep.strip()
            if excep=="":
                continue
            excep_name,url=excep.split(" ",1)
            if excep_name=="xiaoqu_zufang_spider":
                xiaoqu_zufang_spider(db_cj,url)
                count+=1
            else:
                print "wrong format"
            print "have spidered %d exception url" % count
        excep_list=exception_read()
    print 'all done ^_^'
    





if __name__=="__main__":
    command="create table if not exists xiaoqu (name TEXT primary key UNIQUE, regionb TEXT, regions TEXT, style TEXT, year TEXT)"
    db_xq=SQLiteWraper('lianjia-xq.db',command)
    
    command="create table if not exists zufang (href TEXT primary key UNIQUE, name TEXT, style TEXT, area TEXT, orientation TEXT, floor TEXT, id TEXT, sign_time TEXT, totalfloor TEXT, price TEXT, decoration TEXT, signSource TEXT)"
    db_zf=SQLiteWraper('lianjia-zf.db',command)
    
    command="create table if not exists mzufang (id TEXT primary key UNIQUE, name TEXT, style TEXT, sign_time TEXT, price TEXT)"
    db_mzf=SQLiteWraper('mlianjia-zf.db',command)


    #爬下所有的小区信息
    #for region in regions:
    #    area_xiaoqu_search(db_xq,region)
    mlast = 'mlast'
    
    if os.path.exists(mlast):
        done = int(open(mlast, 'r').read(1000))
        print 'last', done
    
    #xq_list=db_xq.fetchall()
    xq_list=[]
    xq = open("zfdata.txt", "r")
    for line in xq:
        xq_list.append(line.strip('\r\n'))

    xq.close()
    total = len(xq_list)
    rest = total - done
    #print xq_list[0][0]
    print "total number of xiaoqu is %d" % total
    if done == 0:
        print "just start"
    else:
        print "last xiaoqu is %s" % xq_list[done - 1][0]
    for i in range(rest):
        #gap = random.randint(4, 10)
        #time.sleep(gap)
        xq = xq_list[i + done]
        print 'spidering xiaoqu %s' % xq
        xiaoqu_zufang_spider(db_mzf,xq, i + done)
    print 'done'

    #exception_spider(db_zf)
    
