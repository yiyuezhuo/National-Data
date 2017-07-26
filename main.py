# -*- coding: utf-8 -*-
"""
Created on Sun Apr 03 12:15:15 2016

@author: yiyuezhuo
"""

import requests
import time
import json
import os
import pickle
import pandas as pd
import numpy as np

url='http://data.stats.gov.cn/easyquery.htm'
s='''id:zb
dbcode:hgnd
wdcode:zb
m:getTree'''

dic=dict(term.split(':') for term in s.split('\n'))

res=requests.get(url,params=dic)

def check_dir(name_list):
    #if type(name_list) in [str,unicode]:
    if type(name_list) in [str,bytes]:
        name_list=name_list.replace('\\','/').split('/')
    now_path=name_list[0]
    for name in name_list[1:]:
        if not os.path.isdir(now_path):
            os.mkdir(now_path)
        now_path=os.path.join(now_path,name)

class TreeNode(object):
    url='http://data.stats.gov.cn/easyquery.htm'
    params={'id':'zb','dbcode':'hgnd','wdcode':'zb','m':'getTree'}
    def __init__(self,iid='zb',name='zb',data_me=None):
        self.id=iid
        self.name=name
        self.data_me=None#Only leaf need this field
        self.data=None
        self.children=[]
        self.leaf=None
    def get(self,force=False,verbose=True):
        if verbose:
            print('getting',self.id,self.name)
        if force or self.data==None:
            params=TreeNode.params.copy()
            params['id']=self.id
            res=requests.get(TreeNode.url,params=params)
            self.data=res.json()
            for data in self.data:
                self.children.append(TreeNode(iid=data['id'],name=data['name'],
                                              data_me=data))
            self.leaf=len(self.children)==0
    def get_recur(self,force=False,verbose=True):
        if force or self.data==None:
            self.get(verbose=verbose)
            for child in self.children:
                child.get_recur()
    def to_dict(self):
        children=[child.to_dict() for child in self.children]
        rd=self.data.copy()
        rd['children']=children
        return rd
    def display(self,level=0):
        print(' '*level+self.name+' '+self.id)
        for child in self.children:
            child.display(level+1)
    def get_all_pair(self):
        if self.leaf:
            return [(self.id,self.name)]
        else:
            rl=[]
            for child in self.children:
                rl.extend(child.get_all_pair())
            return rl
            

class Downloader(object):
    def __init__(self,tree,raw_root='raw',date='1978-2014'):
        self.tree=tree
        self.map_name=dict(tree.get_all_pair())
        self.map_json={}
        self.raw_root=raw_root
        self.date=date
    def get_params(self,valuecode):
        params={'m':'QueryData','dbcode':'hgnd',
                'rowcode':'zb','colcode':'sj',
                'wds':[],
                'dfwds':[{'wdcode':'zb','valuecode':None},
                         {'wdcode':'sj','valuecode':self.date}],
                'k1':None}
        # requests can't deal tuple,list,dict correctly,I transform
        #them to string and replace ' -> " to solve it
        params['dfwds'][0]['valuecode']=str(valuecode)#Shocked!requests can't handle unicode properly
        params['k1']=int(time.time()*1000)
        rp={key:str(value).replace("'",'"') for key,value in params.items()}
        return rp
    def download_once(self,valuecode,to_json=False):
        url='http://data.stats.gov.cn/easyquery.htm'
        params=self.get_params(valuecode)
        res=requests.get(url,params=params)
        if to_json:
            return res.json()
        else:
            return res.content
    def valuecode_path(self,valuecode):
        return os.path.join(self.raw_root,valuecode)
    def cache(self,valuecode,content):
        '''
        f=open(self.valuecode_path(valuecode),'wb')
        f.write(content)
        f.close()
        '''
        with open(self.valuecode_path(valuecode),'wb') as f:
            f.write(content)
    def is_exists(self,valuecode,to_json=False):
        if to_json:
            return self.map_json.has_key(valuecode)
        else:
            path=os.path.join(self.raw_root,valuecode)
            return os.path.isfile(path)
    def download(self,verbose=True,to_json=False):
        length=len(self.map_name)
        for index,valuecode in enumerate(self.map_name.keys()):
            if verbose:
                print('get data',valuecode,self.map_name[valuecode],'clear',float(index)/length)
            if not self.is_exists(valuecode,to_json=to_json):
                res_obj=self.download_once(valuecode,to_json=to_json)
                if to_json:
                    self.map_json[valuecode]=res_obj
                else:
                    self.cache(valuecode,res_obj)
                    
class Document(object):
    def __init__(self,raw_root='raw'):
        self.raw_root=raw_root
    def get(self,name):
        path=os.path.join(self.raw_root,name)
        with open(path,'r', encoding = 'utf8') as f:
            content=f.read()
        return content
    def get_json(self,name):
        return json.loads(self.get(name))
    def json_to_dataframe(self,dic,origin_code=True):
        assert dic['returncode']==200
        returndata=dic['returndata']
        datanodes,wdnodes=returndata['datanodes'],returndata['wdnodes']
        if not origin_code:#parse wdnodes for transform that
            wd={w['wdcode']:{ww['code']:ww['cname'] for ww in w['nodes']} for w in wdnodes}
            zb_wd,sj_wd=wd['zb'],wd['sj']
        rd={}
        for node in datanodes:
            sd={w['wdcode']:w['valuecode'] for w in node['wds']}
            zb,sj=sd['zb'],sd['sj']
            if not origin_code:
                zb,sj=zb_wd[zb],sj_wd[sj]
            rd[(sj,zb)]=node['data']['data'] if node['data']['hasdata'] else np.NaN
        df=pd.Series(rd).unstack()
        return df
    def get_dataframe(self,name,origin_code=False):
        return self.json_to_dataframe(self.get_json(name),origin_code=False)
    def to_csv(self,name,path,encoding='utf8'):
        df=self.get_dataframe(name)
        df.to_csv(path,encoding=encoding)
    def iter_tree(self,tree,path=('zb',),origin_dir=False):
        yield path,tree
        for node in tree.children:
            newpath=path+((node.id,) if origin_dir else (node.name,))
            for r in self.iter_tree(node,path=newpath):
                yield r
    def to_csv_all(self,tree,root='data',encoding='utf8'):
        for path,node in self.iter_tree(tree):
            if node.leaf:
                path_t=(root,)+path
                check_dir(path_t)
                self.to_csv(node.id,os.path.join(*path_t)+'.csv',encoding=encoding)
                
'''
def json_to_dataframe(dic,origin_code=True):
    assert dic['returncode']==200
    returndata=dic['returndata']
    datanodes,wdnodes=returndata['datanodes'],returndata['wdnodes']
    if not origin_code:#parse wdnodes for transform that
        wd={w['wdcode']:{ww['code']:ww['cname'] for ww in w['nodes']} for w in wdnodes}
        zb_wd,sj_wd=wd['zb'],wd['sj']
    rd={}
    for node in datanodes:
        sd={w['wdcode']:w['valuecode'] for w in node['wds']}
        zb,sj=sd['zb'],sd['sj']
        if not origin_code:
            zb,sj=zb_wd[zb],sj_wd[sj]
        rd[(sj,zb)]=node['data']['data']
    df=pd.Series(rd).unstack()
    return df
                
def cache(downloader,root='raw'):
    for key,value in downloader.map_json.items():
        with open(os.path.join(root,key),'wb') as f:
            json.dump(value,f)
            
def semicode(params):
    return {key:str(value).replace("'",'"') for key,value in params.items()}
    
def test_direct():
    valuecode='A0E030A'
    params={'m':'QueryData','dbcode':'hgnd',
            'rowcode':'zb','colcode':'sj',
            'wds':[],
            'dfwds':[{'wdcode':'zb','valuecode':None},
                     {'wdcode':'sj','valuecode':'1978-2014'}],
            'k1':None}
    # requests can't deal tuple,list,dict correctly,I transform
    #them to string and replace ' -> " to solve it
    params['dfwds'][0]['valuecode']=valuecode
    params['k1']=int(time.time()*1000)
    rp={key:str(value).replace("'",'"') for key,value in params.items()}
    url='http://data.stats.gov.cn/easyquery.htm'
    res=requests.get(url,params=rp)
    return res.json()

def test(tree):
    downloader=Downloader(tree)
    return downloader.download_once(downloader.map_name.keys()[0])
'''

def run(args):
    
    print('init tree')
    if os.path.isfile(args.tree):
        print('init tree by cache')
        with open(args.tree,'rb') as f:
            tree=pickle.load(f)
    else:
        print('init tree by web')
        tree=TreeNode()
        tree.get_recur()
        with open(args.tree,'wb') as f:
            print('cache tree information...')
            pickle.dump(tree,f)
            
    if not os.path.isdir(args.raw):
        os.mkdir(args.raw)
    if not os.path.isdir(args.dest):
        os.mkdir(args.dest)
    
    print('start download file')
    downloader=Downloader(tree,raw_root=args.raw,date=args.date)
    downloader.download()
    print('start transform JSON raw file to csv file')
    doc=Document(raw_root=args.raw)
    doc.to_csv_all(tree,root=args.dest,encoding=args.encoding)
    print('clear')
    
def CLI():
    import argparse
    parser = argparse.ArgumentParser(usage=u'python main.py --encoding utf-8 --date 1978-2015 --dest new_data --raw new_tree',
                                     description=u"国家数据抓取器")
    parser.add_argument('--type',default='year',help=u'抓取哪种类型的数据，目前没用')
    parser.add_argument('--encoding',default='utf-8',help=u"输出的csv文件的编码,默认的UTF8可能对Excel不友好")
    parser.add_argument('--date',default='1978-2015',help=u'请求的数据区间如 --date 1978-2015')
    parser.add_argument('--dest',default='data',help=u"输出目录")
    parser.add_argument('--raw',default='raw',help=u'中间json文件保存目录')
    parser.add_argument('--tree',default='tree',help=u'tree文件的缓存地址,默认为tree')
    
    args=parser.parse_args()
    run(args)
    

    
if __name__=="__main__":
    import sys
    if len(sys.argv)<=1:
        print('DEBUG MODE')
        print('IF YOU WANT USE IT IN CLI, YOU NEED A ARGUMENT TO ACTIVATE IT')
        
        # It provide a helper varible to support debug 
        class Args(object):
            pass
        args = Args()
        args.type = 'year'
        args.encoding = 'utf-8'
        #args.date = '1978-2014'
        args.date = '1978-2015'
        args.dest = 'data_test'
        args.raw = 'raw_test'
        args.tree = 'tree_test'
        #run(args)
    else:
        CLI()

'''
tree=TreeNode()
tree.get_recur()
tree.display()
'''

'''
with open('tree','rb') as f:
    tree=pickle.load(f)
downloader=Downloader(tree,date='1978-2014')
downloader.download()
doc=Document()
'''