# -*- coding: utf-8 -*-
"""
Created on Tue May 22 21:48:50 2018

@author: yiyuezhuo
"""

import os

import argparse

parser = argparse.ArgumentParser(usage=u'python main.py data2016 data2016-gbk',
                                 description=u"convert utf8 to gbk tool")
parser.add_argument('source')
parser.add_argument('dest')

args = parser.parse_args()
source = args.source
dest = args.dest

for dir_path,sub_dir,fnames in os.walk(args.source):
    for fname in fnames:
        path = os.path.join(dir_path,fname)
        path_list = path.split(os.path.sep)
        dest_root = os.path.join(dest, os.path.sep.join(path_list[1:-1]))
        dest_path = os.path.join(dest, os.path.sep.join(path_list[1:]))
        os.makedirs(dest_root,exist_ok=True)
        with open(path,encoding='utf8') as f:
            d = f.read()
        with open(dest_path,'w',encoding='gbk') as f:
            f.write(d)
