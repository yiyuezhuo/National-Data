# 国家统计局数据抓取器

数据来自国家统计局的国家数据网站

http://data.stats.gov.cn/index.htm

## 年鉴数据

### 直接使用

即网站的年度数据，在`data/zb`下有可以直接使用的*所有*指标的1978-2014年的csv文件(现在可以抓2015年数据了，
不过不重复上传到GitHub上，可以用下面的gbk版或自己运行。)
，当然特定时候没值就是空。

Excel可能对这里使用的无BOM UTF8编码处理有问题而导致乱码。如遇问题，可修改命令行中的`--encoding`选项
为gbk或者使用我另外转的<a href="https://pan.baidu.com/s/1qYobGdA">1978-2015年数据gbk编码版</a>
（就不重复上传占仓库大小了）。


### 使用脚本

```shell
$ python main.py --type year --date 1978-2015 --encoding utf-8
```

另外吐槽一下其实数据没压缩也才10+Mb。那些统计年鉴都是塞了一堆图才那么大的。