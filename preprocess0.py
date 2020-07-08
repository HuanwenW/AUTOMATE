#!/usr/bin/env python36
# -*- coding: utf-8 -*-
"""
Created on July, 2018

@author: Tangrizzly
"""

import argparse
import time
import csv
import pickle
import operator
import datetime
import os

parser = argparse.ArgumentParser()
parser.add_argument('--dataset', default='yoochoose', help='dataset name: diginetica/yoochoose/sample')
opt = parser.parse_args()
print(opt)

# 确定数据集
dataset = 'sample_train-item-views.csv'
if opt.dataset == 'diginetica':
    dataset = 'train-item-views.csv'
elif opt.dataset =='yoochoose':
    dataset = 'yoochoose-clicks.dat'

print("-- Starting @ %ss" % datetime.datetime.now())
# FIELDS = ['session_id', 'Timestamp', 'item_id', 'Category'] # session_id;user_id;item_id;timeframe;eventdate

# 第一次数据处理： 表中的数据 存储到2个字典里
with open(dataset, "r") as f:
    if opt.dataset == 'yoochoose':
        # data_file = open('yoochoose-clicks.dat', 'wb')
        # writer = csv.DictWriter(FIELDS,fieldnames=FIELDS)
        # writer.writerow(dict(zip(FIELDS, FIELDS)))
        reader = csv.DictReader(f, delimiter=',')#读文件

    else:
        reader = csv.DictReader(f, delimiter=';')
#  定义两个字典
    sess_clicks = {}  # 存储各个key值为 session_id ： value值为 session_id点击的 item_id
    sess_date = {}    # 存储各个key值为 session_id ： value值为 Timestamp
    ctr = 0
    curid = -1
    curdate = None # 当前时间

    for data in reader:
        sessid = data['session_id'] # data 从第一行数据开始读取，获取reader(原始点击文件)中的 session_id

       # 4个if 分别为了获取 sesion_id中最后点击的item_Id对应的时间、点击项目id、点击项目时的时间（不转换形式）、sesion和itemid的组合
        if curdate and not curid == sessid: # not>and>or
            date = ''
            if opt.dataset == 'yoochoose': # yoochoose的时间精确到秒－－－eg:2014-04-07T10:51:09.277Z
                date = time.mktime(time.strptime(curdate[:19], '%Y-%m-%dT%H:%M:%S'))# 将格式字符串转换为时间戳

            else:
                date = time.mktime(time.strptime(curdate, '%Y-%m-%d'))

            sess_date[curid] = date  # 得到各个session_id下最后一个点击item_id的时间

        curid = sessid # 获取当前的session_id

        if opt.dataset == 'yoochoose': # 获取 当前点击项目--item_id#
            item = data['item_id']
        else:
            item = data['itemId'], int(data['timeframe']) #Diginica 数据集中的字段

        curdate = '' # 重新清空

        if opt.dataset == 'yoochoose':  # 获取 当前时间--timestamp#
            curdate = data['timestamp']
        else:
            curdate = data['eventdate']

# 获取 当前session下的点击的所有项目 #
        if sessid in sess_clicks:
            sess_clicks[sessid] += [item]
        else:
            sess_clicks[sessid] = [item]
        ctr += 1
    date = ''
    #  获取最后一条数据(11299811,2014-09-24T19:02:25.146Z,214848658,S)的时间按并将格式字符串转换为时间戳
    if opt.dataset == 'yoochoose': # 两个数据集的时间格式不一样，分开做处理
        date = time.mktime(time.strptime(curdate[:19], '%Y-%m-%dT%H:%M:%S'))
    else:
        date = time.mktime(time.strptime(curdate, '%Y-%m-%d'))

        for i in list(sess_clicks):
            sorted_clicks = sorted(sess_clicks[i], key=operator.itemgetter(1))
            sess_clicks[i] = [c[0] for c in sorted_clicks]
    sess_date[curid] = date

print("-- Reading data @ %ss" % datetime.datetime.now()) # 输出实验运行的当前时间

# 第2次数据处理：过滤掉只有1次点击的会话
# Filter out length 1 sessions 过滤掉只有1次点击的会话
for s in list(sess_clicks):
    if len(sess_clicks[s]) == 1:
        del sess_clicks[s]
        del sess_date[s]

# 第3次数据处理：计算每个item_id出现的次数
# Count number of times each item appears　
iid_counts = {} # 字典 统计各个item_id（key值）出现的总个数（value值）
for s in sess_clicks:
    seq = sess_clicks[s]
    for iid in seq:
        if iid in iid_counts:
            iid_counts[iid] += 1 # 已出现过的item直接+1
        else:
            iid_counts[iid] = 1 # 未出现过的赋值为1

sorted_counts = sorted(iid_counts.items(), key=operator.itemgetter(1)) # 根据第1个域（即item_id的出现的总个数）进行一个从小到大的排序#

# 第4次数据处理：再一次加限制条件对session_clicks做筛选
length = len(sess_clicks) # 目前过滤后剩下7990018个session
for s in list(sess_clicks):
    curseq = sess_clicks[s] # 获取当前session中的点击序列
    # 筛选session中的item_Id在所有数据中出现次数>= 5的;其中iid_counts[i]>= 5表示项目i出现超过5次
    filseq = list(filter(lambda i: iid_counts[i] >= 5, curseq))# lambda 可一次传入多个参数的操作，此语句含义
    # print(filseq[:3])
    if len(filseq) < 2: # 对session中的总数小于2的进行下面的删除操作
        del sess_clicks[s]
        del sess_date[s]
    else:
        sess_clicks[s] = filseq
print("len(sess_clicks):",len(sess_clicks)) # hh测下此次过滤后省剩下的7981581

# 第5次数据处理：根据日期拆分训练和测试集
# Split out test set based on dates　
a = sess_date
b = sess_date.items()
dates = list(sess_date.items()) # 由 字典 形式处理转换为 list 形式 # dates = [('1',1396839420.0),('2',1396850556.0),...]

# 找到时间的最大值
maxdate = dates[0][1] # 初始化maxdate为第一行第一列
for _, date in dates:
    if maxdate < date:
        maxdate = date
print('maxdate',maxdate)

mindate = dates[0][1] # 找date中的中小值
for _, date in dates:
    if mindate > date:
        mindate = date
print('mindate',mindate)

# 确定分割点
splitdate = 0 # 初始化 时间分割点=最大值 减去 一天的时间
if opt.dataset == 'yoochoose':
    splitdate = maxdate - 86400 * 1  # the number of seconds for a day：86400=24×60×60
else:
    splitdate = maxdate - 86400 * 7
print('Splitting date:', splitdate)      # Yoochoose: ('Split date', 1411930799.0)

# 拆分条件
tra_sess = filter(lambda x: x[1] < splitdate, dates) # 训练集
tes_sess = filter(lambda x: x[1] > splitdate, dates) # 测试集

# Sort sessions by date　按日期对会话排序
tra_sess = sorted(tra_sess, key=operator.itemgetter(1))     # 根据对象的第1个域（即时间戳）进行排序
tes_sess = sorted(tes_sess, key=operator.itemgetter(1))     #
                        # sample.data   # yoochoose
print('len(tra_sess):',len(tra_sess))    # 186670        # 7966257
print('len(tes_sess):',len(tes_sess))    # 15979         # 15324
'''
# print(tra_sess[:3])# hh做测试，看下拆分后的数据集样子
# print(tes_sess[:3])
'''
print("-- Splitting train set and test set @ %ss" % datetime.datetime.now()) # 惯例 输出当前时间

# 第6次数据处理：获得 训练集 中各个sessionID内itemID的重新排序(从1开始)
item_dict = {} # 为item定义一个字典
def obtian_tra():
    train_ids = []
    train_seqs = []
    train_dates = []
    item_ctr = 1
    for s, date in tra_sess: # 分别对session_id和 data 做循环
        seq = sess_clicks[s] # 通过s获取session内的item
        outseq = []
        for i in seq:
            if i in item_dict:
                outseq += [item_dict[i]]
            else:
                outseq += [item_ctr]
                item_dict[i] = item_ctr
                item_ctr += 1
        if len(outseq) < 2:  # Doesn't occur
            continue
        train_ids += [s]
        train_dates += [date]
        train_seqs += [outseq]
    print('item_ctr:',item_ctr)     # 43098, 37484
    return train_ids, train_dates, train_seqs  # 得到3个数组

# 第7次数据处理：获得 测试集 中各个sessionID内itemID的重新排序，编号不是从1开始，而是接着train中itemID排的
# Convert test sessions to sequences, ignoring items that do not appear in training set
def obtian_tes():
    test_ids = []
    test_seqs = []
    test_dates = []
    for s, date in tes_sess:
        seq = sess_clicks[s]
        outseq = []
        for i in seq:
            if i in item_dict:
                outseq += [item_dict[i]]
        if len(outseq) < 2:
            continue
        test_ids += [s]
        test_dates += [date]
        test_seqs += [outseq]
    return test_ids, test_dates, test_seqs

tra_ids, tra_dates, tra_seqs = obtian_tra()  # 调用def obtian_tra()函数，得到的是函数的返回值
tes_ids, tes_dates, tes_seqs = obtian_tes()  # 调用def obtian_tes()函数，获取返回值

# 目前还是没有明白  为啥把session中的item序列拆开--20190925
def process_seqs(iseqs, idates):
    out_seqs = []
    out_dates = []
    labs = []
    ids = []
    for id, seq, date in zip(range(len(iseqs)), iseqs, idates):
        for i in range(1, len(seq)):
            tar = seq[-i] # 取当前seq中的第-i个元素
            labs += [tar] # 加到labs中
            out_seqs += [seq[:-i]] # 取seq不含-i之前的所有元素
            out_dates += [date]
            ids += [id]
    return out_seqs, out_dates, labs, ids

# 调用def process_seqs(iseqs, idates)函数，但是传入的参数不同
tr_seqs, tr_dates, tr_labs, tr_ids = process_seqs(tra_seqs, tra_dates)
te_seqs, te_dates, te_labs, te_ids = process_seqs(tes_seqs, tes_dates)

tra = (tr_seqs, tr_labs)  # 包含2列的一个元组
tes = (te_seqs, te_labs)
print('len(tr_seqs):',len(tr_seqs)) # 23670982
print('len(te_seqs):',len(te_seqs)) # 55898
'''
print(tr_seqs[:3], tr_dates[:3], tr_labs[:3]) # hh测试用的
# [[1], [3], [5, 5]] [1396292432.0, 1396292475.0, 1396292502.0] [2, 4, 5]
 print(te_seqs[:3], te_dates[:3], te_labs[:3])
'''
# 获取session的平均长度  3.9727042800167034(总个数==31708461)
all = 0
for seq in tra_seqs:
    all += len(seq)
for seq in tes_seqs:
    all += len(seq)
print('avg length: ', all/(len(tra_seqs) + len(tes_seqs) * 1.0))

# 把处理好数据集处理 写入文件
if opt.dataset == 'diginetica':
    if not os.path.exists('diginetica'):
        os.makedirs('diginetica')
    pickle.dump(tra, open('diginetica/train.txt', 'wb')) # wb二进制格式打开一个文件用于读写
    pickle.dump(tes, open('diginetica/test.txt', 'wb'))
    pickle.dump(tra_seqs, open('diginetica/all_train_seq.txt', 'wb'))

    # 输出yoochoose处理得到的6个文件（）
elif opt.dataset == 'yoochoose':
    # 创建2个路经及对应的test文件
    if not os.path.exists('yoochoose1_4'):
        os.makedirs('yoochoose1_4')
    if not os.path.exists('yoochoose1_8'):
        os.makedirs('yoochoose1_8')
    if not os.path.exists('yoochoose1_16'):
        os.makedirs('yoochoose1_16')
    if not os.path.exists('yoochoose1_32'):
        os.makedirs('yoochoose1_32')
    if not os.path.exists('yoochoose1_48'):
        os.makedirs('yoochoose1_48')
    if not os.path.exists('yoochoose1_64'):
        os.makedirs('yoochoose1_64')
    if not os.path.exists('yoochoose1_80'):
        os.makedirs('yoochoose1_80')
    # 1/4和1/64的测试集一样
    pickle.dump(tes, open('yoochoose1_4/test.txt', 'wb'))
    pickle.dump(tes, open('yoochoose1_8/test.txt', 'wb'))
    pickle.dump(tes, open('yoochoose1_16/test.txt', 'wb'))
    pickle.dump(tes, open('yoochoose1_32/test.txt', 'wb'))
    pickle.dump(tes, open('yoochoose1_48/test.txt', 'wb'))
    pickle.dump(tes, open('yoochoose1_64/test.txt', 'wb'))
    pickle.dump(tes, open('yoochoose1_80/test.txt', 'wb'))

    # 对长度进行1/4和1/61进行拆分规则
    split4,split8,split16, split32, split48, split64, split80 = int(len(tr_seqs) / 4), int(len(tr_seqs) / 8),int(len(tr_seqs) / 16), int(len(tr_seqs) / 32), int(len(tr_seqs) / 48), int(len(tr_seqs) / 64), int(len(tr_seqs) / 80)# yoochoose1/4和1/64的出处

    tra4, tra8, tra16, tra32, tra48, tra64, tra80 = (tr_seqs[-split4:], tr_labs[-split4:]), (tr_seqs[-split8:], tr_labs[-split8:]), (tr_seqs[-split16:], tr_labs[-split16:]), (tr_seqs[-split32:], tr_labs[-split32:]), (tr_seqs[-split48:], tr_labs[-split48:]), (tr_seqs[-split64:], tr_labs[-split64:]),  (tr_seqs[-split80:], tr_labs[-split80:]) # 经过 函数 process_seqs 处理的数据 拆分 1/4 和1/64
    seq4, seq8, seq16, seq32, seq48, seq64, seq80 = tra_seqs[tr_ids[-split4]:], tra_seqs[tr_ids[-split8]:], tra_seqs[tr_ids[-split16]:], tra_seqs[tr_ids[-split32]:], tra_seqs[tr_ids[-split48]:], tra_seqs[tr_ids[-split64]:], tra_seqs[tr_ids[-split80]:] # 对未经过 函数 process_seqs 处理的数据 拆分 1/4 和1/64

    pickle.dump(tra4, open('yoochoose1_4/train.txt', 'wb'))
    pickle.dump(seq4, open('yoochoose1_4/all_train_seq.txt', 'wb'))
    pickle.dump(tra8, open('yoochoose1_8/train.txt', 'wb'))
    pickle.dump(seq8, open('yoochoose1_8/all_train_seq.txt', 'wb'))
    pickle.dump(tra16, open('yoochoose1_16/train.txt', 'wb'))
    pickle.dump(seq16, open('yoochoose1_16/all_train_seq.txt', 'wb'))
    pickle.dump(tra32, open('yoochoose1_32/train.txt', 'wb'))
    pickle.dump(seq32, open('yoochoose1_32/all_train_seq.txt', 'wb'))
    pickle.dump(tra48, open('yoochoose1_48/train.txt', 'wb'))
    pickle.dump(seq48, open('yoochoose1_48/all_train_seq.txt', 'wb'))
    pickle.dump(tra64, open('yoochoose1_64/train.txt', 'wb'))
    pickle.dump(seq64, open('yoochoose1_64/all_train_seq.txt', 'wb'))
    pickle.dump(tra80, open('yoochoose1_80/train.txt', 'wb'))
    pickle.dump(seq80, open('yoochoose1_80/all_train_seq.txt', 'wb'))

else:
    if not os.path.exists('sample'):
        os.makedirs('sample')
    pickle.dump(tra, open('sample/train.txt', 'wb'))
    pickle.dump(tes, open('sample/test.txt', 'wb'))
    pickle.dump(tra_seqs, open('sample/all_train_seq.txt', 'wb'))

print('Done.')
