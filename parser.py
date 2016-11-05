#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8
#
# Copyright © 2016 Hongkun Yu <staryhk@gmail.com>
#
# @AUTHOR:      Hongkun Yu
# @MAIL:        staryhk@gmail.com
# @VERSION:     2016-10-24
#


#### to handle the net.txt to excel

fin = open("net.txt", "r")
lines = fin.readlines()
fin.close()

fout = open("net.xlsx")

total = int(lines[0])
nl = 1

for i in range(1,total):
    while lines[nl] !=
