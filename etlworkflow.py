# -*- coding: utf-8 -*-
"""
Created on Fri Sep  9 15:20:04 2022

@author: DSethura
"""

import subprocess
import utils

out=subprocess.run('python DIM_CHARGE_CATEG_py.py')

if out.returnvalue!=0:
   utile.sendmail('DIM_CHARGE_CATEG_py load failed',['dine@gail.com'])


subprocess.run('python DIM_CUST_py.py',check=True)





