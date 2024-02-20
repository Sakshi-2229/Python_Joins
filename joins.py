# -*- coding: utf-8 -*-
"""
Created on Wed Aug  9 14:29:58 2023

@author: sai
"""

#JOINS

import pandas as pd
technologies={
    'Courses' : ["Spark","pyspark","Python","pandas"],
    'Fee' : [20000,25000,22000,30000],
    'Duration' : ['30days','40days','35days','50days']
    }
index_labels=['r1','r2','r3','r4']
df1 = pd.DataFrame(technologies,index=index_labels)


technologies2={
    'Courses':['Spark','Java','Python','Go'],
    'Discount':[2000,2300,1200,2000]
    }
index_labels2=['r1','r6','r3','r5']     #matching entities should have same indexes.
df2 = pd.DataFrame(technologies2,index=index_labels2)

#matching entities should have same indexes.
#pandas inner join is mostly used join,
#It is used to join two Dataframes on indexes.
#When indexes dont match the rows get dropped from both the Datarame
#Pandas join by default it will join the table left join

df3=df1.join(df2, lsuffix="_left", rsuffix="_right")
df3

#######################################################################

#Pandas inner join dataframes

df3=df1.join(df2, lsuffix="_left", rsuffix="_right", how='inner')
df3

####################################################################

#pandas left join dataframe

df3=df1.join(df2, lsuffix="_left", rsuffix="_right", how='left')
df3

#pandas right join dataframe

df3=df1.join(df2, lsuffix="_left", rsuffix="_right", how="right")
df3

######################################################################

#Pandas join on columns
df3=df1.set_index('Courses').join(df2.set_index('Courses'), how='inner')
df3

#Left join
df3=df1.set_index('Courses').join(df2.set_index('Courses'), how='left')
df3

#right join
df3=df1.set_index('Courses').join(df2.set_index('Courses'), how='right')
df3

#using pandas.merge using inner join
df3=pd.merge(df1,df2)
df3

#using DataFrame.merge()
df3=df1.merge(df2)
df3

#######################################################################

#Using pandas.concat() to concat two DataFrames

import pandas as pd
df=pd.DataFrame({'Courses':["Spark","PySpark","Python","pandas"],
                 'Fee':[20000,250000,22000,24000]})

df1=pd.DataFrame({'Courses':["Pandas","Hadoop","Hyperion","Java"],
                  'Fee':[25000,25200,24500,24900]})
#using pandas.concat() to concat two DataFrames
data=[df, df1]
df2=pd.concat(data)
df2

#######################################################################

#Concatenate Multiple DataFrames using pandas.concat()

df=pd.DataFrame({'Courses':["Spark","PySpark","Python","pandas"],
                 'Fee':[20000,250000,22000,24000]})

df1=pd.DataFrame({'Courses':["Unix","Hadoop","Hyperion","Java"],
                  'Fee':['25000','25200','24500','24900']})

df2=pd.DataFrame({'Courses':['30days','40days','35days','60days','55days'],
                  'Fee':[1000,2300,2500,2000,3000]})

df3=pd.concat([df, df1, df2])
print(df3)
#########################################################################






























