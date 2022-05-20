# -*- coding: utf-8 -*-
"""
Created on Tue Nov 23 14:58:01 2021

@author: Kshitiz
"""

import numpy as np
import pandas as pd
import datetime

df = pd.read_csv('PricingData.csv')

df.head()

print("Total rows:",len(df))

print("Total Unique Buses:",len(df['Bus'].unique()))
print("Total Unique Service dates:",len(df['Service Date'].unique()))

df['Service Date'].unique()

df.isnull().sum()

#Missing Value Imputation
df['Seat Fare Type 2'].fillna('0',inplace=True)
df['Seat Fare Type 1'].fillna('0',inplace=True)

#Droppeing Duplicates
df = df.drop(df[(df['Seat Fare Type 2']=='0') & (df['Seat Fare Type 1']=='0')].index)
df = df.drop_duplicates()

#Converting String Data Format into datetime format
df['RecordedAt'] = pd.to_datetime(df['RecordedAt'],format = '%d-%m-%Y %H:%M')
df['Service Date'] = pd.to_datetime(df['Service Date'],format = '%d-%m-%Y %H:%M')

#Converting Seat Fares into float
for col in ['Seat Fare Type 1','Seat Fare Type 2']:
    df_1 = df[col].str.split(',',expand=True)
    for i in range(0,len(df_1.columns)):
        df_1[i] = df_1[i].astype(float)
    df[col] = df_1.mean(axis=1)
    df[col].fillna(0,inplace=True)

#Imputing the missing values of Seat Fare with its just previous value for same Service Date
Buses = list(df['Bus'].unique())
s_dates = list(df['Service Date'].unique())

for i in Buses:
    for k in s_dates:
        tmp1,tmp2 = 0,0
        for j in df[(df['Bus']==i) & (df['Service Date']==k)].index:
            if df['Seat Fare Type 1'][j]!=0:tmp1 = df['Seat Fare Type 1'][j]
            if df['Seat Fare Type 1'][j]==0:df['Seat Fare Type 1'][j] = tmp1
            if df['Seat Fare Type 2'][j]!=0:tmp2 = df['Seat Fare Type 2'][j]
            if df['Seat Fare Type 2'][j]==0:df['Seat Fare Type 2'][j] = tmp2

df = df.drop_duplicates()
    
non0_SFT1_buses = []
for x in df.Bus.unique():
    if df[df['Bus']==x]['Seat Fare Type 1'].sum()!=0:non0_SFT1_buses.append(x)

#Removal of rows of non0_SFT1_buses having 0 SFT1
for i in non0_SFT1_buses:
    for j in df[df['Bus']==i].index:
        if df['Seat Fare Type 1'][j]==0:
            df = df.drop(j)

zero_SFT1_buses,zero_SFT2_buses,non0_SFT_buses = [],[],[]
for x in df.Bus.unique():
    if df[df['Bus']==x]['Seat Fare Type 1'].sum()==0:zero_SFT1_buses.append(x)
    if df[df['Bus']==x]['Seat Fare Type 2'].sum()==0:zero_SFT2_buses.append(x)
    if (df[df['Bus']==x]['Seat Fare Type 1'].sum()!=0) and (df[df['Bus']==x]['Seat Fare Type 2'].sum()!=0):non0_SFT_buses.append(x)

print('Buses not having type 1 seats:',len(zero_SFT1_buses))
print('Buses not having type 2 seats:',len(zero_SFT2_buses))
print('Total buses (after data cleaning):',len(Buses))
print('Buses having both type 1 and type 2 seats:',len(non0_SFT_buses))

#There are multiple rows at same RecordedAt timestamp. This code will remove them.
for bus in non0_SFT_buses:
    for s_date in s_dates:
        tmp=[]
        for j in list(df[(df['Bus']==bus)&(df['Service Date']==s_date)].index)[::-1]:
            if len(tmp)==0:
                tmp.append(df['RecordedAt'][j])
            elif df['RecordedAt'][j]!=tmp[0]:
                tmp.pop()
                tmp.append(df['RecordedAt'][j])
            else:
                df = df.drop(j)

bus1_corrs=pd.DataFrame(columns=['Bus','Follows','Confidence Score (0 to 1)','Is followed by','Confidence Score (0 to 1)'])

def Avg(lst):
    return round(sum(lst) / len(lst),4)

def follows(bus1list,bus2list,sf_types,followed_by):
    tmpdf = pd.DataFrame(columns=['Bus','Follows','Confidence Score (0 to 1)','Is followed by','Confidence Score (0 to 1)'])
    for bus1 in bus1list:
        bus2_corrs={}
        for bus2 in bus2list:
            s_date_corrs = []
            for s_date in s_dates:
                df1 = df[(df['Bus']==bus1)&(df['Service Date']==s_date)]
                df2 = df[(df['Bus']==bus2)&(df['Service Date']==s_date)]
                if (len(df1)==0) or (len(df2)==0):continue
                for sf_type in sf_types:
                    df1.name,df2.name = 'df1','df2'
                    df1.index,df2.index = range(len(df1)),range(len(df2))
                    for i in [df1,df2]:
                        change=[]
                        tmp=i[sf_type][0]
                        for j in i.index:
                            if i[sf_type][j]==tmp:change.append(0)
                            elif i[sf_type][j]>tmp:
                                change.append(1)
                                tmp = i[sf_type][j]
                            else:
                                change.append(-1)
                                tmp = i[sf_type][j]
                        i['fare_change_'+i.name+'_'+sf_type] = change                        
                    change_in_df1_due_to_df2 = []
                    time_difference = []
                    for i in range(len(df2)):
                        if df2['fare_change_df2_'+sf_type][i]!=0:
                            for j in df1[df1['RecordedAt']>=df2['RecordedAt'][i]].index:
                                if df1['fare_change_df1_'+sf_type][j]==df2['fare_change_df2_'+sf_type][i]:
                                    change_in_df1_due_to_df2.append(1)
                                    time_difference.append(df1['RecordedAt'][j]-df2['RecordedAt'][i])
                                    break
                                elif abs(df1['fare_change_df1_'+sf_type][j]-df2['fare_change_df2_'+sf_type][i])==2:
                                    change_in_df1_due_to_df2.append(-1)
                                    time_difference.append(df1['RecordedAt'][j]-df2['RecordedAt'][i])
                                    break    
                #print(prob_of_change_in_df1_due_to_df2,minimum_time_of_change)        
                try:s_date_corrs.append([Avg(change_in_df1_due_to_df2),sum(time_difference,datetime.timedelta(0))/len(time_difference)])
                except:pass
        #print(s_date_corrs)
            try:bus2_corrs[bus2] = [Avg([row[0] for row in s_date_corrs]),sum([row[0] for row in s_date_corrs]),sum([row[1] for row in s_date_corrs],datetime.timedelta(0))/len(s_date_corrs)]
            except:pass
        if followed_by==False:
            try:del bus2_corrs[bus1]
            except:continue
            #print(bus2_corrs)
            xy = [row[1] for row in list(bus2_corrs.values())]
            z = [row[2] for row in list(bus2_corrs.values())]
            mxy = max(xy) #Max (Probability*Days_followed)
            lxy = [i for i, j in enumerate(xy) if j == mxy] #Index of Maximum Probabilities
            minz = []
            for i in lxy:
                if len(minz)==0:
                    minz.append(z[i])
                elif min(minz)>z[i]:
                    minz.pop()
                    minz.append(z[i])
            lz = [j for i,j in enumerate(lxy) if z[j]==minz[0]] #Index of Minimum time difference
            if len(lxy)>1: #If there are more than 1 bus with maximum (probability*Days_followed)
                bus1_corrs.loc[len(bus1_corrs.index)]=[bus1,list(bus2_corrs.keys())[lz[0]],abs(list(bus2_corrs.values())[lz[0]][0]),0,0]
            else:bus1_corrs.loc[len(bus1_corrs.index)]=[bus1,list(bus2_corrs.keys())[lxy[0]],abs(list(bus2_corrs.values())[lxy[0]][0]),0,0]
        else:
            #print(bus2_corrs)
            if len(list(bus2_corrs.values()))!=0:
                tmpdf.loc[len(tmpdf.index)]=[bus2,list(bus2_corrs.values())[0][1],list(bus2_corrs.values())[0][2],bus1,list(bus2_corrs.values())[0][0]]
            else:
                tmpdf.loc[len(tmpdf.index)]=[bus2,0,'','',0]
    if followed_by==False:
        return bus1_corrs
    else:
        try:tmpdf = tmpdf.drop(tmpdf[tmpdf['Is followed by']==bus2].index)
        except:
            return tmpdf
        #print(tmpdf)
        ma = max(tmpdf['Follows'])
        la =  [i for i, j in enumerate(tmpdf['Follows']) if j == ma]
        minb = []
        for i in la:
            if len(minb)==0:
                minb.append(tmpdf.iloc[[i],[2]].values[0][0])
            elif min(minb)>tmpdf.iloc[[i],[2]].values[0][0]:
                minb.pop()
                minb.append(tmpdf.iloc[[i],[2]].values[0][0])
        lb = [j for i,j in enumerate(la) if tmpdf.iloc[[j],[2]].values[0][0] == minb[0]]
        #print(ma,la,minb,lb)
        tmpdf.iloc[:,[4]] = abs(tmpdf.iloc[:,[4]].values)
        if len(la)>1:
            #print(tmpdf.iloc[lb[0],:])
            return tmpdf.iloc[int(lb[0]),:]
        else:
            #print(tmpdf.iloc[la[0],:])
            return tmpdf.iloc[int(la[0]),:]

follows(zero_SFT2_buses,zero_SFT2_buses+non0_SFT_buses,['Seat Fare Type 1'],False)
follows(zero_SFT1_buses,zero_SFT1_buses+non0_SFT_buses,['Seat Fare Type 2'],False)
follows(non0_SFT_buses,Buses,['Seat Fare Type 1','Seat Fare Type 2'],False)

#Creating followed_by column
for bus in bus1_corrs[bus1_corrs.index!=37]['Bus']:
    if bus in non0_SFT_buses:
        tmpdf = follows(Buses,[str(bus)],['Seat Fare Type 1','Seat Fare Type 2'],True)
    if bus in zero_SFT1_buses:
        tmpdf = follows(zero_SFT1_buses+non0_SFT_buses,[str(bus)],['Seat Fare Type 2'],True)
    if bus in zero_SFT2_buses:
        tmpdf = follows(zero_SFT2_buses+non0_SFT_buses,[str(bus)],['Seat Fare Type 1'],True)
    #print(tmpdf)
    i = bus1_corrs[bus1_corrs['Bus']==bus].index
    bus1_corrs.loc[i,['Is followed by']] = tmpdf['Is followed by']
    bus1_corrs.iloc[i,[4]] = tmpdf[4]

#Appending Independent Buses
df_original = pd.read_csv('PricingData.csv')
buses_initial = df_original['Bus'].unique()
for bus in buses_initial:
    if bus not in list(bus1_corrs['Bus']):
        bus1_corrs.loc[len(bus1_corrs.index)] = [bus,'','','','']
        
bus1_corrs.to_csv('SYE8027_Eccentric_output.csv',index=False)