import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime

sa = gspread.service_account()
sh = sa.open("SCALPEL Ladder")

schedule_ws = sh.worksheet("Singles Results")

schedule = get_as_dataframe(schedule_ws,nrows=90)[['Player A','Player B','1A','1B','2A','2B','3A','3B']]

played = schedule[pd.notna(schedule['1A'])]

players_ws = sh.worksheet("Singles Players")
df_players = get_as_dataframe(players_ws,nrows=pd.notna(get_as_dataframe(players_ws).Player).sum())[['Player','sRAT','dRAT']]
players = list(df_players.Player)

dr={'M':{},'G':{},'P':{}}
for p in players:
    for k in ['M','G','P']:
        dr[k][p]=[0,0]

for m in range(len(played)):
    match = played.iloc[m]

    A,B = match[['Player A','Player B']]
    G1A,G1B,G2A,G2B,G3A,G3B = match[['1A','1B','2A','2B','3A','3B']]
    
    #print(f'1A:{G1A},1B:{G1B}\n2A:{G2A},2B:{G2B}\n3A:{G3A},3B:{G3B}')
    PA = pd.Series([G1A,G2A,G3A]).sum().astype(int)
    PB = pd.Series([G1B,G2B,G3B]).sum().astype(int)
    if pd.isna(G3A):
        #print('settled in 2 games')
        if G2A > G2B:
            #print(f'{A} beat {B} 2 games to 0')
            dr['M'][A][0]+=1
            dr['M'][B][1]+=1
            dr['G'][A][0]+=2
            dr['G'][B][1]+=2
        else:
            #print(f'{B} beat {A} 2 games to 0')
            dr['M'][A][1]+=1
            dr['M'][B][0]+=1
            dr['G'][A][1]+=2
            dr['G'][B][0]+=2
    else:
        #print('settled in 3 games')
        if G3A > G3B:
            #print(f'{A} beat {B} 2 games to 1')
            dr['M'][A][0]+=1
            dr['M'][B][1]+=1
            dr['G'][A][0]+=2
            dr['G'][A][1]+=1
            dr['G'][B][1]+=2
            dr['G'][B][0]+=1
        else:
            #print(f'{B} beat {A} 2 games to 1')
            dr['M'][B][0]+=1
            dr['M'][A][1]+=1
            dr['G'][B][0]+=2
            dr['G'][B][1]+=1
            dr['G'][A][1]+=2
            dr['G'][A][0]+=1
    #print(f'{A} scored {PA} points\n{B} scored {PB} points')
    dr['P'][A]=[dr['P'][A][0]+PA,dr['P'][A][1]+PB]
    dr['P'][B]=[dr['P'][B][0]+PB,dr['P'][B][1]+PA]

df_stats = pd.DataFrame(Counter(pd.concat([played['Player A'],played['Player B']])).items(),columns=['Player','MP']).sort_values('Player')

df_stats['MW']=[dr['M'][x][0] for x in df_stats.Player]
df_stats['ML']=[dr['M'][x][1] for x in df_stats.Player]
df_stats['MR']=(df_stats.MW/df_stats.MP).round(4)

df_stats['W']=[dr['G'][x][0] for x in df_stats.Player]
df_stats['L']=[dr['G'][x][1] for x in df_stats.Player]
df_stats['GP']=df_stats[['W','L']].sum(axis = 1, skipna = True)
df_stats['WR']=(df_stats.W/df_stats.GP).round(4)

df_stats['PF']=[dr['P'][x][0] for x in df_stats.Player]
df_stats['PA']=[dr['P'][x][1] for x in df_stats.Player]
df_stats['PD\'']=((df_stats.PF-df_stats.PA)/df_stats.GP).round(1)


df_stats = pd.merge(left=df_players,right=df_stats,how='outer',on="Player").fillna(0)
df_stats = df_stats.sort_values(['MR','WR','PD\'','sRAT','dRAT'],ascending=[False,False,False,False,False])
print(df_stats)
df_stats['#']=range(1,1+len(df_stats))
df_stats = df_stats[['#','Player','MP','MW','ML','MR','GP','W','L','WR','PF','PA','PD\'']]
print(df_stats.reset_index(drop=True).to_string())

standings_ws = sh.worksheet("Leaderboard")
set_with_dataframe(standings_ws, df_stats, row=2, col=22)
