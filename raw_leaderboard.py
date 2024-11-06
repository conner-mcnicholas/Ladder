import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter
import datetime
import numpy as np

pd.set_option('display.max_rows', None)
pd.set_option('display.precision', 4)


sa = gspread.service_account()
sh = sa.open("Raw SCALPEL Ladder")

top_dict = {}
all_dict = {}
diff_dict = {}

for div in range (1,3):
    schedule_ws = sh.worksheet(f"D{div} Results")
    schedule = get_as_dataframe(schedule_ws)[['Wk','A1', \
        'A2','B1','B2','A','B']]
    played = schedule[pd.notna(schedule['A'])]
    if len(played) == 0:
            break
    events = int(np.max(played.Wk))

    df_allevents = pd.DataFrame(columns = ["EVENT",'#','PLAYER','GP','W','L','WR','PF','PA','PD\''])
    for pe in range(1,1+events):
        played_event = played[played.Wk == pe]
        if len(played_event) == 0:
            break
        players_ws = sh.worksheet("Players")
        df_players = get_as_dataframe(players_ws,nrows=pd.notna(get_as_dataframe(players_ws).PLAYER).sum()) \
            [['PLAYER','D','RAT','AGE','EXP','GEN']]
        df_players = df_players[df_players.D == div]
        
        players = list(df_players.PLAYER)

        dr={'M':{},'P':{}}
        for p in players:
            for k in ['M','P']:
                dr[k][p]=[0,0]

        eventplayers = list(pd.concat([played_event['A1'].str.strip(),played_event['A2'].str.strip(), \
                                    played_event['B1'].str.strip(),played_event['B2'].str.strip()]))
        df_stats = pd.DataFrame(Counter(eventplayers).items(),columns=['PLAYER','GP']).sort_values('PLAYER').reset_index(drop=True)
        df_stats = pd.merge(left=df_players[['PLAYER','RAT']],right=df_stats,how='outer').fillna(0)

        for m in range(len(played_event)):
            match = played_event.iloc[m]
    
            A1,A2,B1,B2 = match[['A1','A2','B1','B2']].str.strip()
            PA,PB = match[['A','B']]
            
            #print(f'A:{A},B:{B}')
            if PA > PB:
                #print(f'{A} beat {B} by score:{PA}-{PB}')
                dr['M'][A1][0]+=1
                dr['M'][B1][1]+=1
                
                dr['M'][A2][0]+=1
                dr['M'][B2][1]+=1
            else:
                #print(f'{B} beat {A} by score:{PB}-{PA}')
                dr['M'][A1][1]+=1
                dr['M'][B1][0]+=1

                dr['M'][A2][1]+=1
                dr['M'][B2][0]+=1
        
            dr['P'][A1]=[dr['P'][A1][0]+PA,dr['P'][A1][1]+PB]
            dr['P'][B1]=[dr['P'][B1][0]+PB,dr['P'][B1][1]+PA]

            dr['P'][A2]=[dr['P'][A2][0]+PA,dr['P'][A2][1]+PB]
            dr['P'][B2]=[dr['P'][B2][0]+PB,dr['P'][B2][1]+PA]

            df_stats = df_stats[pd.notna(df_stats.PLAYER)]
            df_stats['W']=[dr['M'][x][0] for x in df_stats.PLAYER]
            df_stats['L']=[dr['M'][x][1] for x in df_stats.PLAYER]
            df_stats['WR']=(df_stats.W/df_stats.GP).round(4)

            df_stats['PF']=[dr['P'][x][0] for x in df_stats.PLAYER]
            df_stats['PA']=[dr['P'][x][1] for x in df_stats.PLAYER]
            df_stats['PD\'']=((df_stats.PF-df_stats.PA)/df_stats.GP).round(2)
         
        df_stats.sort_values(['WR','PD\''], ascending = [False,False], na_position ='last',inplace=True)
        df_stats['#'] = range(1,len(df_stats)+1)
        df_stats["EVENT"] = f"D{div}.{pe}"
        df_stats = df_stats[['EVENT','#','PLAYER','GP','W','L','WR','PF','PA','PD\'']]
        df_stats = df_stats[df_stats.GP != 0]

        df_allevents = pd.concat([df_allevents,df_stats])

    df_topall = df_allevents.groupby('PLAYER')[['GP','W','L','PF','PA']].sum().reset_index()
    df_topall["WR"] = df_topall.W/df_topall.GP
    df_topall["PD'"] = (df_topall.PF-df_topall.PA)/df_topall.GP
    df_topall.sort_values(['WR','GP','PD\''], ascending = [False,False,False], na_position ='last',inplace=True)
    df_topall['#'] = range(1,len(df_topall)+1)
    df_topall = df_topall[['#','PLAYER','GP','W','L','WR','PF','PA','PD\'']]
    all_dict[div] = df_topall
    print("{:-^65s}".format(f" D{div} ALL "))
    print(f"{df_topall}\n{65*'-'}")

    leader_ws = sh.worksheet(f"Leaderboard")
    set_with_dataframe(leader_ws, df_topall, row=3, col=2+((div-1)*10))