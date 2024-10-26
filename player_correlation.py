import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe
import pandas as pd
from collections import Counter, defaultdict
import numpy as np

# Connect to Google Sheets
sa = gspread.service_account()
sh = sa.open("SCALPEL Player Correlation")

# Initialize dictionaries
top_dict = {}
all_dict = {}
diff_dict = {}

# Loop over divisions
for div in range(1, 2):
    schedule_ws = sh.worksheet(f"D{div} Results")
    schedule = get_as_dataframe(schedule_ws, nrows=100)[['Wk', 'A1', 'A2', 'B1', 'B2', 'A', 'B']]
    played = schedule[pd.notna(schedule['A'])]
    if len(played) == 0:
        break
    events = int(np.max(played.Wk))

    df_allevents = pd.DataFrame(columns=["EVENT", '#', 'PLAYER', 'GP', 'W', 'L', 'WR', 'PF', 'PA', "PD'"])

    # Initialize player point differential tracking dictionaries
    partnership_diff = defaultdict(lambda: defaultdict(int))  # Tracks partnership differentials
    opposition_diff = defaultdict(lambda: defaultdict(int))  # Tracks opposition differentials

    # Loop over each event in the division
    for pe in range(1, 1 + events):
        played_event = played[played.Wk == pe]
        if len(played_event) == 0:
            break
        players_ws = sh.worksheet("Players")
        df_players = get_as_dataframe(players_ws, nrows=pd.notna(get_as_dataframe(players_ws).PLAYER).sum())[['PLAYER', 'D', 'RAT', 'AGE', 'EXP', 'GEN']]
        df_players = df_players[df_players.D == div]
        
        players = list(df_players.PLAYER)

        dr = {'M': {}, 'P': {}}
        for p in players:
            for k in ['M', 'P']:
                dr[k][p] = [0, 0]

        eventplayers = list(pd.concat([played_event['A1'].str.strip(), played_event['A2'].str.strip(), \
                                       played_event['B1'].str.strip(), played_event['B2'].str.strip()]))
        df_stats = pd.DataFrame(Counter(eventplayers).items(), columns=['PLAYER', 'GP']).sort_values('PLAYER').reset_index(drop=True)
        df_stats = pd.merge(left=df_players[['PLAYER', 'RAT']], right=df_stats, how='outer').fillna(0)

        # Loop over each match in the event
        for m in range(len(played_event)):
            match = played_event.iloc[m]
            A1, A2, B1, B2 = match[['A1', 'A2', 'B1', 'B2']].str.strip()
            PA, PB = match[['A', 'B']]

            if PA > PB:
                dr['M'][A1][0] += 1
                dr['M'][B1][1] += 1
                dr['M'][A2][0] += 1
                dr['M'][B2][1] += 1
            else:
                dr['M'][A1][1] += 1
                dr['M'][B1][0] += 1
                dr['M'][A2][1] += 1
                dr['M'][B2][0] += 1
            
            # Update points scored and allowed
            dr['P'][A1] = [dr['P'][A1][0] + PA, dr['P'][A1][1] + PB]
            dr['P'][B1] = [dr['P'][B1][0] + PB, dr['P'][B1][1] + PA]
            dr['P'][A2] = [dr['P'][A2][0] + PA, dr['P'][A2][1] + PB]
            dr['P'][B2] = [dr['P'][B2][0] + PB, dr['P'][B2][1] + PA]
            
            # Update partnership point differential
            for p1, p2 in [(A1, A2), (B1, B2)]:
                partnership_diff[p1][p2] += PA
                partnership_diff[p2][p1] += PA
                partnership_diff[p1][p2] -= PB
                partnership_diff[p2][p1] -= PB
            
            # Update opposition point differential
            for p1, opp1 in [(A1, B1), (A1, B2), (A2, B1), (A2, B2)]:
                opposition_diff[p1][opp1] += PA
                opposition_diff[opp1][p1] -= PA

        # Continue with statistics compilation and saving to Google Sheets as before
        # [Remaining unchanged code]

    # Create the point differential DataFrame
    players_list = list(df_players.PLAYER)
    diff_df = pd.DataFrame(index=players_list, columns=players_list).fillna(0)

    for p1 in players_list:
        for p2 in players_list:
            if p1 != p2:
                diff_df.loc[p1, p2] = partnership_diff[p1][p2] + opposition_diff[p1][p2]

    diff_df.reset_index(drop=False,names="player",inplace=True)
    # Save the DataFrame to a new worksheet in Google Sheets if required
    diff_ws = sh.worksheet(f"D{div} Point Differential")
    set_with_dataframe(diff_ws, diff_df, row=1, col=1)