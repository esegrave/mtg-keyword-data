# import modules
from mtgsdk import Set
from mtgsdk import Card
import config
import pandas as pd

def importCards():
    print('Running for:')
    print('Keywords: ')
    print(config.keywords)
    print('Colors: ')
    print(config.colors)

    #initialize empty feature sets
    nameList = []
    setList = []
    colorList = []
    keywordList = []

    setNameList = []
    setCodeList = []
    setDateList = []

    #loop through keywords and colors, pulling card details
    for keyword in config.keywords:
        for color in config.colors:
            print('Pulling ' + color + ' ' + keyword)
            cards = Card.where(types='creature')         .where(colors=color)         .where(text=keyword)         .all()
            for card in cards:
                nameList.append(card.name)
                setList.append(card.set)
                colorList.append(color)
                keywordList.append(keyword)

    # pull set details
    sets = Set.all()
    for set in sets:
        setNameList.append(set.name)
        setCodeList.append(set.code)
        setDateList.append(set.release_date)

    # initialize dataframes
    card_data = {'name': nameList,
                'set_code': setList,
                'color': colorList,
                'keyword': keywordList
                }
    set_data = {'set': setNameList,
                'set_code': setCodeList,
                'release_date': setDateList
                }
    
    card_df = pd.DataFrame.from_dict(card_data)
    set_df = pd.DataFrame.from_dict(set_data)

    # filter out promo sets (codes without 3 characters)
    set_df = set_df[set_df['set_code'].str.len() == 3]

    # drop silver-bordered sets
    set_df = set_df[set_df['set_code'] != 'UST']
    set_df = set_df[set_df['set_code'] != 'UNH']
    set_df = set_df[set_df['set_code'] != 'UGL']

    # join set info to card info
    df = card_df.merge(set_df, on='set_code')

    # filter just to the earliest instance of a card name to avoid duplicates, for each color
    df_min = df.groupby(['name','color','keyword'], as_index=False)['release_date'].min()
    df = df.merge(df_min, on=['name', 'color', 'keyword', 'release_date'])

    #roll-up by color, keyword, and set
    df_rollup = df.groupby(['color','keyword', 'set', 'release_date'], as_index=False)['name'].count()
    df_rollup.rename(columns={'name': 'count'}, inplace=True)

    # write final output to file
    df_rollup.to_csv(config.output_folder + "data.csv")
    df_rollup.to_json(config.output_folder + "data.json")

if __name__== "__main__":
    importCards()
