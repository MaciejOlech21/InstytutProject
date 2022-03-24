import pandas as pd
import glob, os
import numpy as np
import re

#def convert(df):
    #df['KOJ'] = file['KOJ'].astype(int)

def concat_df(extension = 'csv'):
    empty_df = pd.DataFrame()
    for file in glob.glob(f"*.{extension}"):
        #print(file)
        a = pd.read_csv(file, sep=',')
        empty_df = pd.concat([empty_df, a])

    return empty_df


#plec_osobnika = input("Proszę wybrać:\n1 - knur\n2 - locha\n")
#rok = input("Proszę wybrać rok")


def prepare_df(DT, plec_osobnika = 1, rok = 2021):
    DT.KY3 = DT.KY3.replace(',', '.', regex=True)
    DT.KY5 = DT.KY5.replace(',', '.', regex=True)
    DT.OKO = DT.OKO.replace(',', '.', regex=True)
    DT.MIESO_PROC = DT.MIESO_PROC.replace(',', '.', regex=True)
    DT['KY3'] = DT['KY3'].astype(float)
    DT['KY5'] = DT['KY5'].astype(float)
    DT['OKO'] = DT['OKO'].astype(float)
    DT['MIESO_PROC'] = DT['MIESO_PROC'].astype(float)

    DT = DT[
        ['KE', 'KS', 'KW', 'KSE', 'KP', 'KDOC', 'KH', 'KRO', 'KOJ', 'KRM', 'KMT', 'KNR', 'KDUR', 'KY1', 'KY2', 'KY3',
         'KY4', 'KY5', 'OKO', 'T_PRZYR', 'MIESO_PROC', 'T_INDEX']]
    knur = DT['KP'] == int(plec_osobnika)
    DT = DT[knur]

    DT['KDOC'] = pd.to_datetime(DT['KDOC'])
    DT['KDUR'] = pd.to_datetime(DT['KDUR'])
    DT['DNI'] = 0
    DT['DNI'] = DT['KDOC'] - DT['KDUR']
    DT['DNI'] = DT['DNI'] / np.timedelta64(1, 'D')

    DT = DT[DT['KDOC'].dt.year == int(rok)]
    DT = DT.drop_duplicates()

    DT['KRO'] = DT['KRO'].apply(str)
    DT['KRM'] = DT['KRM'].apply(str)
    DT['combined'] = DT['KRO'] + "|" + DT['KRM']
    clear_dict = {}
    clear_dict['10|10'] = 1
    clear_dict['20|20'] = 2
    clear_dict['60|60'] = 3
    clear_dict['70|70'] = 4
    clear_dict['80|80'] = 5
    clear_dict['90|90'] = 6
    clear_dict['30|30'] = 7
    clear_dict['50|50'] = 8
    clear_dict['40|40'] = 9
    DT['cleared'] = DT['combined'].apply(lambda y: clear_dict[y] if y in clear_dict.keys() else y)
    rest = DT[DT['cleared'].str.contains('|') == True]

    wbp = DT.where(DT['cleared'] == 1).dropna()
    pbz = DT.where(DT['cleared'] == 2).dropna()
    hamp = DT.where(DT['cleared'] == 3).dropna()
    dur = DT.where(DT['cleared'] == 4).dropna()
    piet = DT.where(DT['cleared'] == 5).dropna()
    l990 = DT.where(DT['cleared'] == 6).dropna()
    zb = DT.where(DT['cleared'] == 7).dropna()
    zps = DT.where(DT['cleared'] == 8).dropna()
    pula = DT.where(DT['cleared'] == 9).dropna()


    def createDT(race):
        if plec_osobnika == 1:
            temp = pd.DataFrame(columns=['Rasa', "Liczba Knurów", 'Wiek w dniu oceny', 'Masa ciała w dniu oceny (kg)',
                                         'Średnia grubość słoniny standar. (mm)',
                                         'Wysokość "oka" polędwicy standar. (mm)', 'Przyrost dzienny standar. (g)',
                                         'Procentowa zawartość mięsa standar.',
                                         'Indeks (pkt)'], index=[1])
            if race is wbp:
                temp['Rasa'] = "WBP"
            elif race is pbz:
                temp['Rasa'] = "PBZ"
            elif race is hamp:
                temp['Rasa'] = "Hampshire"
            elif race is dur:
                temp['Rasa'] = "Duroc"
            elif race is piet:
                temp['Rasa'] = "Pietrain"
            elif race is l990:
                temp['Rasa'] = "Linia 990"
            elif race is zb:
                temp['Rasa'] = "Złotnicka Biała"
            elif race is zps:
                temp['Rasa'] = "Złotnicka Pstra"
            elif race is pula:
                temp['Rasa'] = "Puławska"
            elif race is rest:
                temp['Rasa'] = "Mieszańce"
            return temp
        else:
            temp = pd.DataFrame(columns=['Rasa', "Liczba Loch", 'Wiek w dniu oceny', 'Masa ciała w dniu oceny (kg)',
                                         'Średnia grubość słoniny standar. (mm)',
                                         'Wysokość "oka" polędwicy standar. (mm)', 'Przyrost dzienny standar. (g)',
                                         'Procentowa zawartość mięsa standar.',
                                         'Indeks (pkt)'], index=[1])
            if race is wbp:
                temp['Rasa'] = "WBP"
            elif race is pbz:
                temp['Rasa'] = "PBZ"
            elif race is hamp:
                temp['Rasa'] = "Hampshire"
            elif race is piet:
                temp['Rasa'] = "Pietrain"
            elif race is l990:
                temp['Rasa'] = "Linia 990"
            elif race is zb:
                temp['Rasa'] = "Złotnicka Biała"
            elif race is zps:
                temp['Rasa'] = "Złotnicka Pstra"
            elif race is pula:
                temp['Rasa'] = "Puławska"
            elif race is rest:
                temp['Rasa'] = "Mieszańce"
            return temp

    def new_count(DT, DT2):
        if plec_osobnika == 1:
            DT2['Liczba Knurów'] = len(DT)
        else:
            DT2["Liczba Loch"] = len(DT)
        DT2['Wiek w dniu oceny'] = DT['DNI'].mean()
        DT2['Wiek w dniu oceny'] = DT2['Wiek w dniu oceny'].round(decimals=0)
        DT2['Wiek w dniu oceny'] = DT2['Wiek w dniu oceny'].astype(int)
        DT2['Masa ciała w dniu oceny (kg)'] = np.around(DT['KY1'].mean())
        DT2['Masa ciała w dniu oceny (kg)'] = DT2['Masa ciała w dniu oceny (kg)'].astype(int)
        DT2['Średnia grubość słoniny standar. (mm)'] = np.around(((DT['KY3'] + DT['KY5']) / 2).mean(), decimals=2)
        DT2['Wysokość "oka" polędwicy standar. (mm)'] = np.around(DT['OKO'].mean(), decimals=2)
        DT2['Przyrost dzienny standar. (g)'] = np.around(DT['T_PRZYR'].mean())
        DT2['Przyrost dzienny standar. (g)'] = DT2['Przyrost dzienny standar. (g)'].astype(int)
        DT2['Procentowa zawartość mięsa standar.'] = np.around(DT['MIESO_PROC'].mean(), decimals=2)
        DT2['Indeks (pkt)'] = np.around(DT['T_INDEX'].mean())
        DT2['Indeks (pkt)'] = DT2['Indeks (pkt)'].astype(int)
        return DT2

    race_list = [wbp, pbz, hamp, dur, piet, l990, zb, zps, pula, rest]
    DF = pd.DataFrame()
    for race in race_list:
        if race.empty:
            continue
        tempDF = createDT(race)
        tempDF2 = new_count(race, tempDF)
        DF = pd.concat([DF, tempDF2])

    DF.set_index("Rasa", inplace=True)

    WBP = createDT(wbp)
    new_count(wbp, WBP)

    PBZ = createDT(pbz)
    new_count(pbz, PBZ)

    HAMP = createDT(hamp)
    new_count(hamp, HAMP)

    DUR = createDT(dur)
    new_count(dur, DUR)

    PIET = createDT(piet)
    new_count(piet, PIET)

    L990 = createDT(l990)
    new_count(l990, L990)

    ZB = createDT(zb)
    new_count(zb, ZB)

    ZPS = createDT(zps)
    new_count(zps, ZPS)

    PULA = createDT(pula)
    new_count(pula, PULA)

    REST = createDT(rest)
    new_count(rest, REST)


    def createDTCity():
        temp = pd.DataFrame(
            columns=['Filia "POLSUS"', "Liczba Knurów", 'Wiek w dniu oceny', 'Masa ciała w dniu oceny (kg)',
                     'Średnia grubość słoniny standar. (mm)',
                     'Wysokość "oka" polędwicy standar. (mm)', 'Przyrost dzienny standar. (g)',
                     'Procentowa zawartość mięsa standar.',
                     'Indeks (pkt)'])
        temp['Filia "POLSUS"'] = ["Wrocław", "Bydgoszcz", "Lublin", "Zielona Góra", "Łódź", "Kraków", "Warszawa",
                                  "Opole", "Rzeszów", "Białystok", "Gdańsk",
                                  "Katowice", "Kielce", "Olsztyn", "Poznań", "Koszalin"]
        temp = pd.DataFrame(temp).set_index('Filia "POLSUS"')
        return temp

    def round_float(s):
        '''1. if s is float, round it to 0 decimals
           2. else return s as is
        '''
        import re
        m = re.match("(\d+\.\d+)", s.__str__())
        try:
            r = round(float(m.groups(0)[0]), 2)
        except:
            r = s
        return r

    def split_it(days):
        return re.compile('^\d{3}', days)

    def split_2(days2):
        return re.findall('(\[)', days2)

    dic_miasta = {2: 'Wrocław',
                  4: 'Bydgoszcz',
                  6: 'Lublin',
                  8: 'Zielona Góra',
                  10: 'Łódź',
                  12: 'Kraków',
                  14: 'Warszawa',
                  16: 'Opole',
                  18: 'Rzeszów',
                  20: 'Białystok',
                  22: 'Gdańsk',
                  24: 'Katowice',
                  26: 'Kielce',
                  28: 'Olsztyn',
                  30: 'Poznań',
                  32: 'Koszalin'}

    def city_count(DT, PDT):
        for i, sub in PDT.groupby('KW'):
            DT.loc[dic_miasta[i]]['Liczba Knurów'] = sub.KY1.agg(len)
            DT.loc[dic_miasta[i]]['Masa ciała w dniu oceny (kg)'] = sub.KY1.mean()
            DT.loc[dic_miasta[i]]['Wiek w dniu oceny'] = sub.DNI.mean()
            DT.loc[dic_miasta[i]]['Średnia grubość słoniny standar. (mm)'] = ((sub.KY3 + sub.KY5) / 2).mean()
            DT.loc[dic_miasta[i]]['Wysokość "oka" polędwicy standar. (mm)'] = sub.OKO.mean()
            DT.loc[dic_miasta[i]]['Przyrost dzienny standar. (g)'] = sub.T_PRZYR.mean()
            DT.loc[dic_miasta[i]]['Procentowa zawartość mięsa standar.'] = sub.MIESO_PROC.mean()
            DT.loc[dic_miasta[i]]['Indeks (pkt)'] = sub.T_INDEX.mean()

    def roundup(DT):
        DT.dropna(inplace=True)
        DT['Wiek w dniu oceny'] = DT['Wiek w dniu oceny'].astype(int)
        DT['Masa ciała w dniu oceny (kg)'] = DT['Masa ciała w dniu oceny (kg)'].astype(int)
        DT['Średnia grubość słoniny standar. (mm)'] = DT['Średnia grubość słoniny standar. (mm)'].apply(round_float)
        DT['Wysokość "oka" polędwicy standar. (mm)'] = DT['Wysokość "oka" polędwicy standar. (mm)'].apply(round_float)
        DT['Procentowa zawartość mięsa standar.'] = DT['Procentowa zawartość mięsa standar.'].apply(round_float)
        DT['Przyrost dzienny standar. (g)'] = DT['Przyrost dzienny standar. (g)'].astype(int)
        DT['Indeks (pkt)'] = DT['Indeks (pkt)'].astype(int)
        return DT

    city_wbp = createDTCity()
    city_count(city_wbp, wbp)
    roundup(city_wbp)

    city_pbz = createDTCity()
    city_count(city_pbz, pbz)
    roundup(city_pbz)

    city_hamp = createDTCity()
    city_count(city_hamp, hamp)
    roundup(city_hamp)

    city_dur = createDTCity()
    city_count(city_dur, dur)
    roundup(city_dur)

    city_piet = createDTCity()
    city_count(city_piet, piet)
    roundup(city_piet)

    city_l990 = createDTCity()
    city_count(city_l990, l990)
    roundup(city_l990)

    city_zb = createDTCity()
    city_count(city_zb, zb)
    roundup(city_zb)

    city_zps = createDTCity()
    city_count(city_zps, zps)
    roundup(city_zps)

    city_pula = createDTCity()
    city_count(city_pula, pula)
    roundup(city_pula)

    city_rest = createDTCity()
    city_count(city_rest, rest)
    roundup(city_rest)

    # table to display
    last2 = pd.concat(
        [city_wbp, city_pbz, city_hamp, city_dur, city_piet, city_l990, city_zb, city_zps, city_pula, city_rest],
        keys=['Wbp', 'Pbz', 'Hampshire', 'Duroc', 'Pietrain', 'Linia 990', 'Złotnicka biała', 'Złotnicka pstra',
              'Puławska', 'Mieszańce'])
    last2.sort_index(inplace=True)


    return last2

def show_df(file):
    a = pd.read_csv(file, sep=',')

    return a


def write_df_on_server(file, file_name):
    a = pd.read_csv(file, sep=',')
    a.to_csv(file_name[:-4] + ".csv", index=False)

def df_to_file(df, file_name):
    df.to_csv('fina.txt', index = 'False')


def sum_all_rows():
    empty_df = pd.DataFrame()
    for file in glob.glob("*.csv"):
        a = pd.read_csv(file, sep=',')
        empty_df = pd.concat([empty_df, a])

    return empty_df.describe()




