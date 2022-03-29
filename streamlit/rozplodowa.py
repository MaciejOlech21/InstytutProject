import pandas as pd
import numpy as np
import glob

def concat_df(extension = 'csv'):
    DFL = pd.DataFrame()
    DFM = pd.DataFrame()
    for file in glob.glob(f"L*.{extension}"):
        a = pd.read_csv(file, sep=',')
        DFL = pd.concat([DFL, a])

    for file in glob.glob(f"M*.{extension}"):
        b = pd.read_csv(file, sep=',')
        DFM = pd.concat([DFM, b])

    return [DFL,DFM]

    # Rozpłodowa rasa

def prepare_df2(DFL,DFM,date_from = 2020-12-12 ,date_to = 2021-12-11):
    DFL['NMIOT'] = 1
    cols1 = ['LPROS1', 'SUTKI_L', 'SUTKI_P', 'LRASA1', 'LDUR', 'LDOP', 'LRASAOJ', 'LIL11', 'LIL21', 'LWAGA1', 'M_WAGUR',
             'LDBRAK', 'NMIOT', 'LREJ1',
             'LWOJ1', 'LSEK1', 'LSTA1', 'NROJM1', 'LSUT1', 'NRMAM1', 'NOJMIO']
    DFL = DFL[cols1]

    # Zmieniam nazwy kolumn tak by odpowiadały kolumnon z DFL
    DFM['LSUT1'] = np.nan
    DFM = DFM.rename(columns={'LDUR': 'LDUR1'})
    DFM = DFM.rename(columns={'LDOP': 'LDUR'})
    DFM = DFM.rename(columns={'LDUR1': 'LDOP'})
    DFM = DFM.rename(columns={'RASAOJ': 'LRASAOJ'})
    DFM = DFM.rename(columns={'NRO': 'NROJM1'})
    DFM = DFM.rename(columns={'LREJ': 'LREJ1'})
    DFM = DFM.rename(columns={'LWOJ': 'LWOJ1'})
    # Deklaruje listę kolumn którę będą mi potrzebne
    cols2 = ['LPROS1', 'LRASA1', 'LDUR', 'LDOP', 'LRASAOJ', 'LIL11', 'LIL21', 'LWAGA1', 'M_WAGUR', 'LDBRAK', 'NMIOT',
             'LREJ1', 'LWOJ1', 'LSEK1', 'LSTA1', 'LSUT1', 'NROJM1']
    DFM = DFM[cols2]

    # Wypełniam NaN'y zerami
    DFL['SUTKI_L'].fillna(0, inplace=True)
    DFL['SUTKI_P'].fillna(0, inplace=True)
    DFL['LSUT1'].fillna(0, inplace=True)
    # Rozdzielam sutki prawe od lewych
    DFL['PSUT'] = DFL['LSUT1'].astype(str).str[1]
    DFL['LSUT1'] = DFL['LSUT1'].astype(str).str[0]
    DFL['PSUT'].replace('.', '0', inplace=True)

    # Convertuje liczby sutków do inta
    convert_dict = {'LSUT1': int,
                    'PSUT': int,
                    'SUTKI_L': int,
                    'SUTKI_P': int
                    }

    DFL = DFL.astype(convert_dict)

    DFL['ISUT'] = DFL['PSUT'] + DFL['LSUT1'] + DFL['SUTKI_P'] + DFL['SUTKI_L']

    # Funkcja zamienia format kolumn na format daty
    class ToDate():

        def change_to_date(self, a, b):
            a[b] = pd.to_datetime(a[b])

    dat = ToDate()
    dat.change_to_date(DFL, 'LDOP')
    dat.change_to_date(DFM, 'LDOP')
    dat.change_to_date(DFL, 'LDUR')
    dat.change_to_date(DFM, 'LDUR')


    # W DF zostawaimy tylko dane z wcześniej zadeklarowanego przedziału czasowego
    DFL = DFL[(DFL['LDOP'] >= date_from) & (DFL['LDOP'] <= date_to)]
    DFM = DFM[(DFM['LDOP'] >= date_from) & (DFM['LDOP'] <= date_to)]

    # Wyliczamy:
    # Datę pierwszego oprosienia
    DFL['POP'] = DFL['LDOP'] - DFL['LDUR']
    # Okres międzymiotu
    DFM['OMM'] = DFM['LDOP'] - DFM['LDUR']
    # Zamieniam ww wartości na dni
    DFL['POP'] = DFL['POP'] / np.timedelta64(1, 'D')
    DFM['OMM'] = DFM['OMM'] / np.timedelta64(1, 'D')

    # W zmiennej DF3 łączymy poprzednie DF
    DF3 = pd.concat([DFL, DFM])

    DF3 = DF3.rename(columns={'LDUR': 'DURL'})
    DF3 = DF3.rename(columns={'LDOP': 'DURM'})

    # Wyliczamy ile prosiąt nie dożyło 21 dnia życia
    DF3['STRATY'] = DF3['LIL11'] - DF3['LIL21']

    # Wyliczamy wartość % strat
    DF3['ST21'] = (DF3['STRATY'] / DF3['LIL11']) * 100

    # Funkcja tworzy DF z określonymi już kolumnami
    def createDT(name):
        temp = pd.DataFrame(columns=['Rasa', 'liczba loch', 'liczba ocenionych miotów ogółem',
                                     'liczba ocenionych miotów pierw.', 'Liczba prosiąt urodzonych w miocie',
                                     'Liczba prosiąt w 21 dniu życia', 'Straty prosiąt do 21 dnia życia(%)',
                                     'Liczba sutków lochy',
                                     'Wiek w dniu pierwszego oprosienia(dni)', 'Okres miedzymiotu(dni)'], index=[1])
        temp['Rasa'] = name
        return temp

    # Funkcja sortuje dane po odpowiedniej rasie
    def sortedDT(DT, col, number):
        newDT = DT[DT[col] == number]
        return newDT


    # Funkcja sortuje dane po rasie matki i rasie ojca
    def sorted2DT(DT, number1, number2):
        newDT = DT[(DT.LRASA1 == number1) & (DT.LRASAOJ == number2)]
        return newDT

    def sorted3DT(DT, number1, number2):
        newDT = DT[(DT.LRASA1 == number1) & (DT.NMIOT == number2)]
        return newDT


    # Funkcja wylicza wszystkie niezbędne wartości i przypisuje je do DF

    def all_count(DT, new):
        new['liczba loch'] = len(DT.drop_duplicates(subset='LPROS1', keep="first").copy())
        new['liczba ocenionych miotów ogółem'] = len(DT)
        new['liczba ocenionych miotów pierw.'] = len(DT[DT['NMIOT'] == 1])
        new['Liczba prosiąt urodzonych w miocie'] = np.around(DT['LIL11'].mean(), decimals=2)
        new['Liczba prosiąt w 21 dniu życia'] = np.around(DT['LIL21'].mean(), decimals=2)
        new['Straty prosiąt do 21 dnia życia(%)'] = np.around(DT['ST21'].mean(), decimals=2)
        new['Wiek w dniu pierwszego oprosienia(dni)'] = np.around(DT['POP'].mean(), decimals=2)
        new['Okres miedzymiotu(dni)'] = np.around(DT['OMM'].mean(), decimals=2)
        new['Liczba sutków lochy'] = np.around(DT['ISUT'].mean(), decimals=2)

        return new

    def all_count2(DT, new, rasa):
        miot = DT['NMIOT'].unique()
        new['Kolejny miot'] = miot[0]
        new['Mioty szt.'] = len(DT)
        new['Mioty %'] = np.around(len(DT) / len(DF3[(DF3.LRASA1 == rasa)]) * 100, decimals=2)
        new['Liczba prosiąt urodzonych w miocie'] = np.around(DT['LIL11'].mean(), decimals=2)
        new['Liczba prosiąt w 21 dniu życia'] = np.around(DT['LIL21'].mean(), decimals=2)
        new['Straty prosiąt do 21 dnia życia(%)'] = np.around(DT['ST21'].mean(), decimals=2)
        new['Okres międzymiotu(dni)'] = np.around(DT['OMM'].mean())

        return new

    WBP = createDT('Wbp')
    wbp = sortedDT(DF3, 'LRASA1', 10).copy()
    all_count(wbp, WBP)

    PBZ = createDT('Pbz')
    pbz = sortedDT(DF3, 'LRASA1', 20).copy()
    all_count(pbz, PBZ)

    HAMP = createDT('Hampshire')
    hamp = sortedDT(DF3, 'LRASA1', 60).copy()
    all_count(hamp, HAMP)

    DUR = createDT('Duroc')
    dur = sortedDT(DF3, 'LRASA1', 70).copy()
    all_count(dur, DUR)

    PIET = createDT('Pietrain')
    piet = sortedDT(DF3, 'LRASA1', 80).copy()
    all_count(piet, PIET)

    L99 = createDT('Linia 990')
    l99 = sortedDT(DF3, 'LRASA1', 90).copy()
    all_count(l99, L99)

    ZB = createDT('Złotnicka Biała')
    zb = sortedDT(DF3, 'LRASA1', 30).copy()
    all_count(zb, ZB)

    ZPS = createDT('Złotnicka Pstra')
    zps = sortedDT(DF3, 'LRASA1', 50).copy()
    all_count(zps, ZPS)

    PULA = createDT('Puławska')
    pula = sortedDT(DF3, 'LRASA1', 40).copy()
    all_count(pula, PULA)

    # Łącze wyniki ras w jeden DF i ustalam Rasę jako index
    rasaDF = pd.concat([WBP, PBZ, HAMP, DUR, PIET, L99, PULA, ZB, ZPS])
    rasaDF.set_index('Rasa', inplace=True)
    rasaDF['Liczba sutków lochy'] = rasaDF['Liczba sutków lochy'].astype(str)
    rasaDF['Wiek w dniu pierwszego oprosienia(dni)'] = rasaDF['Wiek w dniu pierwszego oprosienia(dni)'].astype(str)
    rasaDF['Liczba sutków lochy'].replace('nan','-',inplace=True)
    rasaDF['Wiek w dniu pierwszego oprosienia(dni)'].replace('nan','-',inplace=True)


    # Rozplodowa rasa/region

    # Deklaruje słoknik miast
    dic_miasta = {1: 'Poznań',
                  2: 'Warszawa',
                  3: 'Poznań',
                  4: 'Białystok',
                  6: 'Bydgoszcz',
                  8: 'Gdańsk',
                  10: 'Katowice',
                  12: 'Kielce',
                  14: 'Koszalin',
                  16: 'Kraków',
                  18: 'Lublin',
                  20: 'Łódź',
                  22: 'Olsztyn',
                  24: 'Opole',
                  26: 'Poznań',
                  28: 'Rzeszów',
                  32: 'Wrocław',
                  34: 'Zielona Góra',
                  98: '98'}

    # Deklaruje listę wydzielonych ras
    race_list = [wbp, pbz, hamp, dur, piet, l99, zb, zps, pula]

    def create_cityDT(city, race):
        temp = pd.DataFrame(columns=['Rasa', 'Miasto', 'liczba loch', 'liczba ocenionych miotów ogółem',
                                     'liczba ocenionych miotów pierw.', 'Liczba prosiąt urodzonych w miocie',
                                     'Liczba prosiąt w 21 dniu życia', 'Straty prosiąt do 21 dnia życia(%)',
                                     'Liczba sutków lochy',
                                     'Wiek w dniu pierwszego oprosienia(dni)', 'Okres miedzymiotu(dni)'], index=[1])
        temp['Miasto'] = city
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
        elif race is l99:
            temp['Rasa'] = "Linia 99"
        elif race is zb:
            temp['Rasa'] = "Złotnicka Biała"
        elif race is zps:
            temp['Rasa'] = "Złotnicka Pstra"
        elif race is pula:
            temp['Rasa'] = "Puławska"
        return temp

    def createDT2(name):
        temp = pd.DataFrame(columns=['Kolejny miot', 'Rasa', 'Mioty szt.',
                                     'Mioty %', 'Liczba prosiąt urodzonych w miocie',
                                     'Liczba prosiąt w 21 dniu życia', 'Straty prosiąt do 21 dnia życia(%)',
                                     'Okres międzymiotu(dni)'], index=[1])
        temp['Rasa'] = name
        return temp

    CompleteCityDF = pd.DataFrame()

    for city in dic_miasta:
        for race in race_list:
            tempDF = create_cityDT(dic_miasta[city], race)
            tempDF2 = sortedDT(race, "LREJ1", city)
            rdy = all_count(tempDF2, tempDF)
            CompleteCityDF = pd.concat([CompleteCityDF, rdy])

    CompleteCityDF['Okres miedzymiotu(dni)'] = CompleteCityDF['Okres miedzymiotu(dni)'].astype(str)
    CompleteCityDF['Liczba sutków lochy'] = CompleteCityDF['Liczba sutków lochy'].astype(str)
    CompleteCityDF['Wiek w dniu pierwszego oprosienia(dni)'] = CompleteCityDF['Wiek w dniu pierwszego oprosienia(dni)'].astype(str)

    CompleteCityDF['Okres miedzymiotu(dni)'].replace('nan','-',inplace=True)
    CompleteCityDF['Liczba sutków lochy'].replace('nan','-',inplace=True)
    CompleteCityDF['Wiek w dniu pierwszego oprosienia(dni)'].replace('nan','-',inplace=True)
    CompleteCityDF = CompleteCityDF.set_index(['Rasa', 'Miasto']).sort_index(ascending=True).dropna()


    # Rozplodowa mieszańce

    WBPxWBP = createDT('WBP x WBP')
    wbpXwbp = sorted2DT(DF3, 10, 10)
    all_count(wbpXwbp, WBPxWBP)

    WBPxPBZ = createDT('WBP x PBZ')
    wbpXpbz = sorted2DT(DF3, 10, 20).copy()
    all_count(wbpXpbz, WBPxPBZ)

    PBZxPBZ = createDT('PBZ x PBZ')
    pbzXpbz = sorted2DT(DF3, 20, 20).copy()
    all_count(pbzXpbz, PBZxPBZ)

    PBZxWBP = createDT('PBZ x WBP')
    pbzXwbp = sorted2DT(DF3, 20, 10).copy()
    all_count(pbzXwbp, PBZxWBP)

    DUROCxDUROC = createDT('Duroc x Duroc')
    durocXduroc = sorted2DT(DF3, 70, 70).copy()
    all_count(durocXduroc, DUROCxDUROC)

    DUROCxPIETRAIN = createDT('Duroc x Pietrain')
    durocXpietrain = sorted2DT(DF3, 70, 80).copy()
    all_count(durocXpietrain, DUROCxPIETRAIN)

    PIETRAINxPIETRAIN = createDT('Pietrain x Pietrain')
    pietrainXpietrain = sorted2DT(DF3, 80, 80).copy()
    all_count(pietrainXpietrain, PIETRAINxPIETRAIN)

    PIETRAINxDUROC = createDT('Pietrain X Duroc')
    pietrainXduroc = sorted2DT(DF3, 80, 70).copy()
    all_count(pietrainXduroc, PIETRAINxDUROC)

    krzyzowkiDF = pd.concat([WBPxWBP, WBPxPBZ, PBZxPBZ, PBZxWBP, DUROCxDUROC,
                             DUROCxPIETRAIN, PIETRAINxPIETRAIN, PIETRAINxDUROC])
    krzyzowkiDF.set_index('Rasa', inplace=True)


    # Rozpłodowa rasa/miot

    race_dict = {
        10: "WBP",
        20: "PBZ",
        30: "Złotnicka Biała",
        40: "Póławska",
        50: "Złotnicka Pstra",
        60: "Hampshire",
        70: "Duroc",
        80: "Pietrain",
        90: "Linia 990"}


    completeDF = pd.DataFrame()
    for race in race_dict:
        dokladna_rasa = DF3[(DF3.LRASA1 == race)]
        mioty = dokladna_rasa['NMIOT'].unique()
        for i in mioty:
            aDF = createDT2(race_dict[race])
            bDF = sorted3DT(DF3, race, i)
            rdy = all_count2(bDF, aDF, race)
            completeDF = pd.concat([completeDF, rdy])

    completeDF.set_index(['Rasa', 'Kolejny miot']).sort_values(by=['Rasa', 'Kolejny miot'])
    completeDF['Okres międzymiotu(dni)'] = completeDF['Okres międzymiotu(dni)'].astype(str)
    completeDF['Okres międzymiotu(dni)'].replace('nan','-',inplace=True)

    return [rasaDF,CompleteCityDF,krzyzowkiDF,completeDF]


