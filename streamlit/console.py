import streamlit as st
import przyzyciowa as pz
import rozplodowa as rz
import numpy as np
#dataFrameSerialization = "legacy"

uploaded_files = st.file_uploader("Choose a CSV file", accept_multiple_files=True)

date_from = st.sidebar.date_input("Data początkowa w formacie rrrr-mm-dd")
date_to = st.sidebar.date_input("Data końcowa w formacie rrrr-mm-dd")

date_from = np.datetime64(date_from)
date_to = np.datetime64(date_to)

for i in uploaded_files:
    pz.write_df_on_server(i, i.name)

data2 = rz.concat_df()
data3 = rz.prepare_df2(data2[0],data2[1],date_from,date_to)
data3['Liczba sutków lochy'] = data3['Liczba sutków lochy'].fillna('-')
data3['Wiek w dniu pierwszego oprosienia(dni)'] = data3['Wiek w dniu pierwszego oprosienia(dni)'].fillna('-')
st.dataframe(data3)





#try:
#    data = pz.prepare_df(cd.concat_df())
#    st.subheader('Średnie wyniki oceny przyżyciowej według rasy w poszczególnych filiach "POLSUS"')
#    st.write(data[0])
#    #pz.df_to_file(data[0], 'przyzyciowa_rasa_region.csv')
#    st.subheader('Średnie wyniki oceny przyżyciowej według rasy')
#    st.write(data[1])
    #pz.df_to_file(data[1], 'przyzyciowa_rasa.csv')
#except:
#    st.write("Brak załadowanych plików")
#try:
#    st.write(cd.show_df('final.txt'))
#except:
#    st.write("brak")
