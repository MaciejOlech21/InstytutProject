import streamlit as st
import przyzyciowa as pz
import rozplodowa as rz
import numpy as np

@st.cache
def convert_df(df):
     # IMPORTANT: Cache the conversion to prevent computation on every rerun
     return df.to_csv().encode('utf-8')

uploaded_files = st.file_uploader("Proszę wybrać pliki csv", accept_multiple_files=True)

choose = st.selectbox("Proszę wybrać",['Rozpłodowa','Przyżyciowa'])
if choose == 'Rozpłodowa':
    date_from = st.sidebar.date_input("Data początkowa")
    date_to = st.sidebar.date_input("Data końcowa")
    date_from = np.datetime64(date_from)
    date_to = np.datetime64(date_to)

    try:
        for i in uploaded_files:
            pz.write_df_on_server(i, i.name)

        data2 = rz.concat_df()
        data3 = rz.prepare_df2(data2[0], data2[1], date_from, date_to)
        st.subheader('Rozpłodowa Rasa')
        st.dataframe(data3[0])

        csv2 = convert_df(data3[0])
        st.download_button(
            label="Download data as CSV",
            data=csv2,
            mime='text/csv',
            file_name='rozplodowa_rasa.csv',
        )
        st.subheader('Rozpłodowa rasa/region')
        st.dataframe(data3[1])
        csv3 = convert_df(data3[1])
        st.download_button(
            label="Download data as CSV",
            data=csv3,
            mime='text/csv',
            file_name='rozplodowa_rasa_region.csv',
        )
        st.subheader('Rozpłodowa czysto rasowa i mieszańce')
        st.dataframe(data3[2])
        csv4 = convert_df(data3[2])
        st.download_button(
            label="Download data as CSV",
            data=csv4,
            mime='text/csv',
            file_name='rozplodowa_krzyzowki.csv',
        )
        st.header('Rozpłodowa użytkowość lochy w kolejno następujących po sobie miotach')
        st.dataframe(data3[3])
        csv5 = convert_df(data3[3])
        st.download_button(
            label="Download data as CSV",
            data=csv5,
            mime='text/csv',
            file_name='rozplodowa_rasa_miot.csv',
        )
    except:
        st.write("Brak załadowanych plików lub wprowadzonych danych")



elif choose == 'Przyżyciowa':
    date = st.sidebar.text_input("Proszę wprowadzić rok")
    st.sidebar.write("Proszę wprowadzić liczbę odpowiadającą płci")
    st.sidebar.write("1 - knur ")
    gender = st.sidebar.text_input("2 -locha")

    try:
        for i in uploaded_files:
            pz.write_df_on_server(i, i.name)

        data = pz.prepare_df(pz.concat_df(),gender,date)
        st.subheader('Średnie wyniki oceny przyżyciowej według rasy w poszczególnych filiach "POLSUS"')
        st.write(data[0])

        csv1 = convert_df(data[0])
        st.download_button(
            label="Download data as CSV",
            data=csv1,
            mime='text/csv',
            file_name='przyzyciowa_rasa_region.csv',
        )
        #pz.df_to_file(data[0], 'przyzyciowa_rasa_region.csv')
        st.subheader('Średnie wyniki oceny przyżyciowej według rasy')
        st.write(data[1])

        csv = convert_df(data[1])
        st.download_button(
        label = "Download data as CSV",
        data = csv,
        mime = 'text/csv',
        file_name = 'przyzyciowa_rasa.csv',
        )
    except:
        st.write("Brak załadowanych plików lub wprowadzonych danych")




