import streamlit as st
import os
import test2 as cd
import pandas as pd
dataFrameSerialization = "legacy"
st.write("hello")
# st.write(cd.show_df('a.txt'))


uploaded_files = st.file_uploader("Choose a CSV file", accept_multiple_files=True)

#st.write(uploaded_files)

for i in uploaded_files:
    #st.write(i)
    #st.write(i.name)
    cd.write_df_on_server(i, i.name)
    #q = cd.concat_df()

try:
    data = cd.prepare_df(cd.concat_df())
    cd.df_to_file(data, 'final.txt')
except:
    st.write("Proszę wybrać pliki, bo nima.")

try:
    st.write(cd.show_df('fina.txt'))
except:
    pass

