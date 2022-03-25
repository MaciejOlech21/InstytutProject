import streamlit as st
import test2 as cd
#dataFrameSerialization = "legacy"

uploaded_files = st.file_uploader("Choose a CSV file", accept_multiple_files=True)

for i in uploaded_files:
    cd.write_df_on_server(i, i.name)

#try:
#    data = cd.prepare_df(cd.concat_df())
#    st.subheader('Średnie wyniki oceny przyżyciowej według rasy w poszczególnych filiach "POLSUS"')
#    st.write(data[0])
#    #cd.df_to_file(data[0], 'przyzyciowa_rasa_region.csv')
#    st.subheader('Średnie wyniki oceny przyżyciowej według rasy')
#    st.write(data[1])
    #cd.df_to_file(data[1], 'przyzyciowa_rasa.csv')
#except:
#    st.write("Brak załadowanych plików")



try:
    st.write(cd.show_df('final.txt'))
except:
    st.write("brak")
