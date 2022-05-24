import pandas as pd
import numpy as np
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from sklearn.feature_extraction.text import CountVectorizer
from scipy.sparse import csr_matrix
from sklearn.neighbors import NearestNeighbors
from application_logging import logger
class recommendation:
    def __init__(self):
        self.logging_file=open('logging.text','+a')
        self.log_writer=logger.App_Logger()
        self.session=""
        self.data_from_cass=""
        self.df=""
        self.genre_vector=""
        self.sparse_data=''

    def dbsession(self):
        try:
            cloud_config = {
                'secure_connect_bundle': 'secure-connect-cas.zip'
            }
            auth_provider = PlainTextAuthProvider('ZNzFJvNpLgTeJdoNYyFFciDz',
                                                  's3H-.1wU3yYdHFAwb8KZXjC5wfOJ+Rm113eUbIA0JozRqGD-NbqUx.eP8uQO.ERADZGeajFBa3Zc3S+H.KXf2J-SixuR8o9fJHZ4nk7yDZ-pvZEdfZKx678MYRQvf2zD')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            self.session = cluster.connect()
            self.log_writer.log(self.loggig_file,'connected to databases')
        except:
            self.log_writer.log(self.logging_file, 'failed  to connect databases')

    def query(self):
        try:
            self.session.execute('use mykeyspace')
            self.data_from_cass = self.session.execute("select * from bookinformations")
            self.log_writer.log(self.loggig_file, 'fetched data from database')

        except:
            self.log_writer.log(self.logging_file, 'not able fetch data from database')

    def dataframe(self):
        self.df=pd.DataFrame(self.data_from_cass.all(),columns=self.data_from_cass.column_names)
    def con_vec_fom(self):
        vectorizer = CountVectorizer()
        vector = vectorizer.fit_transform(self.df['genre'])
        vec = CountVectorizer()
        v = vec.fit_transform(self.df['subgenre'])
        genre_data = pd.DataFrame(vector.toarray(), columns=vectorizer.get_feature_names(), index=self.df['title'])
        sub_genre_data = pd.DataFrame(v.toarray(), columns=vec.get_feature_names(), index=self.df['title'])
        genre_sub_genre= genre_data.merge(sub_genre_data, on=genre_data.index)
        genre_sub_genre.drop_duplicates(inplace=True)
        genre_sub_genre.set_index(genre_sub_genre["key_0"],inplace=True)
        genre_sub_genre.drop(columns=['key_0'],inplace=True)
        self.genre_vector=genre_sub_genre

    def change_to_sparse(self):
        self.sparse_data = csr_matrix(self.genre_vector)


    def train_model(self):
        model = NearestNeighbors(algorithm='brute', metric='cosine')
        model.fit(self.sparse_data)
        self.model=model
    def recommend(self,book_search) :
        l=[]
        self.dbsession()
        self.query()
        self.dataframe()
        self.con_vec_fom()
        self.change_to_sparse()
        self.train_model()
        dis, s = self.model.kneighbors(self.genre_vector[self.genre_vector.index==book_search].values.reshape(1, -1), n_neighbors=6)
        for i in range(6):
            l.append(self.genre_vector.index[s[0][i]])
        return l


