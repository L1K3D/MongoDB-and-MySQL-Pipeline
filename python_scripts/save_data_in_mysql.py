import mysql.connector
import pandas as pd

cnx = mysql.connector.connect(
    
    host = "your_host",
    user = "your_user",
    password = "your_password"
    
)

print(cnx)

cursor = cnx.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS dbprodutos;")

df_livros = pd.read_csv("/root/Documentos/pipeline-python-mongo-mysql/data/tabela_livros.csv")

cursor.execute("""
               
    CREATE TABLE IF NOT EXISTS dbprodutos.tb_livros(
        
               id VARCHAR(100),
               Produto VARCHAR(100),
               Categoria_Produto VARCHAR(100),
               Preco FLOAT(10,2),
               Frete FLOAT(10,2),
               Data_Compra DATE,
               Vendedor VARCHAR(100),
               Local_Compra VARCHAR(100),
               Avaliacao_Compra INT,
               Tipo_Pagamento VARCHAR(100),
               Qntd_Parcelas INT,
               Latitude FLOAT(10,2),
               Longitude FLOAT(10,2),
               
               PRIMARY KEY (id)
               
    );
""")

lista_dados = [tuple(row) for i, row in df_livros.iterrows()]

sql = "INSERT INTO dbprodutos.tb_livros VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

cursor.executemany(sql, lista_dados)
cnx.commit()