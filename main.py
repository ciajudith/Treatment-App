from pyspark.sql import SparkSession
from pyspark.sql.functions import col,sum,month,year,to_date,when
import pandas as pd
import qrcode as qrcode
import os

spark = SparkSession.builder.appName("Analyse de Vente").getOrCreate()

df_orders = spark.read.option("delimiter", ",").csv("sales-2016.csv", header=True)
df_orders = df_orders.withColumn("SalesQuantity", col("SalesQuantity").cast("int")) \
        .withColumn('Date', to_date(col('Date'), 'dd/MM/yyyy')) \
        .withColumn("UnitCost", col("UnitCost").cast("float")) \
        .withColumn("UnitPrice", col("UnitPrice").cast("float")) \
        .withColumn("TotalCost", col("TotalCost").cast("float")) \
        .withColumn("TotalPrice", col("TotalPrice").cast("float")) \
        .withColumn("TotalProfit", col("TotalProfit").cast("float"))
df_orders.show()

df_products = spark.read.option("delimiter", "\t").csv("produits.txt", header=True)
df_products.show()

pays_xlsx = pd.read_excel("Pays.xlsx")
pays_df = spark.createDataFrame(pays_xlsx)
pays_df.printSchema()
pays_df.show()

def visualiser_ventes_par_produit():    
    print("Visualisation du nombre de ventes par produit.")
    df_combined = df_orders.join(df_products, df_orders.Product == df_products.Produit, "inner")
    resultats = df_combined.groupBy("Product").agg(sum("SalesQuantity").alias("NombreVentes"))
    resultats.show()  
    nom_fichier_csv = "visualiser_ventes_par_produit.csv"
    generate_and_rename_csv(resultats, nom_fichier_csv)
    generate_qr_code(f"D:/Treatment App/Data/{nom_fichier_csv}/{nom_fichier_csv}", 'D:/Treatment App/Images/visualiser_ventes_par_produit.png')

def visualiser_chiffre_affaire():
    print("Visualisation du chiffre d'affaires.")
    resultats = df_orders.groupBy().agg(sum("TotalPrice").alias("ChiffreAffaires"))
    resultats.show()
    

def visualiser_evolution_ventes():
    print("Visualisation de l'évolution des ventes au fil des mois ou des années.")
    df_order = df_orders.withColumn("Mois", month("Date"))
    df_order.show()
    df_order = df_order.withColumn(
    "Mois",
    when(col("Mois") == 1, "Janvier")
    .when(col("Mois") == 2, "Février")
    .when(col("Mois") == 3, "Mars")
    .when(col("Mois") == 4, "Avril")
    .when(col("Mois") == 5, "Mai")
    .when(col("Mois") == 6, "Juin")
    .when(col("Mois") == 7, "Juillet")
    .when(col("Mois") == 8, "Août")
    .when(col("Mois") == 9, "Septembre")
    .when(col("Mois") == 10, "Octobre")
    .when(col("Mois") == 11, "Novembre")
    .when(col("Mois") == 12, "Décembre")
    .otherwise("Inconnu")
)
    resultats = df_order.groupBy("Mois").agg(sum("SalesQuantity").alias("NombreVentes"))
    resultats.show()
    nom_fichier_csv = "visualiser_evolution_ventes.csv"
    generate_and_rename_csv(resultats, nom_fichier_csv)
    generate_qr_code(f"D:/Treatment App/Data/{nom_fichier_csv}/{nom_fichier_csv}", 'D:/Treatment App/Images/visualiser_ventes_par_produit.png')


def visualiser_ventes_par_categorie():
    print("Visualisation des ventes par catégories de produit.")
    df_combined = df_orders.join(df_products, df_orders.Product == df_products.Produit, "inner")
    resultats = df_combined.groupBy("Categories").agg(sum("SalesQuantity").alias("NombreVentesParCategorie"))

    resultats.show()


def generate_and_rename_csv(resultats, nom_fichier_csv):
    resultats.write.options(header='True', delimiter=',') \
    .mode("overwrite") \
    .csv(f"D:/Treatment App/Data/{nom_fichier_csv}")
    dossier = f"D:/Treatment App/Data/{nom_fichier_csv}"
    for fichier in os.listdir(dossier):
        if fichier.startswith("part-") and fichier.endswith(".csv"):
            ancien_nom = os.path.join(dossier, fichier)
            nouveau_nom = os.path.join(dossier, nom_fichier_csv)
            os.rename(ancien_nom, nouveau_nom)
            
def generate_qr_code(file_path, output_path='qrcode.png'):
    qr = qrcode.QRCode(
        version=1,
        error_correction=qrcode.constants.ERROR_CORRECT_L,
        box_size=10,
        border=4,
    )

    with open(file_path, 'rb') as file:
        qr.add_data(file.read())
        qr.make(fit=True)

    img = qr.make_image(fill_color="black", back_color="white")
    img.save(output_path)

def main():
    
    while True:
        print("\nOptions :")
        print("1. Visualiser le nombre de ventes par produit")
        print("2. Visualiser le chiffre d'affaires")
        print("3. Visualiser l'évolution des ventes")
        print("4. Visualiser les ventes par catégories de produit")
        print("0. Quitter")

        choix = input("Choisissez une option (0-4) : ")

        actions = {
            '1': visualiser_ventes_par_produit,
            '2': visualiser_chiffre_affaire,
            '3': visualiser_evolution_ventes,
            '4': visualiser_ventes_par_categorie,
            '0': exit
        }
    
        action = actions.get(choix, lambda: print("Option invalide. Veuillez choisir à nouveau."))
        action()
        

if __name__ == "__main__":
    main()
    spark.stop()
